package org.endeavourhealth.transform.common;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.QueuedMessageDalI;
import org.endeavourhealth.core.database.dal.audit.models.QueuedMessageType;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * cache for FHIR Resources, that keeps as many in possible in memory
 * but will start offloading to the DB if it gets too large
 */
public class ResourceCache<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceCache.class);

    private Map<T, CacheEntryProxy> cache = new ConcurrentHashMap<>();
    private int countInMemory = 0;
    private QueuedMessageDalI dal = DalProvider.factoryQueuedMessageDal();
    private ReentrantLock lock = new ReentrantLock();

    /**
     * if no offloadToDiskPath is specified, it'll offload to the queued_message DB table
     */
    public ResourceCache() {}

    public void addToCache(T key, Resource resource) throws Exception {

        if (resource == null) {
            throw new Exception("Can't add null resource to ResourceCache");
        }

        //release any existing entry for the same key
        CacheEntryProxy existingEntry = cache.get(key);
        if (existingEntry != null) {
            existingEntry.release();
        }

        //add the new resource to the map
        CacheEntryProxy entry = new CacheEntryProxy(resource);
        cache.put(key, entry);
        countInMemory++;

        //see if we need to offload anything to the DB
        offloadResourcesIfNecessary();
    }

    public boolean contains(T key) {
        return cache.containsKey(key);
    }

    public int size() {
        return cache.size();
    }

    public Set<T> keySet() {
        return cache.keySet();
    }

    /**
     * due to potential risks in losing changes to resources when taken out
     * of the cache, we ALWAYS remove from the cache. Then the remover can just re-add when done
     */
    /*public Resource getFromCache(T key) throws Exception {
        CacheEntryProxy existingEntry = cache.get(key);
        if (existingEntry == null) {
            return null;

        } else {
            return existingEntry.getResource();
        }
    }*/

    public Resource getAndRemoveFromCache(T key) throws Exception {
        CacheEntryProxy existingEntry = cache.remove(key);
        if (existingEntry == null) {
            return null;

        } else {
            Resource ret = existingEntry.getResource();
            existingEntry.release();
            return ret;
        }
    }

    public void removeFromCache(T key) throws Exception {
        CacheEntryProxy existingEntry = cache.remove(key);
        if (existingEntry != null) {
            existingEntry.release();
        }
    }

    public void clear() throws Exception {
        for (T key : cache.keySet()) {
            CacheEntryProxy entryProxy = cache.get(key);
            entryProxy.release();
        }
        cache.clear();
    }

    /**
     * we have a max limit on the number of EncounterBuilders we can keep in memory, since keeping too many
     * will result in memory problems. So whenever the cache state changes, check
     */
    private void offloadResourcesIfNecessary() throws Exception {

        int maxSizeInMemory = TransformConfig.instance().getResourceCacheMaxSizeInMemory();
        if (countInMemory > maxSizeInMemory) {
            int toOffload = countInMemory - maxSizeInMemory;

            for (Object key : cache.keySet()) {
                CacheEntryProxy entry = cache.get(key);
                if (!entry.isOffloaded()) {
                    entry.offloadFromMemory();
                    toOffload--;
                    if (toOffload <= 0) {
                        break;
                    }
                }
            }
        }

    }

    /**
     * proxy class to hold the reference to a Resource or the UUID used to store it in the DB
     */
    class CacheEntryProxy {
        private byte[] compressedBytes = null;
        private int originalLen = -1;
        /*private String resourceJson = null;
        private ResourceType resourceType = null;
        private String resourceId = null;*/
        //private Resource resource = null;
        private UUID tempStorageUuid = null;

        public CacheEntryProxy(Resource resource) throws Exception {
            compressBytes(resource);

            /*this.resourceJson = FhirSerializationHelper.serializeResource(resource);
            this.resourceType = resource.getResourceType();
            this.resourceId = resource.getId();*/
            //this.resource = encounterBuilder;
        }

        /**
         * offloads the Resource to the audit.queued_message table for safe keeping, to reduce memory load
         */
        public void offloadFromMemory() throws Exception {
            //if (resource == null) {
            //if (resourceJson == null) {
            if (compressedBytes == null) {
                return;
            }

            //need to lock to avoid problems with offloading happening at the same time as this
            try {
                lock.lock();

                if (tempStorageUuid == null) {
                    tempStorageUuid = UUID.randomUUID();
                }

                String tempFileName = getTempFileName();
                if (tempFileName == null) {
                    String writeStr = Base64.getEncoder().encodeToString(compressedBytes);
                    dal.save(tempStorageUuid, writeStr, QueuedMessageType.ResourceTempStore);
                    //dal.save(tempStorageUuid, resourceJson, QueuedMessageType.ResourceTempStore);
                    //LOG.debug("Offloaded " + resourceType + " " + resourceId + " to DB cache ID: " + this.tempStorageUuid);

                } else {
                    FileUtils.writeByteArrayToFile(new File(tempFileName), compressedBytes);
                    //FileUtils.writeStringToFile(new File(tempFileName), resourceJson, "UTF-8");
                    //LOG.debug("Offloaded " + resourceType + " " + resourceId + " to " + tempFileName + " cache ID: " + this.tempStorageUuid);
                }

                this.compressedBytes = null;
                //this.resourceJson = null;
                //this.resource = null;
                countInMemory--;

            } finally {
                lock.unlock();
            }
        }

        private String getTempFileName() {

            String offloadToDiskPath = TransformConfig.instance().getResourceCacheTempPath();
            if (offloadToDiskPath == null) {
                return null;
            } else {
                return FilenameUtils.concat(offloadToDiskPath, tempStorageUuid.toString() + ".tmp");
            }
        }

        public boolean isOffloaded() {
            return this.compressedBytes == null && this.tempStorageUuid != null;
            //return this.resourceJson == null && this.tempStorageUuid != null;
            //return this.resource == null && this.tempStorageUuid != null;
        }

        /**
         * gets the Encounter, either from the variable or from the audit.queued_message table if it was offloaded
         */
        public Resource getResource() throws Exception {

            //if (this.resource == null && this.tempStorageUuid == null) {
            //if (this.resourceJson == null && this.tempStorageUuid == null) {
            if (this.compressedBytes == null && this.tempStorageUuid == null) {
                throw new Exception("Cannot get Resource after removing or cleaning up");
            }

            /*if (resource != null) {
                return resource;*/
            /*if (resourceJson != null) {
                return FhirSerializationHelper.deserializeResource(resourceJson);*/

            Resource ret = decompressBytes();
            if (ret != null) {
                return ret;
            }

            //need to lock to avoid problems with offloading happening at the same time as this
            try {
                lock.lock();

                //have another null check now we're locked
                ret = decompressBytes();
                if (ret != null) {
                    return ret;
                }

                String tempFileName = getTempFileName();
                if (tempFileName == null) {
                    String readStr = dal.getById(tempStorageUuid);
                    compressedBytes = Base64.getDecoder().decode(readStr);
                    //resourceJson = dal.getById(tempStorageUuid);

                } else {
                    compressedBytes = FileUtils.readFileToByteArray(new File(tempFileName));
                    //resourceJson = FileUtils.readFileToString(new File(tempFileName), "UTF-8");
                }

                countInMemory++;
                ret = decompressBytes();
                //LOG.debug("Restored " + resourceType + " " + resourceId + " from cache ID: " + this.tempStorageUuid);

            } finally {
                lock.unlock();
            }

            //if we've just retrieved one from memory, we probably will need to write another one to DB
            offloadResourcesIfNecessary();

            return ret;
        }

        private void compressBytes(Resource resource) throws Exception {
            String json = FhirSerializationHelper.serializeResource(resource);
            byte[] bytes = json.getBytes("UTF-8");

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(bytes);
            gzip.close();

            this.originalLen = bytes.length;
            this.compressedBytes = out.toByteArray();
        }

        private Resource decompressBytes() throws Exception {
            //create local reference in case another thread nulls the class reference
            byte[] bytes = this.compressedBytes;
            if (bytes == null) {
                return null;
            }

            byte[] bytesOut = new byte[this.originalLen];
            ByteArrayInputStream in = new ByteArrayInputStream(compressedBytes);
            GZIPInputStream gzipInputStream = new GZIPInputStream(in);

            int pos = 0;
            int remaining = bytesOut.length;
            while (true) {
                int read = gzipInputStream.read(bytesOut, pos, remaining);
                pos += read;
                remaining -= read;
                if (remaining <= 0) {
                    break;
                }
            }

            String resourceJson = new String(bytesOut, "UTF-8");
            return FhirSerializationHelper.deserializeResource(resourceJson);
        }

        public void release() throws Exception {
            if (this.tempStorageUuid != null) {

                String tempFileName = getTempFileName();
                if (tempFileName == null) {
                    dal.delete(tempStorageUuid);

                } else {
                    new File(tempFileName).delete();
                }

                this.tempStorageUuid = null;
            }

            if (this.compressedBytes != null) {
                countInMemory--;
                this.compressedBytes = null;
            }
            /*if (this.resourceJson != null) {
                countInMemory--;
                this.resourceJson = null;
            }*/
            /*if (this.resource != null) {
                countInMemory--;
                this.resource = null;
            }*/
        }
    }

}
