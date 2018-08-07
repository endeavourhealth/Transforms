package org.endeavourhealth.transform.common;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.QueuedMessageDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * cache for FHIR Resources, that keeps as many in possible in memory
 * but will start offloading to the DB if it gets too large
 */
public class ResourceCache<T, S extends ResourceBuilderBase> {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceCache.class);

    private Map<T, CacheEntryProxy> cache = new HashMap<>();
    //private Set<T> keysInMemory = new HashSet<>();
    private QueuedMessageDalI dal = DalProvider.factoryQueuedMessageDal();
    private ReentrantLock lock = new ReentrantLock();

    /**
     * if no offloadToDiskPath is specified, it'll offload to the queued_message DB table
     */
    public ResourceCache() {}

    public void addToCache(T key, S resourceBuilder) throws Exception {

        if (resourceBuilder == null) {
            throw new Exception("Can't add null resourceBuilder to ResourceCache");
        }

        try {
            lock.lock();

            //release any existing entry for the same key
            CacheEntryProxy existingEntry = cache.get(key);
            if (existingEntry != null) {
                existingEntry.release();
            }

            //add the new resource to the map
            CacheEntryProxy entry = new CacheEntryProxy(key, resourceBuilder);
            cache.put(key, entry);

        } finally {
            lock.unlock();
        }

        //see if we need to offload anything to the DB
        //offloadResourcesIfNecessary();
    }

    public boolean contains(T key) {
        try {
            lock.lock();
            return cache.containsKey(key);

        } finally {
            lock.unlock();
        }
    }

    public int size() {
        try {
            lock.lock();
            return cache.size();

        } finally {
            lock.unlock();
        }
    }

    public Set<T> keySet() {
        try {
            lock.lock();
            return new HashSet<>(cache.keySet());

        } finally {
            lock.unlock();
        }
    }

    public S getAndRemoveFromCache(T key) throws Exception {

        try {
            lock.lock();

            CacheEntryProxy existingEntry = cache.remove(key);

            if (existingEntry == null) {
                return null;

            } else {
                return existingEntry.getResource();
            }

        } finally {
            lock.unlock();
        }
    }

    public void removeFromCache(T key) throws Exception {

        try {
            lock.lock();

            CacheEntryProxy existingEntry = cache.remove(key);
            if (existingEntry != null) {
                existingEntry.release();
            }

        } finally {
            lock.unlock();
        }

    }

    public void clear() throws Exception {

        try {
            lock.lock();

            for (T key : cache.keySet()) {
                CacheEntryProxy entryProxy = cache.get(key);
                entryProxy.release();
            }
            cache.clear();

        } finally {
            lock.unlock();
        }
    }

    /**
     * we have a max limit on the number of EncounterBuilders we can keep in memory, since keeping too many
     * will result in memory problems. So whenever the cache state changes, check
     */
    /*private void offloadResourcesIfNecessary() throws Exception {

        try {
            lock.lock();

            int maxSizeInMemory = TransformConfig.instance().getResourceCacheMaxSizeInMemory();
            if (keysInMemory.size() > maxSizeInMemory) {
                int toOffload = keysInMemory.size() - maxSizeInMemory;

                for (T key : keysInMemory) {
                    CacheEntryProxy entry = cache.get(key);
                    if (entry == null) {
                        continue;
                    }
                    //LOG.debug("Offloading key " + key + " with entry " + entry);
                    entry.offloadFromMemory();

                    toOffload--;
                    if (toOffload <= 0) {
                        break;
                    }
                }
            }

        } finally {
            lock.unlock();
        }
    }*/

    /**
     * proxy class to hold the reference to a Resource or the UUID used to store it in the DB
     */
    class CacheEntryProxy {
        private T key = null;
        private byte[] compressedBytes = null;
        private int originalLen = -1;
        private byte[] compressedAuditBytes = null;
        private int originalAuditLen = -1;

        //private UUID tempStorageUuid = null;

        public CacheEntryProxy(T key, S resource) throws Exception {
            this.key = key;
            compressBytes(resource);

            /*this.resourceJson = FhirSerializationHelper.serializeResource(resource);
            this.resourceType = resource.getResourceType();
            this.resourceId = resource.getId();*/
            //this.resource = encounterBuilder;
        }

        /**
         * offloads the Resource to the audit.queued_message table for safe keeping, to reduce memory load
         */
        /*public void offloadFromMemory() throws Exception {
            //if (resource == null) {
            //if (resourceJson == null) {
            if (compressedBytes == null) {
                return;
            }


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
            keysInMemory.remove(this.key);
            //LOG.debug("Removing key in memory " + key);
        }

        private String getTempFileName() {

            String offloadToDiskPath = TransformConfig.instance().getResourceCacheTempPath();
            if (offloadToDiskPath == null) {
                return null;
            } else {
                return FilenameUtils.concat(offloadToDiskPath, tempStorageUuid.toString() + ".tmp");
            }
        }*/

        /**
         * gets the Encounter, either from the variable or from the audit.queued_message table if it was offloaded
         * NOTE, this is a one-time function as these objects can only be used once
         */
        public S getResource() throws Exception {
            return decompressBytes();
        }
        /*public S getResource() throws Exception {

            if (this.compressedBytes == null && this.tempStorageUuid == null) {
                throw new Exception("Cannot get Resource after removing or cleaning up");
            }

            S ret = decompressBytes();
            if (ret != null) {
                release(); //make sure to release, so our owner knows we're no longer counted
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

            ret = decompressBytes();

            release();

            //LOG.debug("Restored " + resourceType + " " + resourceId + " from cache ID: " + this.tempStorageUuid);
            return ret;
        }*/

        private void compressBytes(ResourceBuilderBase resourceBuilder) throws Exception {

            Resource resource = resourceBuilder.getResource();
            String json = FhirSerializationHelper.serializeResource(resource);
            byte[] bytes = json.getBytes("UTF-8");

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(bytes);
            gzip.close();

            this.originalLen = bytes.length;
            this.compressedBytes = out.toByteArray();

            ResourceFieldMappingAudit audit = resourceBuilder.getAuditWrapper();
            json = audit.writeToJson();
            bytes = json.getBytes("UTF-8");

            out = new ByteArrayOutputStream();
            gzip = new GZIPOutputStream(out);
            gzip.write(bytes);
            gzip.close();

            this.originalAuditLen = bytes.length;
            this.compressedAuditBytes = out.toByteArray();

            //keysInMemory.add(this.key);
            //LOG.debug("Adding key in memory " + key);
        }

        private S decompressBytes() throws Exception {
            //create local reference in case another thread nulls the class reference
            byte[] bytes = this.compressedBytes;
            if (bytes == null) {
                return null;
            }

            byte[] bytesOut = new byte[this.originalLen];
            ByteArrayInputStream in = new ByteArrayInputStream(this.compressedBytes);
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
            Resource resource = FhirSerializationHelper.deserializeResource(resourceJson);

            bytesOut = new byte[this.originalAuditLen];
            in = new ByteArrayInputStream(this.compressedAuditBytes);
            gzipInputStream = new GZIPInputStream(in);

            pos = 0;
            remaining = bytesOut.length;
            while (true) {
                int read = gzipInputStream.read(bytesOut, pos, remaining);
                pos += read;
                remaining -= read;
                if (remaining <= 0) {
                    break;
                }
            }

            String auditJson = new String(bytesOut, "UTF-8");
            ResourceFieldMappingAudit audit = ResourceFieldMappingAudit.readFromJson(auditJson);

            return (S)ResourceBuilderBase.factory(resource, audit);
        }

        public void release() throws Exception {
            this.compressedBytes = null;
            this.compressedAuditBytes = null;
        }
        /*public void release() throws Exception {
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
                keysInMemory.remove(this.key);
                //LOG.debug("Removing key in memory " + key);
                this.compressedBytes = null;
            }

        }*/
    }

}
