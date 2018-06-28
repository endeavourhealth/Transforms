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

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

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
        private Resource resource = null;
        private UUID tempStorageUuid = null;

        public CacheEntryProxy(Resource encounterBuilder) {
            this.resource = encounterBuilder;
        }

        /**
         * offloads the Resource to the audit.queued_message table for safe keeping, to reduce memory load
         */
        public void offloadFromMemory() throws Exception {
            if (resource == null) {
                return;
            }

            //need to lock to avoid problems with offloading happening at the same time as this
            try {
                lock.lock();

                if (tempStorageUuid == null) {
                    tempStorageUuid = UUID.randomUUID();
                }

                String json = FhirSerializationHelper.serializeResource(resource);

                String tempFileName = getTempFileName();
                if (tempFileName == null) {
                    dal.save(tempStorageUuid, json, QueuedMessageType.ResourceTempStore);
                    LOG.debug("Offloaded " + resource.getResourceType() + " " + resource.getId() + " to DB cache ID: " + this.tempStorageUuid);

                } else {
                    FileUtils.writeStringToFile(new File(tempFileName), json, "UTF-8");
                    LOG.debug("Offloaded " + resource.getResourceType() + " " + resource.getId() + " to " + tempFileName + " cache ID: " + this.tempStorageUuid);
                }

                this.resource = null;
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
            return this.resource == null && this.tempStorageUuid != null;
        }

        /**
         * gets the Encounter, either from the variable or from the audit.queued_message table if it was offloaded
         */
        public Resource getResource() throws Exception {

            if (this.resource == null && this.tempStorageUuid == null) {
                throw new Exception("Cannot get Resource after removing or cleaning up");
            }

            if (resource != null) {
                return resource;

            } else {

                //have a separate local reference, in case we immediately get offloaded again
                Resource ret = null;

                //need to lock to avoid problems with offloading happening at the same time as this
                try {
                    lock.lock();

                    //have another null check now we're locked
                    if (this.resource == null) {
                        String json = null;

                        String tempFileName = getTempFileName();
                        if (tempFileName == null) {
                            json = dal.getById(tempStorageUuid);

                        } else {
                            json = FileUtils.readFileToString(new File(tempFileName), "UTF-8");
                        }

                        this.resource = FhirSerializationHelper.deserializeResource(json);
                        ret = this.resource;
                        countInMemory++;
                        LOG.debug("Restored " + resource.getResourceType() + " " + resource.getId() + " from cache ID: " + this.tempStorageUuid);
                    }

                } finally {
                    lock.unlock();
                }

                //if we've just retrieved one from memory, we probably will need to write another one to DB
                offloadResourcesIfNecessary();

                return ret;
            }
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

            if (this.resource != null) {
                countInMemory--;
                this.resource = null;
            }
        }
    }

}
