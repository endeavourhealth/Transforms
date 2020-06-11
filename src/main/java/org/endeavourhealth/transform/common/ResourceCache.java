package org.endeavourhealth.transform.common;

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
 * cache for FHIR ResourceBuilders the uses compression to minimise memory footprint
 * NOTE: due to how the cache works, there is no GET-only method. To use this cache,
 * you getAndRemove(..) and then addToCache(..) when you want to return it.
 */
public class ResourceCache<T, S extends ResourceBuilderBase> {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceCache.class);

    private Map<T, CacheEntryProxy> cache = new HashMap<>();
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
            CacheEntryProxy entry = new CacheEntryProxy(resourceBuilder);
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
                return existingEntry.getResourceBuilder();
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
        private byte[] compressedBytes = null;
        private int originalLen = -1;
        private byte[] compressedAuditBytes = null;
        private int originalAuditLen = -1;

        public CacheEntryProxy(S resourceBuilder) throws Exception {
            compressBytes(resourceBuilder);
        }

        /**
         * decompresses and returns the resource builder
         * note: annotation required because Java reckons it's not a safe cast
         */
        @SuppressWarnings (value="unchecked")
        public S getResourceBuilder() throws Exception {
            return (S)decompressBytes();
        }

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
        }

        private ResourceBuilderBase decompressBytes() throws Exception {
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

            return ResourceBuilderBase.factory(resource, audit);
        }

        public void release() throws Exception {
            this.compressedBytes = null;
            this.compressedAuditBytes = null;
        }
    }

}
