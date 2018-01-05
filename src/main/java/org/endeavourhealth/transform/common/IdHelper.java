package org.endeavourhealth.transform.common;

import com.google.common.base.Strings;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.common.idmappers.BaseIdMapper;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IdHelper {
    private static final Logger LOG = LoggerFactory.getLogger(IdHelper.class);

    private static JCS cache = null;
    private static Map<Class, BaseIdMapper> idMappers = new ConcurrentHashMap<>();
    private static ResourceIdTransformDalI repository = DalProvider.factoryResourceIdTransformDal();

    static {

        //init the cache
        try {
            //by default the Java Caching System has a load of logging enabled, which is really slow, so turn it off
            //not longer required, since it no longer uses log4J and the new default doesn't have debug enabled
            /*org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.apache.jcs");
            logger.setLevel(org.apache.log4j.Level.OFF);*/

            cache = JCS.getInstance("ResourceIdentifiers");
        } catch (CacheException ex) {
            throw new RuntimeException("Error initialising cache", ex);
        }
    }

    public static String getOrCreateEdsResourceIdString(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId) throws Exception {
        return getOrCreateEdsResourceId(serviceId, systemId, resourceType, sourceId).toString();
    }

    private static UUID getOrCreateEdsResourceId(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId) throws Exception {
        return getOrCreateEdsResourceId(serviceId, systemId, resourceType, sourceId, true);
    }

    public static String getEdsResourceIdAsString(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId) throws Exception {
        return getEdsResourceId(serviceId, systemId, resourceType, sourceId).toString();
    }

    public static UUID getEdsResourceId(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId) throws Exception {
        return getOrCreateEdsResourceId(serviceId, systemId, resourceType, sourceId, false);
    }

    private static UUID getOrCreateEdsResourceId(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId, boolean createIfNotFound) throws Exception {
        Reference sourceReference = ReferenceHelper.createReference(resourceType, sourceId);
        String sourceReferenceValue = sourceReference.getReference();

        //check our cache first
        UUID edsId = checkCache(serviceId, systemId, sourceReferenceValue);
        if (edsId == null) {

            //if not in the cache, hit the DB
            List<Reference> sourceReferences = new ArrayList<>();
            sourceReferences.add(sourceReference);

            Map<Reference, Reference> map = repository.findEdsReferencesFromSourceReferences(serviceId, systemId, sourceReferences);
            Reference edsReference = map.get(sourceReference);
            if (edsReference == null) {

                if (createIfNotFound) {
                    //if definitely no mapping on the DB, create and save a new ID
                    edsId = repository.findOrCreateThreadSafe(serviceId, systemId, resourceType.toString(), sourceId);

                } else {
                    return null;
                }

            } else {
                String edsIdStr = ReferenceHelper.getReferenceId(edsReference);
                edsId = UUID.fromString(edsIdStr);
            }

            addToCache(serviceId, systemId, sourceReferenceValue, edsId);
        }

        return edsId;
    }

    private static UUID checkCache(UUID serviceId, UUID systemId, String referenceValue) {
        String cacheKey = createCacheKey(serviceId, systemId, referenceValue);
        return (UUID)cache.get(cacheKey);
    }

    private static void addToCache(UUID serviceId, UUID systemId, String referenceValue, UUID id) {
        String cacheKey = createCacheKey(serviceId, systemId, referenceValue);
        try {
            cache.put(cacheKey, id);
        } catch (Exception ex) {
            LOG.error("Error adding key ["+ referenceValue + "] value [" + id + "] to ID map cache", ex);
        }
    }

    private static String createCacheKey(UUID serviceId, UUID systemId, String referenceValue) {
        //quick optimisation to cut on string creation
        StringBuilder sb = new StringBuilder();
        sb.append(serviceId.toString());
        sb.append("/");
        sb.append(systemId.toString());
        sb.append("/");
        sb.append(referenceValue);
        return sb.toString();
    }

    /*private static String createCacheKey(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId) {
        //quick optimisation to cut on string creation
        StringBuilder sb = new StringBuilder();
        sb.append(serviceId.toString());
        sb.append("/");
        sb.append(systemId.toString());
        sb.append("/");
        sb.append(resourceType.toString());
        sb.append("/");
        sb.append(sourceId);
        return sb.toString();
    }

    public static String getOrCreateEdsResourceIdString(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId) throws Exception {
        return getOrCreateEdsResourceId(serviceId, systemId, resourceType, sourceId).toString();
    }

    public static UUID getOrCreateEdsResourceId(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId) throws Exception {
        String key = createCacheKey(serviceId, systemId, resourceType, sourceId);

        //check out in-memory cache first
        UUID edsId = (UUID)cache.get(key);
        if (edsId == null) {

            //if not in the memory cache, check the DB
            ResourceIdMap mapping = repository.getResourceIdMap(serviceId, systemId, resourceType.toString(), sourceId);
            if (mapping == null) {
                //if definitely now mapping on the DB, create and save a new ID
                edsId = createEdsResourceId(serviceId, systemId, resourceType, sourceId, key);

            } else {
                edsId = mapping.getEdsId();
            }

            //add to our memory cache, as we're likely to use this ID again soon
            try {
                cache.put(key, edsId);
            } catch (Exception ex) {
                LOG.error("Error adding key ["+key+"] value ["+edsId+"] to ID map cache", ex);
            }
        }
        return edsId;
    }

    private static UUID createEdsResourceId(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId, String cacheKey) throws Exception {

        //we need to synch to prevent two threads generating an ID for the same source ID at the same time
        //use an AtomicInt for each cache key as a synchronisation object and as a way to track
        AtomicInteger atomicInteger = null;
        synchronized (synchLocks) {
            atomicInteger = synchLocks.get(cacheKey);
            if (atomicInteger == null) {
                atomicInteger = new AtomicInteger(0);
                synchLocks.put(cacheKey, atomicInteger);
            }

            atomicInteger.incrementAndGet();
        }

        UUID ret = null;

        synchronized (atomicInteger) {

            //check the DB again, from within the sync block, just in case another was just created
            ResourceIdMap mapping = repository.getResourceIdMap(serviceId, systemId, resourceType.toString(), sourceId);
            if (mapping == null) {
                mapping = new ResourceIdMap();
                mapping.setServiceId(serviceId);
                mapping.setSystemId(systemId);
                mapping.setResourceType(resourceType.toString());
                mapping.setSourceId(sourceId);
                mapping.setEdsId(UUID.randomUUID());
                repository.insert(mapping);
            }

            ret = mapping.getEdsId();
        }

        synchronized (synchLocks) {
            int val = atomicInteger.decrementAndGet();
            if (val == 0) {
                synchLocks.remove(cacheKey);
            }
        }

        return ret;
    }

    public static UUID getEdsResourceId(UUID serviceId, UUID systemId, ResourceType resourceType, String sourceId) throws Exception {
        String key = createCacheKey(serviceId, systemId, resourceType, sourceId);

        UUID edsId = (UUID)cache.get(key);
        if (edsId == null) {
            ResourceIdMap mapping = repository.getResourceIdMap(serviceId, systemId, resourceType.toString(), sourceId);
            if (mapping == null) {
                return null;
            }

            edsId = mapping.getEdsId();
            try {
                cache.put(key, edsId);
            } catch (Exception ex) {
                LOG.error("Error adding key ["+key+"] value ["+edsId+"] to ID map cache", ex);
            }
        }
        return edsId;
    }*/

    /**
     * maps the ID and all IDs within references in a FHIR resource to unique ones in the EDS space
     * returns true to indicate the resource is new to us, false otherwise
     */
    public static Set<Resource> mapIds(UUID serviceId, UUID systemId, List<Resource> resources) throws Exception {

        Set<Resource> definitelyNewResources = new HashSet<>();

        //first step is to collect all the references that we want to map
        Set<String> referenceValues = new HashSet<>();
        Map<Resource, String> resourceIdSourceReferenceMap = new HashMap<>();
        Set<String> resourceIdSourceReferenceStrings = new HashSet<>();

        for (Resource resource: resources) {

            //find all the references from the resource, using an ID mapper for that resource type
            BaseIdMapper idMapper = getIdMapper(resource);
            idMapper.getResourceReferences(resource, referenceValues);

            //add the resource ID to the map too, in reference format
            String sourceIdReferenceValue = ReferenceHelper.createResourceReference(resource.getResourceType(), resource.getId());
            referenceValues.add(sourceIdReferenceValue);
            resourceIdSourceReferenceMap.put(resource, sourceIdReferenceValue);
            resourceIdSourceReferenceStrings.add(sourceIdReferenceValue);
        }

        //next, we go to the cache and DB to find and create mappings for the references
        Map<String, String> mappings = new HashMap<>();

        Set<String> definitelyNewResourceIdSourceReferences = populateResourceIdMappings(serviceId, systemId, referenceValues, resourceIdSourceReferenceStrings, mappings);

        //finally apply the references
        for (Resource resource: resources) {

            //apply the mappings to all references
            BaseIdMapper idMapper = getIdMapper(resource);
            idMapper.applyReferenceMappings(resource, mappings, true);

            //and apply the mapping to the ID too
            String sourceIdReferenceValue = resourceIdSourceReferenceMap.get(resource);
            String edsIdReferenceValue = mappings.get(sourceIdReferenceValue);
            Reference edsReference = new Reference().setReference(edsIdReferenceValue);
            String edsId = ReferenceHelper.getReferenceId(edsReference);
            resource.setId(edsId);

            //also find out if our resource is new or not
            boolean isNewResource = definitelyNewResourceIdSourceReferences.contains(sourceIdReferenceValue);
            if (isNewResource) {
                definitelyNewResources.add(resource);
            }
        }

        return definitelyNewResources;
    }

    private static Set<String> populateResourceIdMappings(UUID serviceId, UUID systemId, Set<String> sourceReferencesToMap,
                                                                   Set<String> resourceIdSourceReferenceStrings,
                                                                   Map<String, String> mappingsToPopulate) throws Exception {

        Set<String> definitelyNewResourceIdSourceReferences = new HashSet<>();

        //convert the set of reference Strings to a list of Reference objects
        List<Reference> referencesToHitDb = new ArrayList<>();

        for (String sourceReferenceValue: sourceReferencesToMap) {
            Reference sourceReference = new Reference().setReference(sourceReferenceValue);

            UUID edsId = checkCache(serviceId, systemId, sourceReferenceValue);
            if (edsId != null) {
                //if in the cache, construct a new reference String with the ID and add to our map
                ResourceType resourceType = ReferenceHelper.getResourceType(sourceReference);
                String edsReferenceValue = ReferenceHelper.createResourceReference(resourceType, edsId.toString());
                mappingsToPopulate.put(sourceReferenceValue, edsReferenceValue);

            } else {
                //if not found in the cache, we'll need to go to the DB
                referencesToHitDb.add(sourceReference);
            }
        }

        //call to the cache and DB to find existing mappings
        Map<Reference, Reference> referenceMappingsFromDb = repository.findEdsReferencesFromSourceReferences(serviceId, systemId, referencesToHitDb);

        //ensure we've got a mapping for every reference we started with and create where not
        for (Reference sourceReference: referencesToHitDb) {
            String sourceReferenceValue = sourceReference.getReference();
            Reference mappedReference = referenceMappingsFromDb.get(sourceReference);

            UUID edsId = null;
            String mappedReferenceValue = null;

            //if we don't have a pre-existing mapping for this source reference, we need to create one
            if (mappedReference == null) {

                //if we failed to find a mapping for our resource ID, then it means we're saving a new resource
                if (resourceIdSourceReferenceStrings.contains(sourceReferenceValue)) {
                    definitelyNewResourceIdSourceReferences.add(sourceReferenceValue);
                }

                ReferenceComponents comps = ReferenceHelper.getReferenceComponents(sourceReference);
                String resourceType = comps.getResourceType().toString();
                String sourceId = comps.getId();

                edsId = repository.findOrCreateThreadSafe(serviceId, systemId, resourceType, sourceId);
                mappedReferenceValue = ReferenceHelper.createResourceReference(resourceType, edsId.toString());

            } else {
                //if we do have a mapping, extract the ID as a UUID so we can cache it
                String edsIdStr = ReferenceHelper.getReferenceId(mappedReference);
                edsId = UUID.fromString(edsIdStr);
                mappedReferenceValue = mappedReference.getReference();
            }

            //add to our map of mappings
            mappingsToPopulate.put(sourceReferenceValue, mappedReferenceValue);

            //and add to our cache for next time
            addToCache(serviceId, systemId, sourceReferenceValue, edsId);
        }

        return definitelyNewResourceIdSourceReferences;
    }

    /*public static boolean mapIds(UUID serviceId, UUID systemId, Resource resource) throws Exception {

        //get a suitable mapper implementation for the resource type
        BaseIdMapper idMapper = getIdMapper(resource);

        //find all the references from the resource, using an ID mapper for that resource type
        Set<String> referenceValues = new HashSet<>();
        idMapper.getResourceReferences(resource, referenceValues);

        //if we want to map the resource ID, add that to the list of references
        Reference sourceReference = ReferenceHelper.createReferenceExternal(resource);
        String sourceIdReferenceValue = sourceReference.getReference();
        referenceValues.add(sourceIdReferenceValue);

        Map<String, String> mappings = new HashMap<>();
        boolean isNewResource = populateResourceIdMappings(serviceId, systemId, referenceValues, sourceIdReferenceValue, mappings);

        //now apply the references
        idMapper.applyReferenceMappings(resource, mappings, true);

        //and map the ID if we're doing that
        String edsIdReferenceValue = mappings.get(sourceIdReferenceValue);
        Reference edsReference = new Reference().setReference(edsIdReferenceValue);
        String edsId = ReferenceHelper.getReferenceId(edsReference);
        resource.setId(edsId);

        return isNewResource;
    }

    private static boolean populateResourceIdMappings(UUID serviceId, UUID systemId, Set<String> referenceValues, String sourceResourceId, Map<String, String> mappings) throws Exception {

        boolean isNewResource = false;

        //convert the set of reference Strings to a list of Reference objects
        List<Reference> referencesToHitDb = new ArrayList<>();
        for (String sourceReferenceValue: referenceValues) {
            Reference sourceReference = new Reference().setReference(sourceReferenceValue);

            UUID edsId = checkCache(sourceReferenceValue);
            if (edsId == null) {
                //if not found in the cache, we'll need to go to the DB
                referencesToHitDb.add(sourceReference);

            } else {
                //if in the cache, construct a new reference String with the ID
                ResourceType resourceType = ReferenceHelper.getResourceType(sourceReference);
                String edsReferenveValue = ReferenceHelper.createResourceReference(resourceType, edsId.toString());
                mappings.put(sourceReferenceValue, edsReferenveValue);
            }
        }

        //call to the cache and DB to find or create mappings
        Map<Reference, Reference> referenceMappingsFromDb = repository.findEdsReferencesFromSourceReferences(serviceId, systemId, referencesToHitDb);

        //ensure we've got a mapping for every reference we started with and create where not
        for (Reference sourceReference: referencesToHitDb) {
            String sourceReferenceValue = sourceReference.getReference();
            Reference mappedReference = referenceMappingsFromDb.get(sourceReference);

            UUID edsId = null;
            String mappedReferenceValue = null;

            //if we don't have a pre-existing mapping for this source reference, we need to create one
            if (mappedReference == null) {

                //if we failed to find a mapping for our resource ID, then it means we're saving a new resource
                if (sourceResourceId != null
                        && sourceReferenceValue.equals(sourceResourceId)) {
                    isNewResource = true;
                }

                ReferenceComponents comps = ReferenceHelper.getReferenceComponents(sourceReference);
                String resourceType = comps.getResourceType().toString();
                String sourceId = comps.getId();

                edsId = repository.findOrCreateThreadSafe(serviceId, systemId, resourceType, sourceId);
                mappedReferenceValue = ReferenceHelper.createResourceReference(resourceType, edsId.toString());

            } else {
                //if we do have a mapping, extract the ID as a UUID so we can cache it
                String edsIdStr = ReferenceHelper.getReferenceId(mappedReference);
                edsId = UUID.fromString(edsIdStr);
                mappedReferenceValue = mappedReference.getReference();
            }

            //add to our map of mappings
            mappings.put(sourceReferenceValue, mappedReferenceValue);

            //and add to our cache for next time
            addToCache(sourceReferenceValue, edsId);
        }

        return isNewResource;
    }*/

    /**
     * returns the patient ID of the resource or null if it doesn't have one. If called with
     * a resource that doesn't support a patient ID, an exception is thrown
     */
    public static String getPatientId(Resource resource) throws Exception {
        return getIdMapper(resource).getPatientId(resource);
    }

    public static BaseIdMapper getIdMapper(Resource resource) throws Exception {

        BaseIdMapper mapper = idMappers.get(resource.getClass());
        if (mapper == null) {
            String clsName = "org.endeavourhealth.transform.common.idmappers.IdMapper" + resource.getClass().getSimpleName();
            try {
                Class cls = Class.forName(clsName);
                mapper = (BaseIdMapper)cls.newInstance();
                idMappers.put(resource.getClass(), mapper);
            } catch (Exception ex) {
                throw new TransformException("Exception creating ID Mapper for " + clsName, ex);
            }
        }
        return mapper;
    }

    public static Reference convertLocallyUniqueReferenceToEdsReference(Reference localReference, FhirResourceFiler fhirResourceFiler) throws Exception {
        ReferenceComponents components = ReferenceHelper.getReferenceComponents(localReference);
        String locallyUniqueId = components.getId();
        ResourceType resourceType = components.getResourceType();

        String globallyUniqueId = getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(),
                fhirResourceFiler.getSystemId(),
                resourceType,
                locallyUniqueId);

        return ReferenceHelper.createReference(resourceType, globallyUniqueId);
    }

    public static Reference convertEdsReferenceToLocallyUniqueReference(UUID serviceId, Reference edsReference) throws Exception {
        List<Reference> list = new ArrayList<>();
        list.add(edsReference);

        List<Reference> ret = convertEdsReferencesToLocallyUniqueReferences(serviceId, list);
        if (ret.isEmpty()) {
            return null;

        } else {
            return ret.get(0);
        }
    }

    public static List<Reference> convertEdsReferencesToLocallyUniqueReferences(UUID serviceId, List<Reference> edsReferences) throws Exception {
        List<Reference> ret = new ArrayList<>();

        Map<Reference, Reference> map = repository.findSourceReferencesFromEdsReferences(serviceId, edsReferences);
        for (Reference edsReference: edsReferences) {
            Reference sourceReference = map.get(edsReference);

            if (sourceReference == null) {
                LOG.warn("Failed to find Resource ID Mapping for EDS reference " + edsReference);
                //throw new TransformException("Failed to find Resource ID Mapping for resource type " + resourceType.toString() + " ID " + components.getId());

            } else {
                ret.add(sourceReference);
            }
        }

        return ret;
    }

    /*public static Reference convertEdsReferenceToLocallyUniqueReference(Reference edsReference) throws Exception {
        ReferenceComponents components = ReferenceHelper.getReferenceComponents(edsReference);
        ResourceType resourceType = components.getResourceType();
        ResourceIdMap mapping = repository.getResourceIdMapByEdsId(resourceType.toString(), components.getId());
        if (mapping == null) {
            LOG.warn("Failed to find Resource ID Mapping for resource type " + resourceType.toString() + " ID " + components.getId());
            return null;
            //throw new TransformException("Failed to find Resource ID Mapping for resource type " + resourceType.toString() + " ID " + components.getId());
        }

        String emisId = mapping.getSourceId();
        return ReferenceHelper.createReference(resourceType, emisId);
    }*/

    public static void applyExternalReferenceMappings(Resource resource, Map<String, String> idMappings, boolean failForMissingMappings) throws Exception {
        getIdMapper(resource).applyReferenceMappings(resource, idMappings, failForMissingMappings);

        //the ID mapper classes don't do the resource ID itself, so this must be done manually
        Reference sourceReference = ReferenceHelper.createReferenceExternal(resource);
        String sourceIdReferenceValue = sourceReference.getReference();
        String edsIdReferenceValue = idMappings.get(sourceIdReferenceValue);

        if (!Strings.isNullOrEmpty(edsIdReferenceValue)) {
            Reference edsReference = new Reference().setReference(edsIdReferenceValue);
            String edsId = ReferenceHelper.getReferenceId(edsReference);
            resource.setId(edsId);

        } else if (failForMissingMappings) {
            throw new Exception("Failed to find mapping for reference " + sourceIdReferenceValue);
        }
    }

    /**
     * after completing an inbound transform, we clear the cache as there's no point keeping mappings around
     */
    public static void clearCache() {
        try {
            cache.clear();
        } catch (CacheException ce) {
            LOG.error("Error clearing cache", ce);
        }
    }

}
