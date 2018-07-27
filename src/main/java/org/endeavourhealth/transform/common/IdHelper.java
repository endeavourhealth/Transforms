package org.endeavourhealth.transform.common;

import com.google.common.base.Strings;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.core.exceptions.TransformException;
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

    public static String getOrCreateEdsResourceIdString(UUID serviceId, ResourceType resourceType, String sourceId) throws Exception {
        return getOrCreateEdsResourceId(serviceId, resourceType, sourceId).toString();
    }

    public static UUID getOrCreateEdsResourceId(UUID serviceId, ResourceType resourceType, String sourceId) throws Exception {
        return getOrCreateEdsResourceId(serviceId, resourceType, sourceId, true, null);
    }

    public static UUID getOrCreateEdsResourceId(UUID serviceId, ResourceType resourceType, String sourceId, UUID explicitIdToUse) throws Exception {
        return getOrCreateEdsResourceId(serviceId, resourceType, sourceId, true, explicitIdToUse);
    }

    public static String getEdsResourceIdAsString(UUID serviceId, ResourceType resourceType, String sourceId) throws Exception {
        return getEdsResourceId(serviceId, resourceType, sourceId).toString();
    }

    public static UUID getEdsResourceId(UUID serviceId, ResourceType resourceType, String sourceId) throws Exception {
        return getOrCreateEdsResourceId(serviceId, resourceType, sourceId, false, null);
    }

    private static UUID getOrCreateEdsResourceId(UUID serviceId, ResourceType resourceType, String sourceId, boolean createIfNotFound, UUID explicitIdToUse) throws Exception {
        Reference sourceReference = ReferenceHelper.createReference(resourceType, sourceId);
        String sourceReferenceValue = sourceReference.getReference();

        //check our cache first
        UUID edsId = checkCache(serviceId, sourceReferenceValue);
        if (edsId == null) {

            //if not in the cache, hit the DB
            List<Reference> sourceReferences = new ArrayList<>();
            sourceReferences.add(sourceReference);

            Map<Reference, Reference> map = repository.findEdsReferencesFromSourceReferences(serviceId, sourceReferences);
            Reference edsReference = map.get(sourceReference);
            if (edsReference == null) {

                if (createIfNotFound) {
                    //if definitely no mapping on the DB, create and save a new ID
                    //passing in the explicit ID to use (if null, it'll just generate a new ID)
                    edsId = repository.findOrCreate(serviceId, resourceType.toString(), sourceId, explicitIdToUse);
                } else {
                    return null;
                }

            } else {
                String edsIdStr = ReferenceHelper.getReferenceId(edsReference);
                edsId = UUID.fromString(edsIdStr);
            }

            addToCache(serviceId, sourceReferenceValue, edsId);
        }

        return edsId;
    }

    private static UUID checkCache(UUID serviceId, String referenceValue) {
        String cacheKey = createCacheKey(serviceId, referenceValue);
        return (UUID)cache.get(cacheKey);
    }

    private static void addToCache(UUID serviceId, String referenceValue, UUID id) {
        String cacheKey = createCacheKey(serviceId, referenceValue);
        try {
            cache.put(cacheKey, id);
        } catch (Exception ex) {
            LOG.error("Error adding key ["+ referenceValue + "] value [" + id + "] to ID map cache", ex);
        }
    }

    private static String createCacheKey(UUID serviceId, String referenceValue) {
        //quick optimisation to cut on string creation
        StringBuilder sb = new StringBuilder();
        sb.append(serviceId.toString());
        sb.append("/");
        sb.append(referenceValue);
        return sb.toString();
    }


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
            //LOG.debug("Mapped " + resource.getResourceType() + " ID from " + sourceIdReferenceValue + " to " + edsIdReferenceValue);

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

            UUID edsId = checkCache(serviceId, sourceReferenceValue);
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
        Map<Reference, Reference> referenceMappingsFromDb = repository.findEdsReferencesFromSourceReferences(serviceId, referencesToHitDb);

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

                edsId = repository.findOrCreate(serviceId, resourceType, sourceId);
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
            addToCache(serviceId, sourceReferenceValue, edsId);
        }

        return definitelyNewResourceIdSourceReferences;
    }


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

    public static Reference convertLocallyUniqueReferenceToEdsReference(Reference localReference, HasServiceSystemAndExchangeIdI fhirResourceFiler) throws Exception {
        ReferenceComponents components = ReferenceHelper.getReferenceComponents(localReference);
        String locallyUniqueId = components.getId();
        ResourceType resourceType = components.getResourceType();

        String globallyUniqueId = getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(), resourceType, locallyUniqueId);

        return ReferenceHelper.createReference(resourceType, globallyUniqueId);
    }

    public static Reference convertEdsReferenceToLocallyUniqueReference(HasServiceSystemAndExchangeIdI serviceSystemAndExchangeIdI, Reference edsReference) throws Exception {
        List<Reference> list = new ArrayList<>();
        list.add(edsReference);

        List<Reference> ret = convertEdsReferencesToLocallyUniqueReferences(serviceSystemAndExchangeIdI, list);
        if (ret.isEmpty()) {
            return null;

        } else {
            return ret.get(0);
        }
    }

    public static List<Reference> convertEdsReferencesToLocallyUniqueReferences(HasServiceSystemAndExchangeIdI serviceSystemAndExchangeIdI, List<Reference> edsReferences) throws Exception {
        List<Reference> ret = new ArrayList<>();

        UUID serviceId = serviceSystemAndExchangeIdI.getServiceId();
        Map<Reference, Reference> map = repository.findSourceReferencesFromEdsReferences(serviceId, edsReferences);
        for (Reference edsReference: edsReferences) {
            Reference sourceReference = map.get(edsReference);

            if (sourceReference == null) {
                TransformWarnings.log(LOG, serviceSystemAndExchangeIdI, "Failed to find Resource ID Mapping for EDS reference {}", edsReference.getReference());
                LOG.warn("Failed to find Resource ID Mapping for EDS reference " + edsReference.getReference());
                //throw new TransformException("Failed to find Resource ID Mapping for resource type " + resourceType.toString() + " ID " + components.getId());

            } else {
                ret.add(sourceReference);
            }
        }

        return ret;
    }

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
