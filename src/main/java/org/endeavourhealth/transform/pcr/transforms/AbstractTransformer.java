package org.endeavourhealth.transform.pcr.transforms;

import com.google.common.base.Strings;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.ExchangeBatchExtraResourceDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.PcrIdDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransformer.class);
    private static final ParserPool PARSER_POOL = new ParserPool();

    private static JCS idCache = null;
    private static JCS instanceCache = null;
    private static final ReentrantLock onDemandLock = new ReentrantLock();

    static {
        try {

            //by default the Java Caching System has a load of logging enabled, which is really slow, so turn it off
            //not longer required, since it no longer uses log4J and the new default doesn't have debug enabled
            /*org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.apache.jcs");
            logger.setLevel(org.apache.log4j.Level.OFF);*/

            idCache = JCS.getInstance("PcrResourceMap");
            instanceCache = JCS.getInstance("PcrInstanceMap");

        } catch (CacheException ex) {
            throw new RuntimeException("Error initialising cache", ex);
        }
    }

    public void transformResources(List<ResourceWrapper> resources,
                                   AbstractPcrCsvWriter csvWriter,
                                   PcrTransformParams params) throws Exception {

        Map<ResourceWrapper, Long> pcrIds = mapIds(params.getConfigName(), resources, shouldAlwaysTransform(), params);

        for (ResourceWrapper resource : resources) {

            try {
                Long pcrId = pcrIds.get(resource);
                if (pcrId == null) {

                    /*f (resource.getResourceType().equals("Organisation")) {
                        LOG.trace("NOT transforming Organisation " + resource.getResourceId() + " as no PCR ID");
                    }*/

                    //if we've got a null PCR ID, then it means the ID mapper doesn't want us to do anything
                    //with the resource (e.g. ihe resource is a duplicate instance of another Organisation that is already transformed)
                    continue;
                }

                //check to see if we've already transformed this resource in this batch already,
                //which can happen for dependent items like orgs and practitioners
                ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());
                String resourceId = resource.getResourceId().toString();
                Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceId);

                if (!params.hasResourceBeenTransformedAddIfNot(resourceReference)) {

                    if (resource.isDeleted()) {
                        transformResourceDelete(pcrId, csvWriter, params);

                    } else {
                        Resource fhir = FhirResourceHelper.deserialiseResouce(resource);
                        transformResource(pcrId, fhir, csvWriter, params);
                    }
                }

            } catch (Exception ex) {
                throw new TransformException("Exception transforming " + resource.getResourceType() + " " + resource.getResourceId(), ex);
            }
        }
    }

    //defines whether the resources covered by this transformer should ALWAYS be transformed (e.g. patient data)
    //or only transformed if something refers to it (e.g. orgs and practitioners)
    public abstract boolean shouldAlwaysTransform();

    protected abstract void transformResource(Long pcrId,
                                              Resource resource,
                                              AbstractPcrCsvWriter csvWriter,
                                              PcrTransformParams params) throws Exception;

    protected void transformResourceDelete(Long pcrId,
                                           AbstractPcrCsvWriter csvWriter,
                                           PcrTransformParams params) throws Exception {

        csvWriter.writeDelete(pcrId.longValue());
    }

    protected static Integer convertDatePrecision(TemporalPrecisionEnum precision) throws Exception {
        return Integer.valueOf(precision.getCalendarConstant());
    }



    /*protected static Long findEnterpriseId(PcrTransformParams params, Resource resource) throws Exception {
        String resourceType = resource.getResourceType().toString();
        String resourceId = resource.getId();
        return findEnterpriseId(params, resourceType, resourceId);
    }*/

    protected static Long findPcrId(PcrTransformParams params, Reference reference) throws Exception {
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        String resourceType = comps.getResourceType().toString();
        String resourceId = comps.getId();
        return findPcrId(params, resourceType, resourceId);
    }

    /*protected static Long findEnterpriseId(PcrTransformParams params, ResourceWrapper resource) throws Exception {
        return findEnterpriseId(params, resource.getResourceType(), resource.getResourceId().toString());
    }*/

    public static Long findPcrId(PcrTransformParams params, String resourceType, String resourceId) throws Exception {
        Long ret = checkCacheForId(params.getConfigName(), resourceType, resourceId);
        if (ret == null) {
            PcrIdDalI pcrIdDal = DalProvider.factoryPcrIdDal(params.getConfigName());
            ret = pcrIdDal.findPcrId(resourceType, resourceId);
            //ret = idMappingRepository.getEnterpriseIdMappingId(enterpriseTableName, resourceType, resourceId);
        }
        return ret;
    }

       protected static Long findOrCreatePcrId(PcrTransformParams params, Reference reference) throws Exception {
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        String resourceType = comps.getResourceType().toString();
        String resourceId = comps.getId();
        return findOrCreatePcrId(params, resourceType, resourceId);
    }

    public static Long findOrCreatePcrId(PcrTransformParams params, String resourceType, String resourceId) throws Exception {
        Long ret = checkCacheForId(params.getConfigName(), resourceType, resourceId);
        if (ret == null) {
            PcrIdDalI pcrIdDal = DalProvider.factoryPcrIdDal(params.getConfigName());
            ret = pcrIdDal.findOrCreatePcrId(resourceType, resourceId);
            addIdToCache(params.getConfigName(), resourceType, resourceId, ret);
        }
        return ret;
    }

    private static String createCacheKey(String pcrConfigName, String resourceType, String resourceId) {
        StringBuilder sb = new StringBuilder();
        sb.append(pcrConfigName);
        sb.append(":");
        sb.append(resourceType);
        sb.append("/");
        sb.append(resourceId);
        return sb.toString();
    }

    private static Long checkCacheForId(String pcrConfigName, String resourceType, String resourceId) throws Exception {
        return (Long) idCache.get(createCacheKey(pcrConfigName, resourceType, resourceId));
    }

    private static void addIdToCache(String pcrConfigName, String resourceType, String resourceId, Long toCache) throws Exception {
        if (toCache == null) {
            return;
        }
        idCache.put(createCacheKey(pcrConfigName, resourceType, resourceId), toCache);
    }



    protected static Resource findResource(Reference reference,
                                           PcrTransformParams params) throws Exception {

        String referenceStr = reference.getReference();
        Map<String, ResourceWrapper> hmAllResources = params.getAllResources();

        //look in our resources map first
        ResourceWrapper ret = hmAllResources.get(referenceStr);
        if (ret != null) {
            if (ret.isDeleted()) {
                return null;
            } else {
                return FhirResourceHelper.deserialiseResouce(ret);
            }
        } else {

            //if not in our map, then hit the DB
            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            return resourceDal.getCurrentVersionAsResource(params.getServiceId(), comps.getResourceType(), comps.getId());
        }
    }


    private static Map<ResourceWrapper, Long> mapIds(String pcrConfigName, List<ResourceWrapper> resources, boolean createIfNotFound, PcrTransformParams params) throws Exception {

        Map<ResourceWrapper, Long> ret = new HashMap<>();

        //first, try to find existing IDs for our resources in our memory cache
        findPcrIdsInCache(pcrConfigName, resources, ret);

        List<ResourceWrapper> resourcesToFindOnDb = new ArrayList<>();
        List<ResourceWrapper> resourcesToFindOrCreateOnDb = new ArrayList<>();

        for (ResourceWrapper resource : resources) {

            //if our memory cache contained this ID, then skip it
            if (ret.containsKey(resource)) {
                continue;
            }

            //if this resource is mapped to a different instance of the same concept (e.g. it's a duplicate instance
            //of an organisation), then we don't want to generate an ID for it
            if (params.isUseInstanceMapping()
                    && isResourceMappedToAnotherInstance(resource, params)) {
                continue;
            }

            //if we didn't find an ID in memory, then we'll either want to simply find on the DB or find and create on the DB
            if (resource.isDeleted()
                    || !createIfNotFound) {

                resourcesToFindOnDb.add(resource);
            } else {

                resourcesToFindOrCreateOnDb.add(resource);
            }
        }

        //look up any resources we need
        if (!resourcesToFindOnDb.isEmpty()) {
            PcrIdDalI pcrIdDal = DalProvider.factoryPcrIdDal(pcrConfigName);
            pcrIdDal.findPcrIds(resourcesToFindOnDb, ret);

            //add them to our cache
            for (ResourceWrapper resource : resourcesToFindOnDb) {
                Long pcrId = ret.get(resource);
                addIdToCache(pcrConfigName, resource.getResourceType(), resource.getResourceId().toString(), pcrId);
            }
        }

        //lookup and create any resources we need
        if (!resourcesToFindOrCreateOnDb.isEmpty()) {
            PcrIdDalI pcrIdDal = DalProvider.factoryPcrIdDal(pcrConfigName);
            pcrIdDal.findOrCreatePcrIds(resourcesToFindOrCreateOnDb, ret);

            //add them to our cache
            for (ResourceWrapper resource : resourcesToFindOrCreateOnDb) {
                Long pcrId = ret.get(resource);
                addIdToCache(pcrConfigName, resource.getResourceType(), resource.getResourceId().toString(), pcrId);
            }
        }

        return ret;
    }

    private static boolean isResourceMappedToAnotherInstance(ResourceWrapper resource, PcrTransformParams params) throws Exception {

        ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());
        UUID resourceId = resource.getResourceId();

        //only orgs and practitioners are mapped to other instances
        if (!resourceType.equals(ResourceType.Organization.toString())
                && !resourceType.equals(ResourceType.Practitioner.toString())) {
            return false;
        }

        UUID mappedResourceId = checkInstanceMapCache(params.getConfigName(), resourceType, resourceId);
        if (mappedResourceId == null) {

            //EnterpriseIdDalI instanceMapper = DalProvider.factoryEnterpriseIdDal(params.getConfigName());
            PcrIdDalI instanceMapper = DalProvider.factoryPcrIdDal(params.getConfigName());

            mappedResourceId = instanceMapper.findInstanceMappedId(resourceType, resourceId);

            //if we've not got a mapping, then we need to create one from our resource data
            if (mappedResourceId == null) {
                String mappingValue = findInstanceMappingValue(resource, params);
                mappedResourceId = instanceMapper.findOrCreateInstanceMappedId(resourceType, resourceId, mappingValue);
            }

            addToInstanceMapCache(params.getConfigName(), resourceType, resourceId, mappedResourceId);
        }

        //if the mapped ID is different to the resource ID then it's mapped to another instance
        return !mappedResourceId.equals(resourceId);
    }


    private static String findInstanceMappingValue(ResourceWrapper resourceWrapper, PcrTransformParams params) throws Exception {

        Resource resource = null;
        if (!resourceWrapper.isDeleted()) {
            String json = resourceWrapper.getResourceData();
            resource = FhirResourceHelper.deserialiseResouce(json);
        }

        return findInstanceMappingValue(resource, params);
    }

    public static String findInstanceMappingValue(Resource resource, PcrTransformParams params) throws Exception {

        //if our resource is deleted return null so we just map the resource to itself
        if (resource == null) {
            return null;
        }

        if (resource instanceof Organization) {
            //for orgs, we use the ODS code
            Organization fhirOrg = (Organization) resource;
            return IdentifierHelper.findOdsCode(fhirOrg);

        } else if (resource instanceof Practitioner) {
            //we don't have any unique identifier for a person, so use a combination
            //of their name PLUS their org ods code
            Practitioner fhirPractitioner = (Practitioner) resource;
            if (fhirPractitioner.hasName()) {
                HumanName humanName = fhirPractitioner.getName();
                String name = humanName.getText();

                if (!Strings.isNullOrEmpty(name)
                        && fhirPractitioner.hasPractitionerRole()) {

                    if (fhirPractitioner.getPractitionerRole().size() > 1) {
                        throw new Exception("Don't know how to handle practitioners with more than one role: " + fhirPractitioner.getResourceType() + " " + fhirPractitioner.getId());
                    }

                    Practitioner.PractitionerPractitionerRoleComponent fhirRole = fhirPractitioner.getPractitionerRole().get(0);
                    if (fhirRole.hasManagingOrganization()) {
                        Reference orgReference = fhirRole.getManagingOrganization();
                        Organization fhirOrganisation = (Organization) findResource(orgReference, params);
                        if (fhirOrganisation != null) {
                            String odsCode = IdentifierHelper.findOdsCode(fhirOrganisation);
                            if (!Strings.isNullOrEmpty(odsCode)) {
                                return name + "@" + odsCode;

                            }
                        }
                    }
                }
            }

            //if we don't have enough to make a unique value, then return null
            return null;

        } else {
            throw new IllegalArgumentException("Should only be mapping instances for practitioners and organisations");
        }
    }

    private static void findPcrIdsInCache(String pcrConfigName, List<ResourceWrapper> resources, Map<ResourceWrapper, Long> ids) throws Exception {

        for (ResourceWrapper resource : resources) {
            Long cachedId = checkCacheForId(pcrConfigName, resource.getResourceType(), resource.getResourceId().toString());
            if (cachedId != null) {
                ids.put(resource, cachedId);
            }
        }
    }

    /**
     * transforms a dependent resource not necessarily in the exchange batch we're currently transforming,
     * e.g. transform a practitioner that's referenced by an observation in this batch
     */
    protected Long transformOnDemandAndMapId(Reference reference,
                                             PcrTransformParams params) throws Exception {

        //if we've already got an ID for this resource we must have previously transformed it
        //so we don't need to forcibly transform it now
        Long existingPcrId = findPcrId(params, reference);
        if (existingPcrId != null) {
            return existingPcrId;
        }

        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        ResourceType resourceType = comps.getResourceType();
        UUID resourceId = UUID.fromString(comps.getId());

        //we've have multiple threads potentially trying to transform the same dependent resource (e.g. practitioner)
        //so we need to sync on something to ensure we only
        try {
            onDemandLock.lock();

            //see if this resource is mapped to another instance of the same concept (e.g. organisation),
            //in which case we want to use the PCR ID for that OTHER instance
            if (params.isUseInstanceMapping()
                    && (resourceType == ResourceType.Organization
                    || resourceType == ResourceType.Practitioner)) {

                UUID mappedResourceId = checkInstanceMapCache(params.getConfigName(), resourceType, resourceId);
                if (mappedResourceId == null) {

                    PcrIdDalI pcrIdDal = DalProvider.factoryPcrIdDal(params.getConfigName());
                    mappedResourceId = pcrIdDal.findInstanceMappedId(resourceType, resourceId);

                    //if we've not got a mapping, then we need to create one from our resource data
                    if (mappedResourceId == null) {

                        Resource fhirResource = findResource(reference, params);
                        if (fhirResource == null) {
                            //if it's deleted then just return null since there's no point assigning an ID
                            return null;
                        }

                        String mappingValue = findInstanceMappingValue(fhirResource, params);
                        mappedResourceId = pcrIdDal.findOrCreateInstanceMappedId(resourceType, resourceId, mappingValue);
                    }

                    addToInstanceMapCache(params.getConfigName(), resourceType, resourceId, mappedResourceId);
                }

                //if our mapped ID is different to our proper ID, then we don't need to transform that
                //other resource, as it will already have been done, so we can just return it's PCR ID
                if (!mappedResourceId.equals(resourceId)) {
                    Long mappedInstancePcrId = findPcrId(params, resourceType.toString(), mappedResourceId.toString());
                    if (mappedInstancePcrId == null) {
                        //if we've just started processing the first exchange for an org that's taking over the
                        //instance map, there's a chance we'll catch it mid-way through taking over, in which
                        //case we should just give a second and try again, throwing an error if we fail
                        Thread.sleep(1000);
                        mappedInstancePcrId = findPcrId(params, resourceType.toString(), mappedResourceId.toString());
                        if (mappedInstancePcrId == null) {
                            throw new TransformException("Failed to find PCR ID for mapped instance " + resourceType.toString() + " " + mappedResourceId.toString() + " and original ID " + resourceId);
                        }
                    }
                    return mappedInstancePcrId;
                }
            }

            if (params.hasResourceBeenTransformedAddIfNot(reference)) {
                //if we've already transformed the resource, which could happen because the transform is multi-threaded,
                //then have another look for the PCR ID as it must exist
                return findPcrId(params, reference);
            }

            //if we've got here, we actually want to transform the referred to resource, so retrieve from the DB
            Resource fhir = findResource(reference, params);
            if (fhir == null) {
                //if it's deleted then just return null since there's no point assigning an ID
                return null;
            }


            AbstractTransformer transformer = FhirToPcrCsvTransformer.createTransformerForResourceType(resourceType);
            if (transformer == null) {
                throw new TransformException("No transformer found for resource " + reference.getReference());
            }

            //generate a new PCR ID for our resource. So we have an audit of this, and can recover if we
            //kill the queue reader at this point, we also need to store our resource's ID in the exchange_batch_extra_resource table
            ExchangeBatchExtraResourceDalI exchangeBatchExtraResourceDalI = DalProvider.factoryExchangeBatchExtraResourceDal(params.getConfigName());
            //LOG.info("Saving extra resource exchange " + params.getExchangeId() + " batch " + params.getBatchId() + " resource type " + resourceType + " id " + resourceId);
            exchangeBatchExtraResourceDalI.saveExtraResource(params.getExchangeId(), params.getBatchId(), resourceType, resourceId);

            //then generate the new ID
            Long pcrId = findOrCreatePcrId(params, resourceType.toString(), fhir.getId());

            //and transform the resource
            AbstractPcrCsvWriter csvWriter = FhirToPcrCsvTransformer.findCsvWriterForResourceType(resourceType, params);
            transformer.transformResource(pcrId, fhir, csvWriter, params);

            return pcrId;

        } finally {
            onDemandLock.unlock();
        }
    }

    private static UUID checkInstanceMapCache(String pcrConfigName, ResourceType resourceType, UUID resourceId) {
        Object key = createCacheKey(pcrConfigName, resourceType.toString(), resourceId.toString());
        return (UUID) instanceCache.get(key);
    }

    private static void addToInstanceMapCache(String pcrConfigName, ResourceType resourceType, UUID resourceId, UUID mappedResourceId) throws Exception {
        Object key = createCacheKey(pcrConfigName, resourceType.toString(), resourceId.toString());
        instanceCache.put(key, mappedResourceId);
    }



}
