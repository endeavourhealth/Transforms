package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberInstanceMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberResourceMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.FhirToEnterpriseCsvTransformer;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.endeavourhealth.transform.subscriber.transforms.AbstractSubscriberTransformer;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractEnterpriseTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEnterpriseTransformer.class);
    private static final ParserPool PARSER_POOL = new ParserPool();

    //private static final EnterpriseIdMapRepository idMappingRepository = new EnterpriseIdMapRepository();
    private static JCS idCache = null;
    private static JCS instanceCache = null;
    private static final ReentrantLock onDemandLock = new ReentrantLock();

    static {
        try {

            //by default the Java Caching System has a load of logging enabled, which is really slow, so turn it off
            //not longer required, since it no longer uses log4J and the new default doesn't have debug enabled
            /*org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.apache.jcs");
            logger.setLevel(org.apache.log4j.Level.OFF);*/

            idCache = JCS.getInstance("EnterpriseResourceMap");
            instanceCache = JCS.getInstance("EnterpriseInstanceMap");

        } catch (CacheException ex) {
            throw new RuntimeException("Error initialising cache", ex);
        }
    }

    public void transformResources(List<ResourceWrapper> resourceWrappers,
                                    AbstractEnterpriseCsvWriter csvWriter,
                                    EnterpriseTransformHelper params) throws Exception {

        validateResources(resourceWrappers);

        LOG.trace("Transforming " + resourceWrappers.size() + " " + getExpectedResourceType() + " resources to " + params.getEnterpriseConfigName());
        Map<ResourceWrapper, Long> enterpriseIds = mapIds(params.getEnterpriseConfigName(), resourceWrappers, shouldAlwaysTransform(), params);

        for (ResourceWrapper resourceWrapper: resourceWrappers) {

            try {
                Long enterpriseId = enterpriseIds.get(resourceWrapper);
                if (enterpriseId == null) {
                    //if we've got a null enterprise ID, then it means the ID mapper doesn't want us to do anything
                    //with the resource (e.g. ihe resource is a duplicate instance of another Organisation that is already transformed)
                    params.setResourceAsSkipped(resourceWrapper);
                    continue;
                }

                //check to see if we've already transformed this resource in this batch already,
                //which can happen for dependent items like orgs and practitioners
                if (!params.hasResourceBeenTransformedAddIfNot(resourceWrapper)) {
                    transformResource(enterpriseId, resourceWrapper, csvWriter, params);
                }

            } catch (Exception ex) {
                throw new TransformException("Exception transforming " + resourceWrapper.getResourceType() + " " + resourceWrapper.getResourceId(), ex);
            }
        }
    }

    /**
     * validates that all resources match the type that we expect for this transformer
     */
    private void validateResources(List<ResourceWrapper> resources) throws Exception {
        ResourceType resourceType = getExpectedResourceType();
        String resourceTypeStr = resourceType.toString();

        for (ResourceWrapper wrapper: resources) {
            if (!wrapper.getResourceType().equals(resourceTypeStr)) {
                throw new TransformException("Trying to transform a " + wrapper.getResourceType() + " in " + getClass().getSimpleName());
            }
        }
    }

    protected abstract ResourceType getExpectedResourceType();

    /**
     * 2019/09/18 Kambiz advised the confidential data should still be sent to subscribers
     */
    /*public static boolean isConfidential(Resource fhir) {
        DomainResource resource = (DomainResource)fhir;
        BooleanType bt = (BooleanType)ExtensionConverter.findExtensionValue(resource, FhirExtensionUri.IS_CONFIDENTIAL);
        if (bt == null
                || !bt.hasValue()) {
            return false;
        } else {
            return bt.getValue().booleanValue();
        }
    }*/



    //defines whether the resources covered by this transformer should ALWAYS be transformed (e.g. patient data)
    //or only transformed if something refers to it (e.g. orgs and practitioners)
    public abstract boolean shouldAlwaysTransform();

    protected abstract void transformResource(Long enterpriseId,
                                   ResourceWrapper resourceWrapper,
                                   AbstractEnterpriseCsvWriter csvWriter,
                                   EnterpriseTransformHelper params) throws Exception;

    /*protected void transformResourceDelete(Long enterpriseId,
                                           AbstractEnterpriseCsvWriter csvWriter,
                                           EnterpriseTransformHelper params) throws Exception {

        csvWriter.writeDelete(enterpriseId.longValue());
    }*/

    protected static Integer convertDatePrecision(TemporalPrecisionEnum precision) throws Exception {
        return Integer.valueOf(precision.getCalendarConstant());
    }



    /*protected static Long findEnterpriseId(EnterpriseTransformParams params, Resource resource) throws Exception {
        String resourceType = resource.getResourceType().toString();
        String resourceId = resource.getId();
        return findEnterpriseId(params, resourceType, resourceId);
    }*/

    private static Long findEnterpriseId(EnterpriseTransformHelper params, Reference reference) throws Exception {
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        String resourceType = comps.getResourceType().toString();
        String resourceId = comps.getId();
        return findEnterpriseId(params, resourceType, resourceId);
    }

    /*protected static Long findEnterpriseId(EnterpriseTransformParams params, ResourceWrapper resource) throws Exception {
        return findEnterpriseId(params, resource.getResourceType(), resource.getResourceId().toString());
    }*/

    public static Long findEnterpriseId(EnterpriseTransformHelper params, String resourceType, String resourceId) throws Exception {
        Long ret = checkCacheForId(params.getEnterpriseConfigName(), resourceType, resourceId);
        if (ret == null) {
            SubscriberResourceMappingDalI enterpriseIdDal = DalProvider.factorySubscriberResourceMappingDal(params.getEnterpriseConfigName());
            ret = enterpriseIdDal.findEnterpriseIdOldWay(resourceType, resourceId);
            //ret = idMappingRepository.getEnterpriseIdMappingId(enterpriseTableName, resourceType, resourceId);
        }
        return ret;
    }

    /*protected static Long findOrCreateEnterpriseId(EnterpriseTransformParams params, ResourceWrapper resource) throws Exception {
        String resourceType = resource.getResourceType();
        String resourceId = resource.getResourceId().toString();
        return findOrCreateEnterpriseId(params, resourceType, resourceId);
    }*/

    protected static Long findOrCreateEnterpriseId(EnterpriseTransformHelper params, Reference reference) throws Exception {
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        String resourceType = comps.getResourceType().toString();
        String resourceId = comps.getId();
        return findOrCreateEnterpriseId(params, resourceType, resourceId);
    }

    public static Long findOrCreateEnterpriseId(EnterpriseTransformHelper params, String resourceType, String resourceId) throws Exception {
        Long ret = checkCacheForId(params.getEnterpriseConfigName(), resourceType, resourceId);
        if (ret == null) {
            SubscriberResourceMappingDalI enterpriseIdDal = DalProvider.factorySubscriberResourceMappingDal(params.getEnterpriseConfigName());
            ret = enterpriseIdDal.findOrCreateEnterpriseIdOldWay(resourceType, resourceId);

            addIdToCache(params.getEnterpriseConfigName(), resourceType, resourceId, ret);
        }
        return ret;
    }

    private static String createCacheKey(String enterpriseConfigName, String resourceType, String resourceId) {
        StringBuilder sb = new StringBuilder();
        sb.append(enterpriseConfigName);
        sb.append(":");
        sb.append(resourceType);
        sb.append("/");
        sb.append(resourceId);
        return sb.toString();
    }

    private static Long checkCacheForId(String enterpriseConfigName, String resourceType, String resourceId) throws Exception {
        return (Long)idCache.get(createCacheKey(enterpriseConfigName, resourceType, resourceId));
    }

    private static void addIdToCache(String enterpriseConfigName, String resourceType, String resourceId, Long toCache) throws Exception {
        if (toCache == null) {
            return;
        }
        idCache.put(createCacheKey(enterpriseConfigName, resourceType, resourceId), toCache);
    }



    private static Map<ResourceWrapper, Long> mapIds(String enterpriseConfigName, List<ResourceWrapper> resources, boolean createIfNotFound, EnterpriseTransformHelper params) throws Exception {

        Map<ResourceWrapper, Long> ret = new HashMap<>();

        //first, try to find existing IDs for our resources in our memory cache
        findEnterpriseIdsInCache(enterpriseConfigName, resources, ret);

        List<ResourceWrapper> resourcesToFindOnDb = new ArrayList<>();
        List<ResourceWrapper> resourcesToFindOrCreateOnDb = new ArrayList<>();

        for (ResourceWrapper resource: resources) {

            //if our memory cache contained this ID, then skip it
            if (ret.containsKey(resource)) {
                continue;
            }

            //if this resource is mapped to a different instance of the same concept (e.g. it's a duplicate instance
            //of an organisation), then we don't want to generate an ID for it
            if (isResourceMappedToAnotherInstance(resource, params)) {
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
            SubscriberResourceMappingDalI enterpriseIdDal = DalProvider.factorySubscriberResourceMappingDal(params.getEnterpriseConfigName());
            enterpriseIdDal.findEnterpriseIdsOldWay(resourcesToFindOnDb, ret);

            //add them to our cache
            for (ResourceWrapper resource: resourcesToFindOnDb) {
                Long enterpriseId = ret.get(resource);
                addIdToCache(enterpriseConfigName, resource.getResourceType(), resource.getResourceId().toString(), enterpriseId);
            }
        }

        //lookup and create any resources we need
        if (!resourcesToFindOrCreateOnDb.isEmpty()) {
            SubscriberResourceMappingDalI enterpriseIdDal = DalProvider.factorySubscriberResourceMappingDal(params.getEnterpriseConfigName());
            enterpriseIdDal.findOrCreateEnterpriseIdsOldWay(resourcesToFindOrCreateOnDb, ret);

            //add them to our cache
            for (ResourceWrapper resource: resourcesToFindOrCreateOnDb) {
                Long enterpriseId = ret.get(resource);
                addIdToCache(enterpriseConfigName, resource.getResourceType(), resource.getResourceId().toString(), enterpriseId);
            }
        }

        return ret;
    }

    private static boolean isResourceMappedToAnotherInstance(ResourceWrapper resource, EnterpriseTransformHelper params) throws Exception {

        ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());
        UUID resourceId = resource.getResourceId();

        //only orgs and practitioners are mapped to other instances
        if (!resourceType.equals(ResourceType.Organization.toString())
                && !resourceType.equals(ResourceType.Practitioner.toString())) {
            return false;
        }

        UUID mappedResourceId = checkInstanceMapCache(params.getEnterpriseConfigName(), resourceType, resourceId);
        if (mappedResourceId == null) {

            SubscriberInstanceMappingDalI instanceMapper = DalProvider.factorySubscriberInstanceMappingDal(params.getEnterpriseConfigName());
            mappedResourceId = instanceMapper.findInstanceMappedId(resourceType, resourceId);

            //if we've not got a mapping, then we need to create one from our resource data
            if (mappedResourceId == null) {
                String mappingValue = findInstanceMappingValue(resource, params);
                mappedResourceId = instanceMapper.findOrCreateInstanceMappedId(resourceType, resourceId, mappingValue);
            }

            addToInstanceMapCache(params.getEnterpriseConfigName(), resourceType, resourceId, mappedResourceId);
        }

        //if the mapped ID is different to the resource ID then it's mapped to another instance
        return !mappedResourceId.equals(resourceId);
    }



    private static String findInstanceMappingValue(ResourceWrapper resourceWrapper, EnterpriseTransformHelper params) throws Exception {

        Resource resource = null;
        if (!resourceWrapper.isDeleted()) {
            String json = resourceWrapper.getResourceData();
            resource = FhirResourceHelper.deserialiseResouce(json);
        }

        return findInstanceMappingValue(resource, params);
    }

    public static String findInstanceMappingValue(Resource resource, EnterpriseTransformHelper params) throws Exception {

        //if our resource is deleted return null so we just map the resource to itself
        if (resource == null) {
            return null;
        }

        if (resource instanceof Organization) {
            //for orgs, we use the ODS code
            Organization fhirOrg = (Organization)resource;
            return IdentifierHelper.findOdsCode(fhirOrg);

        } else if (resource instanceof Practitioner) {
            //we don't have any unique identifier for a person, so use a combination
            //of their name PLUS their org ods code
            Practitioner fhirPractitioner = (Practitioner)resource;
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
                        ResourceWrapper orgWrapper = params.findOrRetrieveResource(orgReference);
                        if (orgWrapper != null) {
                            Organization fhirOrganisation = (Organization) FhirSerializationHelper.deserializeResource(orgWrapper.getResourceData());
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

    private static void findEnterpriseIdsInCache(String enterpriseConfigName, List<ResourceWrapper> resources, Map<ResourceWrapper, Long> ids) throws Exception {

        for (ResourceWrapper resource: resources) {
            Long cachedId = checkCacheForId(enterpriseConfigName, resource.getResourceType(), resource.getResourceId().toString());
            if (cachedId != null) {
                ids.put(resource, cachedId);
            }
        }
    }

    /**
     * transforms a dependent resource not necessarily in the exchcnge batch we're currently transforming,
     * e.g. transform a practitioner that's referenced by an observation in this batch
     */
    protected static Long transformOnDemandAndMapId(Reference reference, EnterpriseTransformHelper params) throws Exception {

        //if we've already got an ID for this resource we must have previously transformed it
        //so we don't need to forcibly transform it now
        Long existingEnterpriseId = findEnterpriseId(params, reference);
        if (existingEnterpriseId != null) {
            return existingEnterpriseId;
        }

        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        ResourceType resourceType = comps.getResourceType();
        UUID resourceId = UUID.fromString(comps.getId());

        //we've have multiple threads potentially trying to transform the same dependent resource (e.g. practitioner)
        //so we need to sync on something to ensure we only
        try {
            onDemandLock.lock();

            //see if this resource is mapped to another instance of the same concept (e.g. organisation),
            //in which case we want to use the enterprise ID for that OTHER instance
            if (resourceType == ResourceType.Organization
                    || resourceType == ResourceType.Practitioner) {

                UUID mappedResourceId = checkInstanceMapCache(params.getEnterpriseConfigName(), resourceType, resourceId);
                if (mappedResourceId == null) {

                    SubscriberInstanceMappingDalI enterpriseIdDal = DalProvider.factorySubscriberInstanceMappingDal(params.getEnterpriseConfigName());
                    mappedResourceId = enterpriseIdDal.findInstanceMappedId(resourceType, resourceId);

                    //if we've not got a mapping, then we need to create one from our resource data
                    if (mappedResourceId == null) {

                        ResourceWrapper wrapper = params.findOrRetrieveResource(reference);
                        if (wrapper == null) {
                            //if it's deleted then just return null since there's no point assigning an ID
                            return null;
                        }

                        Resource fhirResource = FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
                        String mappingValue = findInstanceMappingValue(fhirResource, params);
                        mappedResourceId = enterpriseIdDal.findOrCreateInstanceMappedId(resourceType, resourceId, mappingValue);
                    }

                    addToInstanceMapCache(params.getEnterpriseConfigName(), resourceType, resourceId, mappedResourceId);
                }

                //if our mapped ID is different to our proper ID, then we don't need to transform that
                //other resource, as it will already have been done, so we can just return it's Enterprise ID
                if (!mappedResourceId.equals(resourceId)) {
                    Long mappedInstanceEnterpriseId = findEnterpriseId(params, resourceType.toString(), mappedResourceId.toString());
                    if (mappedInstanceEnterpriseId == null) {
                        //if we've just started processing the first exchange for an org that's taking over the
                        //instance map, there's a chance we'll catch it mid-way through taking over, in which
                        //case we should just give a second and try again, throwing an error if we fail
                        Thread.sleep(1000);
                        mappedInstanceEnterpriseId = findEnterpriseId(params, resourceType.toString(), mappedResourceId.toString());
                        if (mappedInstanceEnterpriseId == null) {
                            throw new TransformException("Failed to find enterprise ID for mapped instance " + resourceType.toString() + " " + mappedResourceId.toString() + " and original ID " + resourceId);
                        }
                    }
                    return mappedInstanceEnterpriseId;
                }
            }

            //if we've already transformed the resource, which could happen because the transform is multi-threaded,
            //then have another look for the enterprise ID as it must exist
            existingEnterpriseId = findEnterpriseId(params, reference);
            if (existingEnterpriseId != null) {
                return existingEnterpriseId;
            }

            AbstractEnterpriseTransformer transformer = FhirToEnterpriseCsvTransformer.createTransformerForResourceType(resourceType);
            if (transformer == null) {
                throw new TransformException("No transformer found for resource " + reference.getReference());
            }

            //then generate the new ID
            Long enterpriseId = findOrCreateEnterpriseId(params, resourceType.toString(), comps.getId());

            //if we've got here, we actually want to transform the referred to resource, so retrieve from the DB
            ResourceWrapper wrapper = params.findOrRetrieveResource(reference);
            if (wrapper != null) {

                //add to the helper so we know we've transformed it, but also immediately mark it as having been transformed
                params.addResourceToTransform(wrapper);
                params.hasResourceBeenTransformedAddIfNot(wrapper);

                //and transform the resource
                AbstractEnterpriseCsvWriter csvWriter = FhirToEnterpriseCsvTransformer.findCsvWriterForResourceType(resourceType, params);
                transformer.transformResource(enterpriseId, wrapper, csvWriter, params);
            }

            return enterpriseId;

        } finally {
            onDemandLock.unlock();
        }
    }

    private static UUID checkInstanceMapCache(String enterpriseConfigName, ResourceType resourceType, UUID resourceId) {
        Object key = createCacheKey(enterpriseConfigName, resourceType.toString(), resourceId.toString());
        return (UUID)instanceCache.get(key);
    }

    private static void addToInstanceMapCache(String enterpriseConfigName, ResourceType resourceType, UUID resourceId, UUID mappedResourceId) throws Exception {
        Object key = createCacheKey(enterpriseConfigName, resourceType.toString(), resourceId.toString());
        instanceCache.put(key, mappedResourceId);
    }


    /**
     * although most enterprise transforms use the enterprise_id_map tables for their ID maps, when we have dependent
     * records from within FHIR resources to map (e.g. addresses within a FHIR Patient) then we use the new subscriber ID
     * map table since that supports that
     */
    public static Map<String, SubscriberId> findOrCreateSubscriberIds(EnterpriseTransformHelper params, SubscriberTableId subscriberTable, List<String> sourceIds, boolean createIfNotFound) throws Exception {
        return AbstractSubscriberTransformer.findOrCreateSubscriberIds(params.getEnterpriseConfigName(), subscriberTable, sourceIds, createIfNotFound);
    }



}
