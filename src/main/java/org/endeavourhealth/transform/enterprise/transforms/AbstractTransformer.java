package org.endeavourhealth.transform.enterprise.transforms;

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
import org.endeavourhealth.core.database.dal.subscriberTransform.EnterpriseIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.EnterpriseInstanceMapDalI;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.FhirToEnterpriseCsvTransformer;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransformer.class);
    private static final ParserPool PARSER_POOL = new ParserPool();

    //private static final EnterpriseIdMapRepository idMappingRepository = new EnterpriseIdMapRepository();
    private static JCS cache = null;
    /*private static Map<String, AtomicInteger> maxIdMap = new ConcurrentHashMap<>();
    private static ReentrantLock futuresLock = new ReentrantLock();*/

    static {
        try {

            //by default the Java Caching System has a load of logging enabled, which is really slow, so turn it off
            //not longer required, since it no longer uses log4J and the new default doesn't have debug enabled
            /*org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.apache.jcs");
            logger.setLevel(org.apache.log4j.Level.OFF);*/

            cache = JCS.getInstance("EnterpriseResourceMap");

        } catch (CacheException ex) {
            throw new RuntimeException("Error initialising cache", ex);
        }
    }

    public void transform(List<ResourceWrapper> resources,
                          AbstractEnterpriseCsvWriter csvWriter,
                          EnterpriseTransformParams params) throws Exception {

        Map<ResourceWrapper, Long> enterpriseIds = mapIds(params.getEnterpriseConfigName(), resources, shouldAlwaysTransform(), params);

        for (ResourceWrapper resource: resources) {

            try {
                Long enterpriseId = enterpriseIds.get(resource);
                if (enterpriseId == null) {
                    continue;

                } else if (resource.isDeleted()) {
                    csvWriter.writeDelete(enterpriseId.longValue());

                } else {

                    //check to see if we've already transformed this resource in this batch already,
                    //which can happen for dependent items like orgs and practitioners
                    ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());
                    String resourceId = resource.getResourceId().toString();
                    Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceId);
                    if (!params.hasResourceBeenTransformedAddIfNot(resourceReference)) {

                        Resource fhir = FhirResourceHelper.deserialiseResouce(resource);
                        transform(enterpriseId, fhir, csvWriter, params);
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

    protected abstract void transform(Long enterpriseId,
                                   Resource resource,
                                   AbstractEnterpriseCsvWriter csvWriter,
                                   EnterpriseTransformParams params) throws Exception;

    protected static Integer convertDatePrecision(TemporalPrecisionEnum precision) throws Exception {
        return Integer.valueOf(precision.getCalendarConstant());
    }



    /*protected static Long findEnterpriseId(EnterpriseTransformParams params, Resource resource) throws Exception {
        String resourceType = resource.getResourceType().toString();
        String resourceId = resource.getId();
        return findEnterpriseId(params, resourceType, resourceId);
    }*/

    protected static Long findEnterpriseId(EnterpriseTransformParams params, Reference reference) throws Exception {
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        String resourceType = comps.getResourceType().toString();
        String resourceId = comps.getId();
        return findEnterpriseId(params, resourceType, resourceId);
    }

    /*protected static Long findEnterpriseId(EnterpriseTransformParams params, ResourceWrapper resource) throws Exception {
        return findEnterpriseId(params, resource.getResourceType(), resource.getResourceId().toString());
    }*/

    public static Long findEnterpriseId(EnterpriseTransformParams params, String resourceType, String resourceId) throws Exception {
        Long ret = checkCacheForId(params.getEnterpriseConfigName(), resourceType, resourceId);
        if (ret == null) {
            EnterpriseIdDalI enterpriseIdDal = DalProvider.factoryEnterpriseIdDal(params.getEnterpriseConfigName());
            ret = enterpriseIdDal.findEnterpriseId(resourceType, resourceId);
            //ret = idMappingRepository.getEnterpriseIdMappingId(enterpriseTableName, resourceType, resourceId);
        }
        return ret;
    }

    /*protected static Long findOrCreateEnterpriseId(EnterpriseTransformParams params, ResourceWrapper resource) throws Exception {
        String resourceType = resource.getResourceType();
        String resourceId = resource.getResourceId().toString();
        return findOrCreateEnterpriseId(params, resourceType, resourceId);
    }*/

    protected static Long findOrCreateEnterpriseId(EnterpriseTransformParams params, Reference reference) throws Exception {
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        String resourceType = comps.getResourceType().toString();
        String resourceId = comps.getId();
        return findOrCreateEnterpriseId(params, resourceType, resourceId);
    }

    public static Long findOrCreateEnterpriseId(EnterpriseTransformParams params, String resourceType, String resourceId) throws Exception {
        Long ret = checkCacheForId(params.getEnterpriseConfigName(), resourceType, resourceId);
        if (ret == null) {
            EnterpriseIdDalI enterpriseIdDal = DalProvider.factoryEnterpriseIdDal(params.getEnterpriseConfigName());
            ret = enterpriseIdDal.findOrCreateEnterpriseId(resourceType, resourceId);

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
        return (Long)cache.get(createCacheKey(enterpriseConfigName, resourceType, resourceId));
    }

    private static void addIdToCache(String enterpriseConfigName, String resourceType, String resourceId, Long toCache) throws Exception {
        if (toCache == null) {
            return;
        }
        cache.put(createCacheKey(enterpriseConfigName, resourceType, resourceId), toCache);
    }

    /*public static Resource deserialiseResouce(ResourceByExchangeBatch resourceByExchangeBatch) throws Exception {
        String json = resourceByExchangeBatch.getResourceData();
        return deserialiseResouce(json);
    }

    public static Resource deserialiseResouce(String json) throws Exception {
        try {
            return PARSER_POOL.parse(json);

        } catch (Exception ex) {
            LOG.error("Error deserialising resource", ex);
            LOG.error(json);
            throw ex;
        }

    }*/

    protected static Resource findResource(Reference reference,
                                           EnterpriseTransformParams params) throws Exception {

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
            return resourceDal.getCurrentVersionAsResource(comps.getResourceType(), comps.getId());
        }
    }

    /*protected static Long mapId(String enterpriseConfigName, ResourceByExchangeBatch resource, boolean createIfNotFound) throws Exception {

        if (resource.getIsDeleted()) {
            //if it's a delete, then don't bother creating a new Enterprise ID if we've never previously sent it
            //to Enterprise, since there's no point just sending a delete
            return findEnterpriseId(enterpriseConfigName, resource);

        } else {

            if (createIfNotFound) {
                return findOrCreateEnterpriseId(enterpriseConfigName, resource);

            } else {
                return findEnterpriseId(enterpriseConfigName, resource);
            }
        }
    }*/

    private static Map<ResourceWrapper, Long> mapIds(String enterpriseConfigName, List<ResourceWrapper> resources, boolean createIfNotFound, EnterpriseTransformParams params) throws Exception {

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
            //TODO - finish
            /*if (isResourceMappedToAnotherInstance(enterpriseConfigName, resource, params)) {
                continue;
            }*/

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
            EnterpriseIdDalI enterpriseIdDal = DalProvider.factoryEnterpriseIdDal(enterpriseConfigName);
            enterpriseIdDal.findEnterpriseIds(resourcesToFindOnDb, ret);

            //add them to our cache
            for (ResourceWrapper resource: resourcesToFindOnDb) {
                Long enterpriseId = ret.get(resource);
                addIdToCache(enterpriseConfigName, resource.getResourceType(), resource.getResourceId().toString(), enterpriseId);
            }
        }

        //lookup and create any resources we need
        if (!resourcesToFindOrCreateOnDb.isEmpty()) {
            EnterpriseIdDalI enterpriseIdDal = DalProvider.factoryEnterpriseIdDal(enterpriseConfigName);
            enterpriseIdDal.findOrCreateEnterpriseIds(resourcesToFindOrCreateOnDb, ret);

            //add them to our cache
            for (ResourceWrapper resource: resourcesToFindOrCreateOnDb) {
                Long enterpriseId = ret.get(resource);
                addIdToCache(enterpriseConfigName, resource.getResourceType(), resource.getResourceId().toString(), enterpriseId);
            }
        }

        return ret;
    }

    private static boolean isResourceMappedToAnotherInstance(String enterpriseConfigName, ResourceWrapper resource, EnterpriseTransformParams params) throws Exception {

        String resourceType = resource.getResourceType();
        UUID resourceId = resource.getResourceId();

        //only orgs and practitioners are mapped to other instances
        if (!resourceType.equals(ResourceType.Organization.toString())
                && !resourceType.equals(ResourceType.Practitioner.toString())) {
            return false;
        }

        EnterpriseInstanceMapDalI instanceMapper = DalProvider.factoryEnterpriseInstanceDal(enterpriseConfigName);
        UUID mappedResourceId = instanceMapper.findInstanceMappedId(resourceType, resourceId);

        //if we've not got a mapping, then we need to create one from our resource data
        if (mappedResourceId == null) {
            String mappingValue = findInstanceMappingValue(resource, params);
            mappedResourceId = instanceMapper.findOrCreateInstanceMappedId(resourceType, resourceId, mappingValue);
        }

        //if the mapped ID is different to the resource ID then it's mapped to another instance
        return !mappedResourceId.equals(resourceId);
    }

    private static String findInstanceMappingValue(ResourceWrapper resourceWrapper, EnterpriseTransformParams params) throws Exception {

        //if our resource is deleted return null so we just map the resource to itself
        if (resourceWrapper.isDeleted()) {
            return null;
        }

        String json = resourceWrapper.getResourceData();
        Resource resource = FhirResourceHelper.deserialiseResouce(json);

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
                        throw new Exception("Don't know how to handle practitioners with more than one role");
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
    //TODO - finish this
    /*protected Long transformOnDemandAndMapId(Reference reference,
                                             EnterpriseTransformParams params) throws Exception {

        Long enterpriseId = null;

        if (!params.hasResourceBeenTransformedAddIfNot(reference)) {

            Resource fhir = findResource(reference, params);
            if (fhir == null) {
                //if the target resource doesn't exist, or has been deleted, just return null as we can't use it
                return null;
            }

            enterpriseId = findOrCreateEnterpriseId(params, reference);

            ResourceType resourceType = fhir.getResourceType();
            AbstractTransformer transformer = FhirToEnterpriseCsvTransformer.createTransformerForResourceType(resourceType);
            if (transformer == null) {
                throw new TransformException("No transformer found for resource " + reference.getReference());
            }

            AbstractEnterpriseCsvWriter csvWriter = FhirToEnterpriseCsvTransformer.findCsvWriterForResourceType(resourceType, params);
            transformer.transform(enterpriseId, fhir, csvWriter, params);

        } else {
            enterpriseId = findEnterpriseId(params, reference);
        }

        return enterpriseId;
    }*/
    protected Long transformOnDemandAndMapId(Reference reference,
                                            EnterpriseTransformParams params) throws Exception {

        Long existingEnterpriseId = findEnterpriseId(params, reference);
        if (existingEnterpriseId != null) {
            //if we've already got an ID for this resource we must have previously transformed it
            //so we don't need to forcibly transform it now
            return existingEnterpriseId;
        }

        Resource fhir = findResource(reference, params);
        if (fhir == null) {
            return null;
        }

        ResourceType resourceType = fhir.getResourceType();
        AbstractTransformer transformer = FhirToEnterpriseCsvTransformer.createTransformerForResourceType(resourceType);
        if (transformer == null) {
            throw new TransformException("No transformer found for resource " + reference.getReference());
        }

        AbstractEnterpriseCsvWriter csvWriter = FhirToEnterpriseCsvTransformer.findCsvWriterForResourceType(resourceType, params);
        Long enterpriseId = findOrCreateEnterpriseId(params, resourceType.toString(), fhir.getId());
        transformer.transform(enterpriseId, fhir, csvWriter, params);

        return enterpriseId;
    }
}
