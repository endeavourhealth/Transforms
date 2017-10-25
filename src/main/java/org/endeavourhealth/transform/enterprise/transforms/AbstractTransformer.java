package org.endeavourhealth.transform.enterprise.transforms;

import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriber.EnterpriseIdDalI;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.FhirToEnterpriseCsvTransformer;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.hl7.fhir.instance.model.TemporalPrecisionEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        Map<ResourceWrapper, Long> enterpriseIds = mapIds(params.getEnterpriseConfigName(), resources, shouldAlwaysTransform());

        for (ResourceWrapper resource: resources) {

            try {
                Long enterpriseId = enterpriseIds.get(resource);
                if (enterpriseId == null) {
                    continue;

                } else if (resource.isDeleted()) {
                    csvWriter.writeDelete(enterpriseId.longValue());

                } else {
                    Resource fhir = FhirResourceHelper.deserialiseResouce(resource);
                    transform(enterpriseId, fhir, csvWriter, params);
                }
            } catch (Exception ex) {
                throw new TransformException("Exception transforming " + resource.getResourceType() + " " + resource.getResourceId(), ex);
            }
        }
    }

    public abstract boolean shouldAlwaysTransform();

    public abstract void transform(Long enterpriseId,
                                   Resource resource,
                                   AbstractEnterpriseCsvWriter csvWriter,
                                   EnterpriseTransformParams params) throws Exception;

    protected static Integer convertDatePrecision(TemporalPrecisionEnum precision) throws Exception {
        return Integer.valueOf(precision.getCalendarConstant());
    }



    protected static Long findEnterpriseId(EnterpriseTransformParams params, Resource resource) throws Exception {
        String resourceType = resource.getResourceType().toString();
        String resourceId = resource.getId();
        return findEnterpriseId(params, resourceType, resourceId);
    }

    protected static Long findEnterpriseId(EnterpriseTransformParams params, Reference reference) throws Exception {
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        String resourceType = comps.getResourceType().toString();
        String resourceId = comps.getId();
        return findEnterpriseId(params, resourceType, resourceId);
    }

    protected static Long findEnterpriseId(EnterpriseTransformParams params, ResourceWrapper resource) throws Exception {
        return findEnterpriseId(params, resource.getResourceType(), resource.getResourceId().toString());
    }

    public static Long findEnterpriseId(EnterpriseTransformParams params, String resourceType, String resourceId) throws Exception {
        Long ret = checkCacheForId(params.getEnterpriseConfigName(), resourceType, resourceId);
        if (ret == null) {
            EnterpriseIdDalI enterpriseIdDal = DalProvider.factoryEnterpriseIdDal(params.getEnterpriseConfigName());
            ret = enterpriseIdDal.findEnterpriseId(resourceType, resourceId);
            //ret = idMappingRepository.getEnterpriseIdMappingId(enterpriseTableName, resourceType, resourceId);
        }
        return ret;
    }

    protected static Long findOrCreateEnterpriseId(EnterpriseTransformParams params, ResourceWrapper resource) throws Exception {
        String resourceType = resource.getResourceType();
        String resourceId = resource.getResourceId().toString();
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

    protected static Map<ResourceWrapper, Long> mapIds(String enterpriseConfigName, List<ResourceWrapper> resources, boolean createIfNotFound) throws Exception {

        Map<ResourceWrapper, Long> ids = new HashMap<>();

        //first, try to find existing IDs for our resources in our memory cache
        findEnterpriseIdsInCache(enterpriseConfigName, resources, ids);

        List<ResourceWrapper> resourcesToFindOnDb = new ArrayList<>();
        List<ResourceWrapper> resourcesToFindOrCreateOnDb = new ArrayList<>();

        for (ResourceWrapper resource: resources) {

            //if our memory cache contained this ID, then skip it
            if (ids.containsKey(resource)) {
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
            EnterpriseIdDalI enterpriseIdDal = DalProvider.factoryEnterpriseIdDal(enterpriseConfigName);
            enterpriseIdDal.findEnterpriseIds(resourcesToFindOnDb, ids);

            //add them to our cache
            for (ResourceWrapper resource: resourcesToFindOnDb) {
                Long enterpriseId = ids.get(resource);
                addIdToCache(enterpriseConfigName, resource.getResourceType(), resource.getResourceId().toString(), enterpriseId);
            }
        }

        //lookup and create any resources we need
        if (!resourcesToFindOrCreateOnDb.isEmpty()) {
            EnterpriseIdDalI enterpriseIdDal = DalProvider.factoryEnterpriseIdDal(enterpriseConfigName);
            enterpriseIdDal.findOrCreateEnterpriseIds(resourcesToFindOrCreateOnDb, ids);

            //add them to our cache
            for (ResourceWrapper resource: resourcesToFindOrCreateOnDb) {
                Long enterpriseId = ids.get(resource);
                addIdToCache(enterpriseConfigName, resource.getResourceType(), resource.getResourceId().toString(), enterpriseId);
            }
        }

        return ids;
    }

    private static void findEnterpriseIdsInCache(String enterpriseConfigName, List<ResourceWrapper> resources, Map<ResourceWrapper, Long> ids) throws Exception {

        for (ResourceWrapper resource: resources) {
            Long cachedId = checkCacheForId(enterpriseConfigName, resource.getResourceType(), resource.getResourceId().toString());
            if (cachedId != null) {
                ids.put(resource, cachedId);
            }
        }
    }




    protected Long transformOnDemand(Reference reference,
                                     EnterpriseTransformParams params) throws Exception {

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
