package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.ExchangeBatchExtraResourceDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberInstanceMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberResourceMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.FhirToSubscriberCsvTransformer;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSubscriberTransformer.class);
    private static final ParserPool PARSER_POOL = new ParserPool();

    //private static final EnterpriseIdMapRepository idMappingRepository = new EnterpriseIdMapRepository();
    private static JCS idCache = null;
    private static JCS instanceCache = null;
    private static final ReentrantLock onDemandLock = new ReentrantLock();

    private HashMap<String, Patient> patients = new HashMap<>();

    static {
        try {

            //by default the Java Caching System has a load of logging enabled, which is really slow, so turn it off
            //not longer required, since it no longer uses log4J and the new default doesn't have debug enabled
            /*org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.apache.jcs");
            logger.setLevel(org.apache.log4j.Level.OFF);*/

            idCache = JCS.getInstance("SubscriberResourceMap");
            instanceCache = JCS.getInstance("SubscriberInstanceMap");

        } catch (CacheException ex) {
            throw new RuntimeException("Error initialising cache", ex);
        }
    }

    public void transformResources(List<ResourceWrapper> resources, SubscriberTransformParams params) throws Exception {

        //find or create subscriber DB IDs for each of our resources
        Map<String, SubscriberId> idsForMainTable = mapIds(params.getEnterpriseConfigName(), getMainSubscriberTableId(), resources, shouldAlwaysTransform(), params);

        for (ResourceWrapper resource: resources) {

            try {
                ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());
                AbstractSubscriberTransformer transformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(resourceType);
                String resourceId = resource.getResourceId().toString();
                String sourceId = ReferenceHelper.createResourceReference(resourceType, resourceId);

                SubscriberId subscriberId = idsForMainTable.get(sourceId);
                if (subscriberId == null) {
                    //if we've got a null enterprise ID, then it means the ID mapper doesn't want us to do anything
                    //with the resource (e.g. ihe resource is a duplicate instance of another Organisation that is already transformed)
                    continue;
                }

                //check to see if we've already transformed this resource in this batch already,
                //which can happen for dependent items like orgs and practitioners
                if (params.hasResourceBeenTransformedAddIfNot(sourceId)) {
                    continue;
                }

                transformer.transformResource(subscriberId, resource, params);

            } catch (Exception ex) {
                throw new TransformException("Exception transforming " + resource.getResourceType() + " " + resource.getResourceId(), ex);
            }
        }
    }

    /*protected static void writeEventLog(SubscriberTransformParams params, ResourceWrapper resourceWrapper, SubscriberId subscriberId) throws Exception {

        long id = subscriberId.getSubscriberId();

        //mode is worked out from what we previously sent (unless it's a delete)
        byte event;
        if (resourceWrapper.isDeleted()) {
            event = EventLog.EVENT_LOG_DELETE;
        } else if (subscriberId.getDtUpdatedPreviouslySent() == null) {
            event = EventLog.EVENT_LOG_INSERT;
        } else {
            event = EventLog.EVENT_LOG_UPDATE;
        }

        byte tableId = subscriberId.getSubscriberTable();

        EventLog eventLog = params.getOutputContainer().getEventLog();
        eventLog.writeUpsert(id, new Date(), event, tableId);


        //if we've just deleted the resource, set the datetime to null so that if the resource is un-deleted,
        //we know that it must be next sent as an insert rather than an update
        if (resourceWrapper.isDeleted()) {
            subscriberId.setDtUpdatedPreviouslySent(null);

        } else {
            subscriberId.setDtUpdatedPreviouslySent(resourceWrapper.getCreatedAt());
        }

        //cache our subscriber ID so we can get our event log right next time
        params.addSubscriberId(subscriberId);
    }*/

    //defines whether the resources covered by this transformer should ALWAYS be transformed (e.g. patient data)
    //or only transformed if something refers to it (e.g. orgs and practitioners)
    public abstract boolean shouldAlwaysTransform();

    protected abstract void transformResource(SubscriberId subscriberId,
                                              ResourceWrapper resourceWrapper,
                                              SubscriberTransformParams params) throws Exception;

    protected abstract SubscriberTableId getMainSubscriberTableId();



    protected static Integer convertDatePrecision(SubscriberTransformParams params, Resource fhirResource, TemporalPrecisionEnum precision) throws Exception {
        String code = null;
        if (precision == TemporalPrecisionEnum.YEAR) {
            code = "year";

        } else if (precision == TemporalPrecisionEnum.MONTH) {
            code = "month";

        } else if (precision == TemporalPrecisionEnum.DAY) {
            code = "day";

        } else if (precision == TemporalPrecisionEnum.MINUTE) {
            code = "minute";

        } else if (precision == TemporalPrecisionEnum.SECOND) {
            code = "second";

        } else if (precision == TemporalPrecisionEnum.MILLI) {
            code = "millisecond";

        } else {
            throw new Exception("Unknown date time " + precision);
        }

        return IMHelper.getIMConcept(params, fhirResource, IMConstant.FHIR_DATETIME_PRECISION, code);
        //return Integer.valueOf(precision.getCalendarConstant());
    }



    /*protected static Long findEnterpriseId(SubscriberTransformParams params, Resource resource) throws Exception {
        String resourceType = resource.getResourceType().toString();
        String resourceId = resource.getId();
        return findEnterpriseId(params, resourceType, resourceId);
    }*/

    protected static Long findEnterpriseId(SubscriberTransformParams params, SubscriberTableId subscriberTable, Reference reference) throws Exception {
        SubscriberId ret = findSubscriberId(params, subscriberTable, reference.getReference());
        if (ret != null) {
            return ret.getSubscriberId();
        } else {
            return null;
        }
    }

    public static Long findEnterpriseId(SubscriberTransformParams params, SubscriberTableId subscriberTable, String sourceId) throws Exception {
        SubscriberId ret = findSubscriberId(params, subscriberTable, sourceId);
        if (ret != null) {
            return ret.getSubscriberId();
        } else {
            return null;
        }
    }

    protected static SubscriberId findSubscriberId(SubscriberTransformParams params, SubscriberTableId subscriberTable, Reference reference) throws Exception {
        return findSubscriberId(params, subscriberTable, reference.getReference());
    }

    public static SubscriberId findSubscriberId(SubscriberTransformParams params, SubscriberTableId subscriberTable, String sourceId) throws Exception {

        SubscriberId ret = checkCacheForId(params.getEnterpriseConfigName(), subscriberTable, sourceId);
        if (ret == null) {
            SubscriberResourceMappingDalI enterpriseIdDal = DalProvider.factorySubscriberResourceMappingDal(params.getEnterpriseConfigName());
            ret = enterpriseIdDal.findSubscriberId(subscriberTable.getId(), sourceId);
            if (ret != null) {
                addIdToCache(params.getEnterpriseConfigName(), subscriberTable, sourceId, ret);
            }
        }
        return ret;
    }


    /*protected static Long findOrCreateEnterpriseId(SubscriberTransformParams params, ResourceWrapper resource) throws Exception {
        String resourceType = resource.getResourceType();
        String resourceId = resource.getResourceId().toString();
        return findOrCreateEnterpriseId(params, resourceType, resourceId);
    }*/

    /*protected static SubscriberId findOrCreateEnterpriseId(SubscriberTransformParams params, Reference reference) throws Exception {
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        String resourceType = comps.getResourceType().toString();
        String resourceId = comps.getId();
        return findOrCreateEnterpriseId(params, resourceType, resourceId);
    }*/

    public static SubscriberId findOrCreateSubscriberId(SubscriberTransformParams params, SubscriberTableId subscriberTable, String sourceId) throws Exception {
        SubscriberId ret = checkCacheForId(params.getEnterpriseConfigName(), subscriberTable, sourceId);
        if (ret == null) {
            SubscriberResourceMappingDalI enterpriseIdDal = DalProvider.factorySubscriberResourceMappingDal(params.getEnterpriseConfigName());
            ret = enterpriseIdDal.findOrCreateSubscriberId(subscriberTable.getId(), sourceId);

            addIdToCache(params.getEnterpriseConfigName(), subscriberTable, sourceId, ret);
        }
        return ret;
    }

    private static String createSubscriberIdCacheKey(String enterpriseConfigName, SubscriberTableId subscriberTableId, String sourceId) {
        StringBuilder sb = new StringBuilder();
        sb.append(enterpriseConfigName);
        sb.append(":");
        sb.append(subscriberTableId.getId());
        sb.append(":");
        sb.append(sourceId);
        return sb.toString();
    }

    private static String createInstanceMapCacheKey(String enterpriseConfigName, ResourceType resourceType, UUID resourceId) {
        StringBuilder sb = new StringBuilder();
        sb.append(enterpriseConfigName);
        sb.append(":");
        sb.append(resourceType.toString());
        sb.append(":");
        sb.append(resourceId.toString());
        return sb.toString();
    }

    private static SubscriberId checkCacheForId(String enterpriseConfigName, SubscriberTableId subscriberTableId, String sourceId) throws Exception {
        return (SubscriberId)idCache.get(createSubscriberIdCacheKey(enterpriseConfigName, subscriberTableId, sourceId));
    }

    private static void addIdToCache(String enterpriseConfigName, SubscriberTableId subscriberTableId, String sourceId, SubscriberId toCache) throws Exception {
        if (toCache == null) {
            return;
        }
        idCache.put(createSubscriberIdCacheKey(enterpriseConfigName, subscriberTableId, sourceId), toCache);
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

    private static ResourceWrapper findResourceWrapper(Reference reference,
                                                        SubscriberTransformParams params) throws Exception {

        String referenceStr = reference.getReference();

        //look in our resources map first
        Map<String, ResourceWrapper> hmAllResources = params.getAllResources();
        ResourceWrapper ret = hmAllResources.get(referenceStr);
        if (ret == null) {
            //if not in our map, then hit the DB
            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            ret = resourceDal.getCurrentVersion(params.getServiceId(), comps.getResourceType().toString(), UUID.fromString(comps.getId()));
        }

        return ret;
    }

    public static Resource findResource(Reference reference,
                                           SubscriberTransformParams params) throws Exception {

        ResourceWrapper ret = findResourceWrapper(reference, params);
        //the above fn returns null if it's deleted
        //if (ret.isDeleted()) {
        if (ret == null) {
            return null;
        } else {
            return FhirResourceHelper.deserialiseResouce(ret);
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

    private static Map<String, SubscriberId> mapIds(String enterpriseConfigName, SubscriberTableId mainTable, List<ResourceWrapper> resources, boolean createIfNotFound, SubscriberTransformParams params) throws Exception {

        Map<String, SubscriberId> ret = new HashMap<>();

        //then work out what resources we need to hit the DB for
        List<String> resourcesToFindOnDb = new ArrayList<>();
        List<String> resourcesToFindOrCreateOnDb = new ArrayList<>();

        for (ResourceWrapper wrapper: resources) {

            String sourceId = ReferenceHelper.createResourceReference(wrapper.getResourceType(), wrapper.getResourceId().toString());

            //check our cache for an ID
            SubscriberId cachedId = checkCacheForId(enterpriseConfigName, mainTable, sourceId);
            if (cachedId != null) {
                ret.put(sourceId, cachedId);
                continue;
            }

            //if this resource is mapped to a different instance of the same concept (e.g. it's a duplicate instance
            //of an organisation), then we don't want to generate an ID for it
            if (isResourceMappedToAnotherInstance(wrapper, params)) {
                continue;
            }

            //if we didn't find an ID in memory, then we'll either want to simply find on the DB or find and create on the DB
            if (wrapper.isDeleted()
                    || !createIfNotFound) {

                resourcesToFindOnDb.add(sourceId);
            } else {

                resourcesToFindOrCreateOnDb.add(sourceId);
            }
        }

        SubscriberResourceMappingDalI enterpriseIdDal = DalProvider.factorySubscriberResourceMappingDal(enterpriseConfigName);

        //look up any resources we need
        if (!resourcesToFindOnDb.isEmpty()) {
            Map<String, SubscriberId> foundMap = enterpriseIdDal.findSubscriberIds(mainTable.getId(), resourcesToFindOnDb);
            for (String sourceId: resourcesToFindOnDb) {
                SubscriberId foundId = foundMap.get(sourceId);
                if (foundId != null) {
                    ret.put(sourceId, foundId);

                    //add them to our cache
                    addIdToCache(enterpriseConfigName, mainTable, sourceId, foundId);
                }
            }
        }

        //lookup and create any resources we need
        if (!resourcesToFindOrCreateOnDb.isEmpty()) {
            Map<String, SubscriberId> createdMap = enterpriseIdDal.findOrCreateSubscriberIds(mainTable.getId(), resourcesToFindOrCreateOnDb);
            for (String sourceId: resourcesToFindOrCreateOnDb) {
                SubscriberId foundId = createdMap.get(sourceId);
                if (foundId != null) {
                    ret.put(sourceId, foundId);

                    //add them to our cache
                    addIdToCache(enterpriseConfigName, mainTable, sourceId, foundId);
                }
            }
        }

        return ret;
    }

    private static boolean isResourceMappedToAnotherInstance(ResourceWrapper resource, SubscriberTransformParams params) throws Exception {

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



    private static String findInstanceMappingValue(ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        Resource resource = null;
        if (!resourceWrapper.isDeleted()) {
            String json = resourceWrapper.getResourceData();
            resource = FhirResourceHelper.deserialiseResouce(json);
        }

        return findInstanceMappingValue(resource, params);
    }

    public static String findInstanceMappingValue(Resource resource, SubscriberTransformParams params) throws Exception {

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


    /**
     * transforms a dependent resource not necessarily in the exchcnge batch we're currently transforming,
     * e.g. transform a practitioner that's referenced by an observation in this batch
     */
    protected Long transformOnDemandAndMapId(Reference reference,
                                             SubscriberTransformParams params) throws Exception {

        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        ResourceType resourceType = comps.getResourceType();
        UUID resourceId = UUID.fromString(comps.getId());
        String sourceId = reference.getReference();

        AbstractSubscriberTransformer transformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(resourceType);
        if (transformer == null) {
            //some resource types don't have a transformer, but we shouldn't be calling this for them
            throw new TransformException("No transformer found for resource " + reference.getReference());
        }

        //if we've already generated (now or in the past) an ID for this resource we must have previously transformed it
        //so we don't need to forcibly transform it now
        SubscriberTableId mainTable = transformer.getMainSubscriberTableId();
        SubscriberId existingEnterpriseId = findSubscriberId(params, mainTable, sourceId);
        if (existingEnterpriseId != null) {
            return existingEnterpriseId.getSubscriberId();
        }

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

                    SubscriberInstanceMappingDalI instanceMappingDal = DalProvider.factorySubscriberInstanceMappingDal(params.getEnterpriseConfigName());
                    mappedResourceId = instanceMappingDal.findInstanceMappedId(resourceType, resourceId);

                    //if we've not got a mapping, then we need to create one from our resource data
                    if (mappedResourceId == null) {

                        Resource fhirResource = findResource(reference, params);
                        if (fhirResource == null) {
                            //if it's deleted then just return null since there's no point assigning an ID
                            return null;
                        }

                        String mappingValue = findInstanceMappingValue(fhirResource, params);
                        mappedResourceId = instanceMappingDal.findOrCreateInstanceMappedId(resourceType, resourceId, mappingValue);
                    }

                    addToInstanceMapCache(params.getEnterpriseConfigName(), resourceType, resourceId, mappedResourceId);
                }

                //if our mapped ID is different to our proper ID, then we don't need to transform that
                //other resource, as it will already have been done, so we can just return it's Enterprise ID
                if (!mappedResourceId.equals(resourceId)) {

                    //source ID needs changing to reflected the new ID
                    String mappedSourceId = ReferenceHelper.createResourceReference(resourceType, mappedResourceId.toString());

                    SubscriberId mappedInstanceEnterpriseId = findSubscriberId(params, mainTable, mappedSourceId);
                    if (mappedInstanceEnterpriseId == null) {
                        //if we've just started processing the first exchange for an org that's taking over the
                        //instance map, there's a chance we'll catch it mid-way through taking over, in which
                        //case we should just give a second and try again, throwing an error if we fail
                        Thread.sleep(1000);
                        mappedInstanceEnterpriseId = findSubscriberId(params, mainTable, mappedSourceId);
                        if (mappedInstanceEnterpriseId == null) {
                            throw new TransformException("Failed to find enterprise ID for mapped instance " + resourceType.toString() + " " + mappedResourceId.toString() + " and original ID " + resourceId);
                        }
                    }
                    return mappedInstanceEnterpriseId.getSubscriberId();
                }
            }

            if (params.hasResourceBeenTransformedAddIfNot(sourceId)) {
                //if we've already transformed the resource, which could happen because the transform is multi-threaded,
                //then have another look for the enterprise ID as it must exist by now
                SubscriberId subscriberIdJustCreated = findSubscriberId(params, mainTable, sourceId);
                if (subscriberIdJustCreated == null) {
                    throw new Exception("Failed to find ID for resource just transformed " + reference);
                }
                return subscriberIdJustCreated.getSubscriberId();
            }

            //if we've got here, we actually want to transform the referred to resource, so retrieve from the DB
            ResourceWrapper wrapper = findResourceWrapper(reference, params);

            /*Resource fhir = findResource(reference, params);
            if (fhir == null) {
                //if it's deleted then just return null since there's no point assigning an ID
                return null;
            }*/


            //record in the DB that we've explicitly added this resource to this exchange batch,
            //so we have an audit of this, and can recover if we kill the queue reader at this point.
            ExchangeBatchExtraResourceDalI exchangeBatchExtraResourceDalI = DalProvider.factoryExchangeBatchExtraResourceDal(params.getEnterpriseConfigName());
            exchangeBatchExtraResourceDalI.saveExtraResource(params.getExchangeId(), params.getBatchId(), resourceType, resourceId);

            //then generate the new ID
            SubscriberId subscriberId = findOrCreateSubscriberId(params, mainTable, sourceId);

            //if the resource is deleted, the wrapper will be null (rather than an empty wrapper). Since we've never
            //sent anything over for this resource before, we don't need to call into the transform to send over a delete or anything.
            if (wrapper != null) {
                transformer.transformResource(subscriberId, wrapper, params);
            }

            return subscriberId.getSubscriberId();

        } finally {
            onDemandLock.unlock();
        }
    }

    private static UUID checkInstanceMapCache(String enterpriseConfigName, ResourceType resourceType, UUID resourceId) {
        Object key = createInstanceMapCacheKey(enterpriseConfigName, resourceType, resourceId);
        return (UUID)instanceCache.get(key);
    }

    private static void addToInstanceMapCache(String enterpriseConfigName, ResourceType resourceType, UUID resourceId, UUID mappedResourceId) throws Exception {
        Object key = createInstanceMapCacheKey(enterpriseConfigName, resourceType, resourceId);
        LOG.debug("Added to cache [key:" + key + " value:" + mappedResourceId.toString() + "]");
        instanceCache.put(key, mappedResourceId);
    }

    protected static Double getPatientAgeInDecimalYears(Patient patient) {
        if (patient.getBirthDate() != null) {
            LocalDate date = patient.getBirthDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            Period diff = Period.between(date, LocalDate.now());
            if (diff.getYears() == 0 && diff.getMonths() == 0) {
                diff.plusMonths(1);
            }
            Double inYears = diff.getYears() + ((double) diff.getMonths()) / 12;
            DecimalFormat df = new DecimalFormat("#.##");
            return Double.valueOf(df.format(inYears));
        }
        return null;
    }

    protected Patient getCachedPatient(Reference patient, SubscriberTransformParams params) throws Exception {

        Patient ret = patients.get(patient.getReference());
        if (ret != null) {
            return ret;
        }

        transformOnDemandAndMapId(patient, params);
        ret = (Patient) findResource(patient, params);
        patients.put(patient.getReference(), ret);

        return ret;
    }

    protected static String getScheme(String codingSystem) {
        String str = null;
        if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)) {
            str = IMConstant.SNOMED;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_READ2)) {
            str = IMConstant.READ2;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_CTV3)) {
            str = IMConstant.CTV3;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_ICD10)) {
            str = IMConstant.ICD10;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_OPCS4)) {
            str = IMConstant.OPCS4;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID)) {
            str = IMConstant.BARTS_CERNER;
        }
        if (str == null) {
            str = codingSystem;
        }
        return str;
    }

    /*protected Long transformOnDemandAndMapId(Reference reference,
                                             SubscriberTransformParams params) throws Exception {

        Long enterpriseId = null;

        if (!params.hasResourceBeenTransformedAddIfNot(reference)) {

            Resource fhir = findResource(reference, params);
            if (fhir == null) {
                //if the target resource doesn't exist, or has been deleted, just return null as we can't use it
                return null;
            }

            enterpriseId = findOrCreateEnterpriseId(params, reference);

            ResourceType resourceType = fhir.getResourceType();
            AbstractTransformer transformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(resourceType);
            if (transformer == null) {
                throw new TransformException("No transformer found for resource " + reference.getReference());
            }

            AbstractPcrCsvWriter csvWriter = FhirToSubscriberCsvTransformer.findCsvWriterForResourceType(resourceType, params);
            transformer.transform(enterpriseId, fhir, csvWriter, params);

        } else {
            enterpriseId = findEnterpriseId(params, reference);
        }

        return enterpriseId;
    }*/


}
