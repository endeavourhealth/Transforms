package org.endeavourhealth.transform.subscriber;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.*;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirToXTransformerBase;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.endeavourhealth.transform.subscriber.targetTables.OutputContainer;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.endeavourhealth.transform.subscriber.transforms.*;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public class FhirToSubscriberCsvTransformer extends FhirToXTransformerBase {

    private static final Logger LOG = LoggerFactory.getLogger(FhirToSubscriberCsvTransformer.class);

    private static final int DEFAULT_TRANSFORM_BATCH_SIZE = 50;
    //private static Map<String, Integer> transformBatchSizeCache = new HashMap<>();

    private static final PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();

    public static String transformFromFhir(UUID serviceId,
                                           UUID systemId,
                                           UUID exchangeId,
                                           UUID batchId,
                                           List<ResourceWrapper> resources,
                                           String configName,
                                           UUID protocolId,
                                           String exchangeBody) throws Exception {

        LOG.trace("Transforming batch " + batchId + " and " + resources.size() + " resources for service " + serviceId + " -> " + configName);

        JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");

        boolean pseudonymised = config.has("pseudonymised")
                && config.get("pseudonymised").asBoolean();
        //boolean pseudonymised = config.get("pseudonymised").asBoolean();

        int batchSize = DEFAULT_TRANSFORM_BATCH_SIZE;
        if (config.has("transform_batch_size")) {
            batchSize = config.get("transform_batch_size").asInt();
        }

        //hash the resources by reference to them, so the transforms can quickly look up dependant resources
        Map<String, ResourceWrapper> resourcesMap = hashResourcesByReference(resources);

        SubscriberTransformParams params = new SubscriberTransformParams(serviceId, systemId, protocolId, exchangeId, batchId, configName, resourcesMap, exchangeBody, pseudonymised);

        Long enterpriseOrgId = findEnterpriseOrgId(serviceId, params, resources);
        params.setEnterpriseOrganisationId(enterpriseOrgId);
        params.setBatchSize(batchSize);

        //sometimes we may fail to find an org id, so just return null as there's nothing to send
        if (enterpriseOrgId == null) {
            return null;
        }

        try {
            transformResources(resources, params);

            OutputContainer data = params.getOutputContainer();
            byte[] bytes = data.writeToZip();
            String ret = Base64.getEncoder().encodeToString(bytes);

            //update the state table so we know the datetime of each resource we just transformed, so the event log is maintained properly
            //TODO - if the queue reader is killed or fails after this point, we won't accurately know what we previously sent, since we'll think we sent something when we didn't
            updateSubscriberStateTable(params);

            return ret;

        } catch (Exception ex) {
            throw new TransformException("Exception transforming batch " + batchId, ex);
        }
    }

    private static void updateSubscriberStateTable(SubscriberTransformParams params) throws Exception {

        SubscriberResourceMappingDalI resourceMappingDal = DalProvider.factorySubscriberResourceMappingDal(params.getEnterpriseConfigName());

        List<SubscriberId> map = params.getSubscriberIds();

        List<SubscriberId> batch = new ArrayList<>();
        for (SubscriberId id: map) {

            batch.add(id);

            if (batch.size() > params.getBatchSize()) {
                resourceMappingDal.updateDtUpdatedForSubscriber(batch);
                batch = new ArrayList<>();
            }
        }

        //and do any remaining ones
        if (!batch.isEmpty()) {
            resourceMappingDal.updateDtUpdatedForSubscriber(batch);
        }
    }

    /*private static int findTransformBatchSize(String configName) throws Exception {
        Integer i = transformBatchSizeCache.get(configName);
        if (i == null) {
            JsonNode json = ConfigManager.getConfigurationAsJson(configName, "subscriber");
            JsonNode batchSize = json.get("TransformBatchSize");
            if (batchSize == null) {
                i = new Integer(DEFAULT_TRANSFORM_BATCH_SIZE);
            } else {
                i = new Integer(batchSize.asInt());
            }
            transformBatchSizeCache.put(configName, i);
        }
        return i.intValue();
    }*/

    /**
     * finds or creates the ID for the organization that all this data is for
     */
    private static Long findEnterpriseOrgId(UUID serviceId, SubscriberTransformParams params, List<ResourceWrapper> resources) throws Exception {

        //if we've previously transformed for our ODS code, then we'll have a mapping to the enterprise ID for that ODS code
        SubscriberOrgMappingDalI subscriberInstanceMappingDal = DalProvider.factorySubscriberOrgMappingDal(params.getEnterpriseConfigName());
        Long enterpriseOrganisationId = subscriberInstanceMappingDal.findEnterpriseOrganisationId(serviceId.toString());
        if (enterpriseOrganisationId != null) {
            return enterpriseOrganisationId;
        }

        //if this is our first time transforming for our org, then we need to find the FHIR resource
        //that represents our organisation. Unfortunately, the very first batch for an org will
        //not contain enough info to work out which resource is our interesting one, so we need to
        //rely on there being a patient resource that tells us.
        ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
        ResourceWrapper resourceByService = resourceRepository.getFirstResourceByService(serviceId, ResourceType.Patient);
        if (resourceByService == null) {
            //Emis sometimes activate practices before they send up patient data, so we may have a service with all the
            //non-patient metadata, but no patient data. If this happens, then don't send anything to Enterprise, as
            //it'll all be sorted out when they do send patient data.
            return null;
            //throw new TransformException("Cannot find a Patient resource for service " + serviceId + " and system " + systemId);
        }

        String json = resourceByService.getResourceData();
        //LOG.info("First resource for service " + serviceId + " is " + resourceByService.getResourceType() + " " + resourceByService.getResourceId());

        //if the first patient has been deleted, then we need to look at its history to find the JSON from when it wasn't deleted
        if (Strings.isNullOrEmpty(json)) {
            List<ResourceWrapper> history = resourceRepository.getResourceHistory(serviceId, resourceByService.getResourceType(), resourceByService.getResourceId());
            for (ResourceWrapper historyItem: history) {
                json = historyItem.getResourceData();
                if (!Strings.isNullOrEmpty(json)) {
                    break;
                }
            }
        }

        Patient patient = (Patient)FhirResourceHelper.deserialiseResouce(json);
        if (!patient.hasManagingOrganization()) {
            throw new TransformException("Patient " + patient.getId() + " doesn't have a managing org for service " + serviceId);
        }

        Reference orgReference = patient.getManagingOrganization();
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(orgReference);
        ResourceType resourceType = comps.getResourceType();
        UUID resourceId = UUID.fromString(comps.getId());
        //LOG.info("Managing organisation is " + resourceType + " " + resourceId);

        //we need to see if our organisation is mapped to another instance of the same place,
        //in which case we need to use the enterprise ID of that other instance
        SubscriberInstanceMappingDalI instanceMapper = DalProvider.factorySubscriberInstanceMappingDal(params.getEnterpriseConfigName());
        UUID mappedResourceId = instanceMapper.findInstanceMappedId(resourceType, resourceId);

        //if we've not got a mapping, then we need to create one from our resource data
        if (mappedResourceId == null) {
            Resource fhir = resourceRepository.getCurrentVersionAsResource(serviceId, resourceType, resourceId.toString());
            String mappingValue = AbstractSubscriberTransformer.findInstanceMappingValue(fhir, params);
            mappedResourceId = instanceMapper.findOrCreateInstanceMappedId(resourceType, resourceId, mappingValue);
        }

        //if our mapped resource ID is different to our proper ID, then there's a different instance of our organisation
        //already on the database. So we want to "take over" that organisation record, with our own instance
        if (!mappedResourceId.equals(resourceId)) {
            instanceMapper.takeOverInstanceMapping(resourceType, mappedResourceId, resourceId);
        }

        //generate (or find) an enterprise ID for our organization
        String sourceId = orgReference.getReference();
        SubscriberId subscriberId = AbstractSubscriberTransformer.findOrCreateSubscriberId(params, SubscriberTableId.ORGANIZATION, sourceId);
        enterpriseOrganisationId = subscriberId.getSubscriberId();
        //LOG.info("Created enterprise org ID " + enterpriseOrganisationId);

        //and store the organization's enterprise ID in a separate table so we don't have to repeat all this next time
        subscriberInstanceMappingDal.saveEnterpriseOrganisationId(serviceId.toString(), enterpriseOrganisationId);

        //we also want to ensure that our organisation is transformed right now, so need to make sure it's in our list of resources
        String orgReferenceValue = ReferenceHelper.createResourceReference(resourceType, resourceId.toString());
        Map<String, ResourceWrapper> map = params.getAllResources();
        if (!map.containsKey(orgReferenceValue)) {
            /*LOG.info("=====Reference map doesn't contain " + orgReferenceValue);
            for (String key: map.keySet()) {
                LOG.info("Key = " + key);
            }
            LOG.info("<<<<<Reference map doesn't contain " + orgReferenceValue);*/

            //record the audit of us adding a new resource to the batch
            ExchangeBatchExtraResourceDalI exchangeBatchExtraResourceDalI = DalProvider.factoryExchangeBatchExtraResourceDal(params.getEnterpriseConfigName());
            exchangeBatchExtraResourceDalI.saveExtraResource(params.getExchangeId(), params.getBatchId(), resourceType, resourceId);

            ResourceWrapper resourceWrapper = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), resourceId);

            if (resourceWrapper == null) {
                throw new TransformException("Failed to find non-null version of " + resourceType + " " + resourceId);
            }

            //and actually add to the two collections of resources
            resources.add(resourceWrapper);
            map.put(orgReferenceValue, resourceWrapper);
        }

        return enterpriseOrganisationId;
    }

    /**
     * all resources in a batch are for the same patient (or no patient at all), so rather than looking
     * up the Enterprise patient ID for each resource, we can do it once at the start. To do that
     * we need the Discovery patient ID from one of the resources.
     */
    private static String findPatientId(List<ResourceWrapper> resourceWrappers) throws Exception {

        for (ResourceWrapper resourceWrapper: resourceWrappers) {
            if (resourceWrapper.isDeleted()) {
                continue;
            }

            String resourceTypeStr = resourceWrapper.getResourceType();
            ResourceType resourceType = ResourceType.valueOf(resourceTypeStr);
            if (!FhirResourceFiler.isPatientResource(resourceType)) {
                continue;
            }

            try {
                Resource resource = FhirResourceHelper.deserialiseResouce(resourceWrapper);
                String patientId = IdHelper.getPatientId(resource);
                if (Strings.isNullOrEmpty(patientId)) {
                    continue;
                }

                return patientId;

            } catch (PatientResourceException ex) {
                //we've had this exception because a batch has ended up containing JUST
                //a Slot resource, which means we can't get the patient ID. The matching Appointment
                //resource was created in a separate exchange_batch, but errors meant this data was
                //split into a separate batch. This being the case, the Slot will already have been sent
                //to the subscriber, because that's manually done when the appointment is done. So we
                //can safely ignore this
                if (resourceType != ResourceType.Slot) {
                    throw ex;
                }
            }
        }

        return null;
    }

    private static Patient findPatient(List<ResourceWrapper> resourceWrappers) throws Exception {

        for (ResourceWrapper resourceWrapper: resourceWrappers) {
            if (resourceWrapper.isDeleted()) {
                continue;
            }

            String resourceTypeStr = resourceWrapper.getResourceType();
            ResourceType resourceType = ResourceType.valueOf(resourceTypeStr);
            if (!FhirResourceFiler.isPatientResource(resourceType)) {
                continue;
            }

            try {
                Resource resource = FhirResourceHelper.deserialiseResouce(resourceWrapper);
                String patientId = IdHelper.getPatientId(resource);
                if (Strings.isNullOrEmpty(patientId)) {
                    continue;
                }

                return (Patient) resource;

            } catch (PatientResourceException ex) {
                //we've had this exception because a batch has ended up containing JUST
                //a Slot resource, which means we can't get the patient ID. The matching Appointment
                //resource was created in a separate exchange_batch, but errors meant this data was
                //split into a separate batch. This being the case, the Slot will already have been sent
                //to the subscriber, because that's manually done when the appointment is done. So we
                //can safely ignore this
                if (resourceType != ResourceType.Slot) {
                    throw ex;
                }
            }
        }

        return null;
    }


    private static void transformResources(List<ResourceWrapper> resources,
                                          SubscriberTransformParams params) throws Exception {

        int threads = Math.min(10, resources.size()/10); //limit to 10 threads, but don't create too many unnecessarily if we only have a few resources
        threads = Math.max(threads, 1); //make sure we have a min of 1

        ThreadPool threadPool = new ThreadPool(threads, 1000);

        //we detect whether we're doing an update or insert, based on whether we're previously mapped
        //a reference to a resource, so we need to transform the resources in a specific order, so
        //that we transform resources before we ones that refer to them
        transformResources(ResourceType.Organization, resources, threadPool, params);
        transformResources(ResourceType.Location, resources, threadPool, params);
        transformResources(ResourceType.Practitioner, resources, threadPool, params);
        transformResources(ResourceType.Schedule, resources, threadPool, params);

        //do the patient resource
        transformResources(ResourceType.Patient, resources, threadPool, params);

        //if we transformed a patient resource, we need to guarantee that the patient is fully transformed before continuing
        //so we need to let the threadpool empty before doing anything more
        List<ThreadPoolError> errors = threadPool.waitUntilEmpty();
        handleErrors(errors);

        //having done any patient resource in our batch, we should have created an enterprise patient ID and person ID that we can use for all remaining resources
        String discoveryPatientId = findPatientId(resources);
        if (!Strings.isNullOrEmpty(discoveryPatientId)) {
            String sourceId = ReferenceHelper.createResourceReference(ResourceType.Patient.toString(), discoveryPatientId);
            Long enterprisePatientId = AbstractSubscriberTransformer.findEnterpriseId(params, SubscriberTableId.PATIENT, sourceId);
            if (enterprisePatientId == null) {
                //with the Homerton data, we just get data from a point in time, not historic data too, so we have some episodes of
                //care where we don't have patients. If we're in this situation, then don't send over the data.
                LOG.warn("No enterprise patient ID for patient " + discoveryPatientId + " so not doing patient resources");
                return;
                //throw new TransformException("No enterprise patient ID found for discovery patient " + discoveryPatientId);
            }
            if (params.getEnterprisePatientId() == null) {
                params.setEnterprisePatientId(enterprisePatientId);
            }

            String discoveryPersonId = patientLinkDal.getPersonId(discoveryPatientId);

            //if we've got some cases where we've got a deleted patient but non-deleted patient-related resources
            //all in the same batch, because Emis sent it like that. In that case we won't have a person ID, so
            //return out without processing any of the remaining resources, since they're for a deleted patient.
            if (Strings.isNullOrEmpty(discoveryPersonId)) {
                return;
            }

            SubscriberPersonMappingDalI subscriberPersonMappingDal = DalProvider.factorySubscriberPersonMappingDal(params.getEnterpriseConfigName());
            Long enterprisePersonId = subscriberPersonMappingDal.findOrCreateEnterprisePersonId(discoveryPersonId);
            if (params.getEnterprisePersonId() == null) {
                params.setEnterprisePersonId(enterprisePersonId);
            }
        }

        Patient patient = findPatient(resources);
        if (patient != null && StringUtils.isNotEmpty(IdentifierHelper.findNhsNumber(patient))) {
            transformResources(ResourceType.EpisodeOfCare, resources, threadPool, params);
            transformResources(ResourceType.Appointment, resources, threadPool, params);
            transformResources(ResourceType.Encounter, resources, threadPool, params);
            transformResources(ResourceType.Condition, resources, threadPool, params);
            transformResources(ResourceType.Procedure, resources, threadPool, params);
            transformResources(ResourceType.ReferralRequest, resources, threadPool, params);
            transformResources(ResourceType.ProcedureRequest, resources, threadPool, params);
            transformResources(ResourceType.Observation, resources, threadPool, params);
            transformResources(ResourceType.MedicationStatement, resources, threadPool, params);
            transformResources(ResourceType.MedicationOrder, resources, threadPool, params);
            transformResources(ResourceType.Immunization, resources, threadPool, params);
            transformResources(ResourceType.FamilyMemberHistory, resources, threadPool, params);
            transformResources(ResourceType.AllergyIntolerance, resources, threadPool, params);
            transformResources(ResourceType.DiagnosticOrder, resources, threadPool, params);
            transformResources(ResourceType.DiagnosticReport, resources, threadPool, params);
            transformResources(ResourceType.Specimen, resources, threadPool, params);
            transformResources(ResourceType.Flag, resources, threadPool, params);
        }

        //for these resource types, call with a null transformer as they're actually transformed when
        //doing one of the above entities, but we want to remove them from the resources list
        transformResources(ResourceType.Slot, resources, threadPool, params);

        //close the thread pool
        errors = threadPool.waitAndStop();
        handleErrors(errors);

        //if there's anything left in the list, then we've missed a resource type
        if (!resources.isEmpty()) {
            Set<String> resourceTypesMissed = new HashSet<>();
            for (ResourceWrapper resource: resources) {
                String resourceType = resource.getResourceType();
                resourceTypesMissed.add(resourceType);
            }
            String s = String.join(", ", resourceTypesMissed);
            throw new TransformException("Transform to Enterprise doesn't handle " + s + " resource type(s)");
        }
    }

    /*public static AbstractSubscriberCsvWriter findCsvWriterForResourceType(ResourceType resourceType, SubscriberTransformParams params) throws Exception {

        OutputContainer data = params.getOutputContainer();

        if (resourceType == ResourceType.Organization) {
            return data.getOrganisations();
        } else if (resourceType == ResourceType.Location) {
            return data.getLocations();
        } else if (resourceType == ResourceType.Practitioner) {
            return data.getPractitioners();
        } else if (resourceType == ResourceType.Schedule) {
            return data.getSchedules();
        } else if (resourceType == ResourceType.Patient) {
            return data.getPatients();
        } else if (resourceType == ResourceType.EpisodeOfCare) {
            return data.getEpisodesOfCare();
        } else if (resourceType == ResourceType.Appointment) {
            return data.getAppointments();
        } else if (resourceType == ResourceType.Encounter) {
            return data.getEncounters();
        } else if (resourceType == ResourceType.Flag) {
            return data.getFlags();
        } else if (resourceType == ResourceType.Condition) {
            return data.getObservations();
        } else if (resourceType == ResourceType.Procedure) {
            return data.getObservations();
        } else if (resourceType == ResourceType.ReferralRequest) {
            return data.getReferralRequests();
        } else if (resourceType == ResourceType.ProcedureRequest) {
            return data.getProcedureRequests();
        } else if (resourceType == ResourceType.Observation) {
            return data.getObservations();
        } else if (resourceType == ResourceType.MedicationStatement) {
            return data.getMedicationStatements();
        } else if (resourceType == ResourceType.MedicationOrder) {
            return data.getMedicationOrders();
        } else if (resourceType == ResourceType.Immunization) {
            return data.getObservations();
        } else if (resourceType == ResourceType.FamilyMemberHistory) {
            return data.getObservations();
        } else if (resourceType == ResourceType.AllergyIntolerance) {
            return data.getAllergyIntolerances();
        } else if (resourceType == ResourceType.DiagnosticOrder) {
            return data.getObservations();
        } else if (resourceType == ResourceType.DiagnosticReport) {
            return data.getObservations();
        } else if (resourceType == ResourceType.Specimen) {
            return data.getObservations();
        } else if (resourceType == ResourceType.Slot) {
            //slots are handled in the appointment transformer, so have no dedicated one
            return null;
        } else {
            throw new TransformException("Unhandled resource type " + resourceType);
        }
    }*/

    public static AbstractSubscriberTransformer createTransformerForResourceType(ResourceType resourceType) throws Exception {
        if (resourceType == ResourceType.Organization) {
            return new OrganisationTransformer();
        } else if (resourceType == ResourceType.Location) {
            return new LocationTransformer();
        } else if (resourceType == ResourceType.Practitioner) {
            return new PractitionerTransformer();
        } else if (resourceType == ResourceType.Schedule) {
            return new ScheduleTransformer();
        } else if (resourceType == ResourceType.Patient) {
            return new PatientTransformer();
        } else if (resourceType == ResourceType.EpisodeOfCare) {
            return new EpisodeOfCareTransformer();
        } else if (resourceType == ResourceType.Appointment) {
            return new AppointmentTransformer();
        } else if (resourceType == ResourceType.Encounter) {
            return new EncounterTransformer();
        } else if (resourceType == ResourceType.Flag) {
            return new FlagTransformer();
        } else if (resourceType == ResourceType.Condition) {
            return new ConditionTransformer();
        } else if (resourceType == ResourceType.Procedure) {
            return new ProcedureTransformer();
        } else if (resourceType == ResourceType.ReferralRequest) {
            return new ReferralRequestTransformer();
        } else if (resourceType == ResourceType.ProcedureRequest) {
            return new ProcedureRequestTransformer();
        } else if (resourceType == ResourceType.Observation) {
            return new ObservationTransformer();
        } else if (resourceType == ResourceType.MedicationStatement) {
            return new MedicationStatementTransformer();
        } else if (resourceType == ResourceType.MedicationOrder) {
            return new MedicationOrderTransformer();
        } else if (resourceType == ResourceType.Immunization) {
            return new ImmunisationTransformer();
        } else if (resourceType == ResourceType.FamilyMemberHistory) {
            return new FamilyMemberHistoryTransformer();
        } else if (resourceType == ResourceType.AllergyIntolerance) {
            return new AllergyIntoleranceTransformer();
        } else if (resourceType == ResourceType.DiagnosticOrder) {
            return new DiagnosticOrderTransformer();
        } else if (resourceType == ResourceType.DiagnosticReport) {
            return new DiagnosticReportTransformer();
        } else if (resourceType == ResourceType.Specimen) {
            return new SpecimenTransformer();
        } else if (resourceType == ResourceType.Slot) {
            //slots are handled in the appointment transformer, so have no dedicated one
            return null;
        } else {
            throw new TransformException("Unhandled resource type " + resourceType);
        }
    }

    private static void transformResources(ResourceType resourceType,
                                         List<ResourceWrapper> resources,
                                         ThreadPool threadPool,
                                         SubscriberTransformParams params) throws Exception {

        //find all the ones we want to transform
        List<ResourceWrapper> resourcesToTransform = new ArrayList<>();

        for (int i=resources.size()-1; i>=0; i--) {
            ResourceWrapper resource = resources.get(i);
            String resourceResourceType = resource.getResourceType();

            //if it matches our type, then remove from the original list
            if (resourceResourceType.equals(resourceType.toString())) {
                resourcesToTransform.add(resource);
                resources.remove(i);
            }
        }

        if (resourcesToTransform.isEmpty()) {
            return;
        }

        //transform in batches
        List<ResourceWrapper> batch = new ArrayList<>();
        AbstractSubscriberTransformer transformer = createTransformerForResourceType(resourceType);

        //some resource types don't have a transformer, so just return out
        if (transformer == null) {
            return;
        }

        for (ResourceWrapper resource: resourcesToTransform) {

            batch.add(resource);

            if (batch.size() >= params.getBatchSize()) {
                addBatchToThreadPool(transformer, batch, threadPool, params);

                //create a new batch and transformer
                batch = new ArrayList<>();
                transformer = createTransformerForResourceType(resourceType);
            }
        }

        //don't forget to do any in the last batch
        if (!batch.isEmpty()) {
            addBatchToThreadPool(transformer, batch, threadPool, params);
        }

    }

    private static void addBatchToThreadPool(AbstractSubscriberTransformer transformer,
                                             List<ResourceWrapper> resources,
                                             ThreadPool threadPool,
                                             SubscriberTransformParams params) throws Exception {

        TransformResourceCallable callable = new TransformResourceCallable(transformer, resources, params);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        handleErrors(errors);
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        Throwable cause = first.getException();
        //the cause may be an Exception or Error so we need to explicitly
        //cast to the right type to throw it without changing the method signature
        if (cause instanceof Exception) {
            throw (Exception)cause;
        } else if (cause instanceof Error) {
            throw (Error)cause;
        }
    }



    /*private static void transformResources(ResourceType resourceType,
                                          AbstractTransformer transformer,
                                          OutputContainer data,
                                          List<ResourceByExchangeBatch> resources,
                                          Map<String, ResourceByExchangeBatch> resourcesMap,
                                          Long enterpriseOrganisationId,
                                          Long enterprisePatientId,
                                          Long enterprisePersonId,
                                          String configName) throws Exception {

        HashSet<ResourceByExchangeBatch> resourcesProcessed = new HashSet<>();

        *//*for (int i=resources.size()-1; i>=0; i--) {
            ResourceByExchangeBatch resource = resources.get(i);*//*
        for (ResourceByExchangeBatch resource: resources) {
            if (resource.getResourceType().equals(resourceType.toString())) {

                //we use this function with a null transformer for resources we want to ignore
                if (transformer != null) {
                    try {
                        transformer.transform(resource, data, resourcesMap, enterpriseOrganisationId, enterprisePatientId, enterprisePersonId, configName);
                    } catch (Exception ex) {
                        throw new TransformException("Exception transforming " + resourceType + " " + resource.getResourceId(), ex);
                    }

                }

                resourcesProcessed.add(resource);
                //resources.remove(i);
            }
        }

        //remove all the resources we processed, so we can check for ones we missed at the end
        resources.removeAll(resourcesProcessed);
    }*/

    /**
     * hashes the resources by a reference to them, so the transforms can quickly look up dependant resources
     */
    private static Map<String, ResourceWrapper> hashResourcesByReference(List<ResourceWrapper> allResources) throws Exception {

        Map<String, ResourceWrapper> ret = new HashMap<>();

        for (ResourceWrapper resource: allResources) {

            ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());
            String resourceId = resource.getResourceId().toString();

            Reference reference = ReferenceHelper.createReference(resourceType, resourceId);
            String referenceStr = reference.getReference();
            ret.put(referenceStr, resource);
        }

        return ret;
    }

    static class TransformResourceCallable implements Callable {

        private AbstractSubscriberTransformer transformer = null;
        private List<ResourceWrapper> resources = null;
        private SubscriberTransformParams params = null;

        public TransformResourceCallable(AbstractSubscriberTransformer transformer,
                                         List<ResourceWrapper> resources,
                                         SubscriberTransformParams params) {

            this.transformer = transformer;
            this.resources = resources;
            this.params = params;
        }

        @Override
        public Object call() throws Exception {
            transformer.transformResources(resources, params);
            return null;
        }
    }

}
