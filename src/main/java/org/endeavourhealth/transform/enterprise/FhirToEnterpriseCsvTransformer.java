package org.endeavourhealth.transform.enterprise;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.data.ehr.ResourceRepository;
import org.endeavourhealth.core.data.ehr.models.ResourceByExchangeBatch;
import org.endeavourhealth.core.data.ehr.models.ResourceByService;
import org.endeavourhealth.core.data.ehr.models.ResourceHistory;
import org.endeavourhealth.core.rdbms.eds.PatientLinkHelper;
import org.endeavourhealth.core.rdbms.transform.EnterpriseIdHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirToXTransformerBase;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.enterprise.outputModels.OutputContainer;
import org.endeavourhealth.transform.enterprise.transforms.*;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public class FhirToEnterpriseCsvTransformer extends FhirToXTransformerBase {

    private static final Logger LOG = LoggerFactory.getLogger(FhirToEnterpriseCsvTransformer.class);

    private static final int DEFAULT_TRANSFORM_BATCH_SIZE = 50;
    //private static Map<String, Integer> transformBatchSizeCache = new HashMap<>();

    public static String transformFromFhir(UUID serviceId,
                                           UUID systemId,
                                           UUID batchId,
                                           Map<ResourceType,
                                           List<UUID>> resourceIds,
                                           String configName,
                                           UUID protocolId) throws Exception {

        //retrieve our resources
        List<ResourceByExchangeBatch> filteredResources = getResources(batchId, resourceIds);
        if (filteredResources.isEmpty()) {
            return null;
        }

        LOG.trace("Transforming batch " + batchId + " and " + filteredResources.size() + " resources for service " + serviceId + " -> " + configName);

        //hash the resources by reference to them, so the transforms can quickly look up dependant resources
        Map<String, ResourceByExchangeBatch> resourcesMap = hashResourcesByReference(filteredResources);

        JsonNode config = ConfigManager.getConfigurationAsJson(configName, "subscriber");
        boolean pseudonymised = config.get("pseudonymised").asBoolean();

        //nasty workaround to handle a column missing from a table in AIMES
        boolean hasProblemEndDate = true;
        if (config.has("problem_end_date")) {
            hasProblemEndDate = config.get("problem_end_date").asBoolean();
        }

        int batchSize = DEFAULT_TRANSFORM_BATCH_SIZE;
        if (config.has("transform_batch_size")) {
            batchSize = config.get("transform_batch_size").asInt();
        }
        //int batchSize = findTransformBatchSize(configName);

        OutputContainer data = new OutputContainer(pseudonymised, hasProblemEndDate);
        EnterpriseTransformParams params = new EnterpriseTransformParams(protocolId, configName, data, resourcesMap);

        Long enterpriseOrgId = findEnterpriseOrgId(serviceId, systemId, params);
        params.setEnterpriseOrganisationId(enterpriseOrgId);
        params.setBatchSize(batchSize);

        //sometimes we may fail to find an org id, so just return null as there's nothing to send
        if (enterpriseOrgId == null) {
            return null;
        }

        try {
            tranformResources(filteredResources, params);

            byte[] bytes = data.writeToZip();
            return Base64.getEncoder().encodeToString(bytes);

        } catch (Exception ex) {
            throw new TransformException("Exception transforming batch " + batchId, ex);
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

    private static Long findEnterpriseOrgId(UUID serviceId, UUID systemId, EnterpriseTransformParams params) throws Exception {

        //if we've previously transformed for our ODS code, then we'll have a mapping to the enterprise ID for that ODS code
        Long enterpriseOrganisationId = EnterpriseIdHelper.findEnterpriseOrganisationId(serviceId.toString(), systemId.toString(), params.getEnterpriseConfigName());
        if (enterpriseOrganisationId != null) {
            return enterpriseOrganisationId;
        }

        //if this is our first time transforming for our org, then we need to find the FHIR resource
        //that represents our organisation. Unfortunately, the very first batch for an org will
        //not contain enough info to work out which resource is our interesting one, so we need to
        //rely on there being a patient resource that tells us.
        ResourceRepository resourceRepository = new ResourceRepository();
        ResourceByService resourceByService = resourceRepository.getFirstResourceByService(serviceId, systemId, ResourceType.Patient);
        if (resourceByService == null) {
            //Emis sometimes activate practices before they send up patient data, so we may have a service with all the
            //non-patient metadata, but no patient data. If this happens, then don't send anything to Enterprise, as
            //it'll all be sorted out when they do send patient data.
            return null;
            //throw new TransformException("Cannot find a Patient resource for service " + serviceId + " and system " + systemId);
        }

        String json = resourceByService.getResourceData();

        //if the first patient has been deleted, then we need to look at its history to find the JSON from when it wasn't deleted
        if (Strings.isNullOrEmpty(json)) {
            List<ResourceHistory> history = resourceRepository.getResourceHistory(resourceByService.getResourceType(), resourceByService.getResourceId());
            for (ResourceHistory historyItem: history) {
                json = historyItem.getResourceData();
                if (!Strings.isNullOrEmpty(json)) {
                    break;
                }
            }
        }

        Patient patient = (Patient) AbstractTransformer.deserialiseResouce(json);
        if (!patient.hasManagingOrganization()) {
            throw new TransformException("Patient " + patient.getId() + " doesn't have a managing org for service " + serviceId + " and system " + systemId);
        }

        Reference orgReference = patient.getManagingOrganization();
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(orgReference);
        ResourceType resourceType = comps.getResourceType();
        String resourceId = comps.getId();

        enterpriseOrganisationId = AbstractTransformer.findOrCreateEnterpriseId(params, resourceType.toString(), resourceId);

        EnterpriseIdHelper.saveEnterpriseOrganisationId(serviceId.toString(), systemId.toString(), params.getEnterpriseConfigName(), enterpriseOrganisationId);

        return enterpriseOrganisationId;
    }

    /**
     * all resources in a batch are for the same patient (or no patient at all), so rather than looking
     * up the Enterprise patient ID for each resource, we can do it once at the start. To do that
     * we need the Discovery patient ID from one of the resources.
     */
    private static String findPatientId(List<ResourceByExchangeBatch> resourceWrappers) throws Exception {

        for (ResourceByExchangeBatch resourceWrapper: resourceWrappers) {
            if (resourceWrapper.getIsDeleted()) {
                continue;
            }

            String resourceTypeStr = resourceWrapper.getResourceType();
            ResourceType resourceType = ResourceType.valueOf(resourceTypeStr);
            if (!FhirResourceFiler.isPatientResource(resourceType)) {
                continue;
            }

            Resource resource = AbstractTransformer.deserialiseResouce(resourceWrapper);
            String patientId = IdHelper.getPatientId(resource);
            if (Strings.isNullOrEmpty(patientId)) {
                continue;
            }

            return patientId;
        }

        return null;
    }


    private static void tranformResources(List<ResourceByExchangeBatch> resources,
                                          EnterpriseTransformParams params) throws Exception {



        int threads = Math.min(10, resources.size()/10); //limit to 10 threads, but don't create too many unnecessarily if we only have a few resources
        threads = Math.max(threads, 1); //make sure we have a min of 1

        ThreadPool threadPool = new ThreadPool(threads, 1000);

        //we detect whether we're doing an update or insert, based on whether we're previously mapped
        //a reference to a resource, so we need to transform the resources in a specific order, so
        //that we transform resources before we ones that refer to them
        tranformResources(ResourceType.Organization, resources, threadPool, params);
        tranformResources(ResourceType.Practitioner, resources, threadPool, params);
        tranformResources(ResourceType.Schedule, resources, threadPool, params);
        boolean didPatient = tranformResources(ResourceType.Patient, resources, threadPool, params);

        //if we transformed a patient resource, we need to guarantee that the patient is fully transformed before continuing
        //so we need to close the thread pool and wait. Then re-open for any remaining resources.
        if (didPatient) {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);

            threadPool = new ThreadPool(threads, 1000);
        }

        //having done any patient resource in our batch, we should have created an enterprise patient ID and person ID that we can use for all remaining resources
        String discoveryPatientId = findPatientId(resources);
        if (!Strings.isNullOrEmpty(discoveryPatientId)) {
            Long enterprisePatientId = AbstractTransformer.findEnterpriseId(params, ResourceType.Patient.toString(), discoveryPatientId);
            if (enterprisePatientId == null) {
                //with the Homerton data, we just get data from a point in time, not historic data too, so we have some episodes of
                //care where we don't have patients. If we're in this situation, then don't send over the data.
                LOG.warn("No enterprise patient ID for patient " + discoveryPatientId + " so not doing patient resources");
                return;
                //throw new TransformException("No enterprise patient ID found for discovery patient " + discoveryPatientId);
            }
            params.setEnterprisePatientId(enterprisePatientId);

            String discoveryPersonId = PatientLinkHelper.getPersonId(discoveryPatientId);

            //if we've got some cases where we've got a deleted patient but non-deleted patient-related resources
            //all in the same batch, because Emis sent it like that. In that case we won't have a person ID, so
            //return out without processing any of the remaining resources, since they're for a deleted patient.
            if (Strings.isNullOrEmpty(discoveryPersonId)) {
                return;
            }

            Long enterprisePersonId = EnterpriseIdHelper.findOrCreateEnterprisePersonId(discoveryPersonId, params.getEnterpriseConfigName());
            params.setEnterprisePersonId(enterprisePersonId);
        }

        tranformResources(ResourceType.EpisodeOfCare, resources, threadPool, params);
        tranformResources(ResourceType.Appointment, resources, threadPool, params);
        tranformResources(ResourceType.Encounter, resources, threadPool, params);
        tranformResources(ResourceType.Condition, resources, threadPool, params);
        tranformResources(ResourceType.Procedure, resources, threadPool, params);
        tranformResources(ResourceType.ReferralRequest, resources, threadPool, params);
        tranformResources(ResourceType.ProcedureRequest, resources, threadPool, params);
        tranformResources(ResourceType.Observation, resources, threadPool, params);
        tranformResources(ResourceType.MedicationStatement, resources, threadPool, params);
        tranformResources(ResourceType.MedicationOrder, resources, threadPool, params);
        tranformResources(ResourceType.Immunization, resources, threadPool, params);
        tranformResources(ResourceType.FamilyMemberHistory, resources, threadPool, params);
        tranformResources(ResourceType.AllergyIntolerance, resources, threadPool, params);
        tranformResources(ResourceType.DiagnosticOrder, resources, threadPool, params);
        tranformResources(ResourceType.DiagnosticReport, resources, threadPool, params);
        tranformResources(ResourceType.Specimen, resources, threadPool, params);

        //for these resource types, call with a null transformer as they're actually transformed when
        //doing one of the above entities, but we want to remove them from the resources list
        tranformResources(ResourceType.Slot, resources, threadPool, params);
        tranformResources(ResourceType.Location, resources, threadPool, params);

        //close the thread pool
        List<ThreadPoolError> errors = threadPool.waitAndStop();
        handleErrors(errors);

        //if there's anything left in the list, then we've missed a resource type
        if (!resources.isEmpty()) {
            Set<String> resourceTypesMissed = new HashSet<>();
            for (ResourceByExchangeBatch resource: resources) {
                String resourceType = resource.getResourceType();
                resourceTypesMissed.add(resourceType);
            }
            String s = String.join(", " + resourceTypesMissed);
            throw new TransformException("Transform to Enterprise doesn't handle " + s + " resource type(s)");
        }
    }

    public static AbstractEnterpriseCsvWriter findCsvWriterForResourceType(ResourceType resourceType, EnterpriseTransformParams params) throws Exception {

        OutputContainer data = params.getData();

        if (resourceType == ResourceType.Organization) {
            return data.getOrganisations();
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
        } else if (resourceType == ResourceType.Location) {
            //locations are handled in the organisation transformer, so have no dedicated one
            return null;
        } else {
            throw new TransformException("Unhandled resource type " + resourceType);
        }
    }

    public static AbstractTransformer createTransformerForResourceType(ResourceType resourceType) throws Exception {
        if (resourceType == ResourceType.Organization) {
            return new OrganisationTransformer();
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
        } else if (resourceType == ResourceType.Location) {
            //locations are handled in the organisation transformer, so have no dedicated one
            return null;
        } else {
            throw new TransformException("Unhandled resource type " + resourceType);
        }
    }

    private static boolean tranformResources(ResourceType resourceType,
                                         List<ResourceByExchangeBatch> resources,
                                         ThreadPool threadPool,
                                         EnterpriseTransformParams params) throws Exception {

        //find all the ones we want to transform
        List<ResourceByExchangeBatch> resourcesToTransform = new ArrayList<>();
        HashSet<ResourceByExchangeBatch> hsResourcesToTransform = new HashSet<>();
        for (ResourceByExchangeBatch resource: resources) {
            if (resource.getResourceType().equals(resourceType.toString())) {
                resourcesToTransform.add(resource);
                hsResourcesToTransform.add(resource);
            }
        }

        if (resourcesToTransform.isEmpty()) {
            return false;
        }

        //remove all the resources we processed, so we can check for ones we missed at the end
        //removeAll is really slow, so changing around
        for (int i=resources.size()-1; i>=0; i--) {
            ResourceByExchangeBatch r = resources.get(i);
            if (hsResourcesToTransform.contains(r)) {
                resources.remove(i);
            }
        }
        //resources.removeAll(resourcesToTransform);

        //we use this function with a null transformer for resources we want to ignore
        AbstractTransformer transformer = createTransformerForResourceType(resourceType);
        AbstractEnterpriseCsvWriter csvWriter = findCsvWriterForResourceType(resourceType, params);
        if (transformer != null) {

            List<ResourceByExchangeBatch> batch = new ArrayList<>();

            for (ResourceByExchangeBatch resource: resourcesToTransform) {

                batch.add(resource);

                if (batch.size() >= params.getBatchSize()) {
                    addBatchToThreadPool(transformer, csvWriter, batch, threadPool, params);
                    batch = new ArrayList<>();
                }
            }

            //don't forget to do any in the last batch
            if (!batch.isEmpty()) {
                addBatchToThreadPool(transformer, csvWriter, batch, threadPool, params);
            }
        }

        return true;
    }

    private static void addBatchToThreadPool(AbstractTransformer transformer,
                                             AbstractEnterpriseCsvWriter csvWriter,
                                             List<ResourceByExchangeBatch> resources,
                                             ThreadPool threadPool,
                                             EnterpriseTransformParams params) throws Exception {

        TransformResourceCallable callable = new TransformResourceCallable(transformer,
                                                                        resources,
                                                                        csvWriter,
                                                                        params);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        handleErrors(errors);
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        Exception exception = first.getException();
        throw exception;
    }



    /*private static void tranformResources(ResourceType resourceType,
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
    private static Map<String, ResourceByExchangeBatch> hashResourcesByReference(List<ResourceByExchangeBatch> allResources) throws Exception {

        Map<String, ResourceByExchangeBatch> ret = new HashMap<>();

        for (ResourceByExchangeBatch resource: allResources) {

            ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());
            String resourceId = resource.getResourceId().toString();

            Reference reference = ReferenceHelper.createReference(resourceType, resourceId);
            String referenceStr = reference.getReference();
            ret.put(referenceStr, resource);
        }

        return ret;
    }

    static class TransformResourceCallable implements Callable {

        private AbstractTransformer transformer = null;
        private List<ResourceByExchangeBatch> resources = null;
        private AbstractEnterpriseCsvWriter csvWriter = null;
        private EnterpriseTransformParams params = null;

        public TransformResourceCallable(AbstractTransformer transformer,
                                         List<ResourceByExchangeBatch> resources,
                                         AbstractEnterpriseCsvWriter csvWriter,
                                         EnterpriseTransformParams params) {

            this.transformer = transformer;
            this.resources = resources;
            this.csvWriter = csvWriter;
            this.params = params;
        }

        @Override
        public Object call() throws Exception {
            transformer.transform(resources, csvWriter, params);
            return null;
        }
    }

}
