package org.endeavourhealth.transform.enterprise;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberInstanceMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberOrgMappingDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.FhirToXTransformerBase;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.enterprise.outputModels.OutputContainer;
import org.endeavourhealth.transform.enterprise.transforms.*;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class FhirToEnterpriseCsvTransformer extends FhirToXTransformerBase {

    private static final Logger LOG = LoggerFactory.getLogger(FhirToEnterpriseCsvTransformer.class);

    public static String transformFromFhir(UUID serviceId,
                                           UUID systemId,
                                           UUID exchangeId,
                                           UUID batchId,
                                           List<ResourceWrapper> resources,
                                           String configName,
                                           boolean isBulkDeleteFromSubscriber) throws Exception {

        LOG.trace("Transforming batch " + batchId + " and " + resources.size() + " resources for service " + serviceId + " -> " + configName);

        EnterpriseTransformHelper params = new EnterpriseTransformHelper(serviceId, systemId, exchangeId, batchId, configName, resources, isBulkDeleteFromSubscriber);

        Long enterpriseOrgId = findEnterpriseOrgId(serviceId, params);
        params.setEnterpriseOrganisationId(enterpriseOrgId);

        OutputContainer data = params.getOutputContainer();

        //sometimes we may fail to find an org id, so just return null as there's nothing to send
        if (enterpriseOrgId == null) {
            return null;
        }

        try {
            runTransforms(params);

            byte[] bytes = data.writeToZip();
            if (bytes != null) {
                return Base64.getEncoder().encodeToString(bytes);
            } else {
                return null;
            }

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

    public static Long findEnterpriseOrgId(UUID serviceId, EnterpriseTransformHelper params) throws Exception {

        //if we've previously transformed for our ODS code, then we'll have a mapping to the enterprise ID for that ODS code
        SubscriberOrgMappingDalI subscriberOrgDal = DalProvider.factorySubscriberOrgMappingDal(params.getEnterpriseConfigName());
        Long enterpriseOrganisationId = subscriberOrgDal.findEnterpriseOrganisationId(serviceId.toString());
        if (enterpriseOrganisationId != null) {
            return enterpriseOrganisationId;
        }

        //if this is our first time transforming for our org, then we need to find the FHIR resource
        //that represents our organisation. Unfortunately, the very first batch for an org will
        //not contain enough info to work out which resource is our interesting one, so we need to
        //rely on there being a patient resource that tells us.
        Reference orgReference = EnterpriseTransformHelper.findOrganisationReferenceForPublisher(serviceId);

        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(orgReference);
        ResourceType resourceType = comps.getResourceType();
        UUID resourceId = UUID.fromString(comps.getId());
        //LOG.info("Managing organisation is " + resourceType + " " + resourceId);

        SubscriberInstanceMappingDalI instanceMappingDal = DalProvider.factorySubscriberInstanceMappingDal(params.getEnterpriseConfigName());
        ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

        //we need to see if our organisation is mapped to another instance of the same place,
        //in which case we need to use the enterprise ID of that other instance
        UUID mappedResourceId = instanceMappingDal.findInstanceMappedId(resourceType, resourceId);
        if (mappedResourceId == null) {
            //if we've not got a mapping, then we need to create one from our resource data
            Resource fhir = resourceRepository.getCurrentVersionAsResource(serviceId, resourceType, resourceId.toString());
            String mappingValue = AbstractEnterpriseTransformer.findInstanceMappingValue(fhir, params);
            mappedResourceId = instanceMappingDal.findOrCreateInstanceMappedId(resourceType, resourceId, mappingValue);
        }

        //if our mapped resource ID is different to our proper ID, then there's a different instance of our organisation
        //already on the database. So we want to "take over" that organisation record, with our own instance
        if (!mappedResourceId.equals(resourceId)) {
            instanceMappingDal.takeOverInstanceMapping(resourceType, mappedResourceId, resourceId);
        }

        //generate (or find) an enterprise ID for our organization
        enterpriseOrganisationId = AbstractEnterpriseTransformer.findOrCreateEnterpriseId(params, resourceType.toString(), resourceId.toString());
        //LOG.info("Created enterprise org ID " + enterpriseOrganisationId);

        //we also want to ensure that our organisation is transformed right now, so need to make sure it's in our list of resources
        ResourceWrapper resourceWrapper = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), resourceId);
        params.addResourceToTransform(resourceWrapper);

        //and store the organization's enterprise ID in a separate table so we don't have to repeat all this next time
        subscriberOrgDal.saveEnterpriseOrganisationId(serviceId.toString(), enterpriseOrganisationId);
        return enterpriseOrganisationId;
    }




    private static void runTransforms(EnterpriseTransformHelper params) throws Exception {

        int resourceCount = params.getResourceCount();
        int threads = Math.min(10, resourceCount/10); //limit to 10 threads, but don't create too many unnecessarily if we only have a few resources
        threads = Math.max(threads, 1); //make sure we have a min of 1

        ThreadPool threadPool = new ThreadPool(threads, 1000, "FhirToEnterprise");
        try {

            //we detect whether we're doing an update or insert, based on whether we're previously mapped
            //a reference to a resource, so we need to transform the resources in a specific order, so
            //that we transform resources before we ones that refer to them
            transformResources(ResourceType.Organization, threadPool, params);
            transformResources(ResourceType.Location, threadPool, params);
            transformResources(ResourceType.Practitioner, threadPool, params);
            transformResources(ResourceType.Schedule, threadPool, params);
            transformResources(ResourceType.Patient, threadPool, params);

            //if we transformed a patient resource, we need to guarantee that the patient is fully transformed and saved before continuing
            //so we need to close the thread pool and wait. Then re-open for any remaining resources.
            List<ThreadPoolError> errors = threadPool.waitUntilEmpty();
            handleErrors(errors);

            //having done any patient resource in our batch, we should have created an enterprise patient ID and person ID that we can use for all remaining resources
            params.populatePatientAndPersonIds();

            transformResources(ResourceType.EpisodeOfCare, threadPool, params);
            transformResources(ResourceType.Appointment, threadPool, params);
            transformResources(ResourceType.Encounter, threadPool, params);
            transformResources(ResourceType.Condition, threadPool, params);
            transformResources(ResourceType.Procedure, threadPool, params);
            transformResources(ResourceType.ReferralRequest, threadPool, params);
            transformResources(ResourceType.ProcedureRequest, threadPool, params);
            transformResources(ResourceType.Observation, threadPool, params);
            transformResources(ResourceType.MedicationStatement, threadPool, params);
            transformResources(ResourceType.MedicationOrder, threadPool, params);
            transformResources(ResourceType.Immunization, threadPool, params);
            transformResources(ResourceType.FamilyMemberHistory, threadPool, params);
            transformResources(ResourceType.AllergyIntolerance, threadPool, params);
            transformResources(ResourceType.DiagnosticOrder, threadPool, params);
            transformResources(ResourceType.DiagnosticReport, threadPool, params);
            transformResources(ResourceType.Specimen, threadPool, params);
            transformResources(ResourceType.Flag, threadPool, params);
            transformResources(ResourceType.QuestionnaireResponse, threadPool, params); //Note we return null for QuestionnaireResponse as we don't know how to store them yet

            //for these resource types, call with a null transformer as they're actually transformed when
            //doing one of the above entities, but we want to remove them from the resources list
            transformResources(ResourceType.Slot, threadPool, params);

        } finally {
            //close the thread pool
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }

        //if there's anything left in the list, then we've missed a resource type
        params.checkForMissedResources();
    }

    public static AbstractEnterpriseCsvWriter findCsvWriterForResourceType(ResourceType resourceType, EnterpriseTransformHelper params) throws Exception {

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
        } else if (resourceType == resourceType.QuestionnaireResponse) {
            //TODO Work out how to store them later
            return null;
        } else if (resourceType == resourceType.Composition) {
            //TODO Work out how to store them later if relevant
            return null;
        } else {
            throw new TransformException("Unhandled resource type " + resourceType);
        }
    }

    public static AbstractEnterpriseTransformer createTransformerForResourceType(ResourceType resourceType) throws Exception {
        if (resourceType == ResourceType.Organization) {
            return new OrganisationEnterpriseTransformer();
        } else if (resourceType == ResourceType.Location) {
            return new LocationEnterpriseTransformer();
        } else if (resourceType == ResourceType.Practitioner) {
            return new PractitionerEnterpriseTransformer();
        } else if (resourceType == ResourceType.Schedule) {
            return new ScheduleEnterpriseTransformer();
        } else if (resourceType == ResourceType.Patient) {
            return new PatientEnterpriseTransformer();
        } else if (resourceType == ResourceType.EpisodeOfCare) {
            return new EpisodeOfCareEnterpriseTransformer();
        } else if (resourceType == ResourceType.Appointment) {
            return new AppointmentEnterpriseTransformer();
        } else if (resourceType == ResourceType.Encounter) {
            return new EncounterEnterpriseTransformer();
        } else if (resourceType == ResourceType.Flag) {
            return new FlagEnterpriseTransformer();
        } else if (resourceType == ResourceType.Condition) {
            return new ConditionEnterpriseTransformer();
        } else if (resourceType == ResourceType.Procedure) {
            return new ProcedureEnterpriseTransformer();
        } else if (resourceType == ResourceType.ReferralRequest) {
            return new ReferralRequestEnterpriseTransformer();
        } else if (resourceType == ResourceType.ProcedureRequest) {
            return new ProcedureRequestEnterpriseTransformer();
        } else if (resourceType == ResourceType.Observation) {
            return new ObservationEnterpriseTransformer();
        } else if (resourceType == ResourceType.MedicationStatement) {
            return new MedicationStatementEnterpriseTransformer();
        } else if (resourceType == ResourceType.MedicationOrder) {
            return new MedicationOrderEnterpriseTransformer();
        } else if (resourceType == ResourceType.Immunization) {
            return new ImmunisationEnterpriseTransformer();
        } else if (resourceType == ResourceType.FamilyMemberHistory) {
            return new FamilyMemberHistoryEnterpriseTransformer();
        } else if (resourceType == ResourceType.AllergyIntolerance) {
            return new AllergyIntoleranceEnterpriseTransformer();
        } else if (resourceType == ResourceType.DiagnosticOrder) {
            return new DiagnosticOrderEnterpriseTransformer();
        } else if (resourceType == ResourceType.DiagnosticReport) {
            return new DiagnosticReportEnterpriseTransformer();
        } else if (resourceType == ResourceType.Specimen) {
            return new SpecimenEnterpriseTransformer();
        } else if (resourceType == ResourceType.Slot) {
            //slots are handled in the appointment transformer, so have no dedicated one
            return null;
        } else if (resourceType == resourceType.QuestionnaireResponse) {
            //TODO Work out how to store them later
            return null;
        } else if (resourceType == resourceType.Composition) {
            //TODO Work out how to store them later if relevant
            return null;
        } else {
            throw new TransformException("Unhandled resource type " + resourceType);
        }
    }

    private static void transformResources(ResourceType resourceType, ThreadPool threadPool, EnterpriseTransformHelper params) throws Exception {

        //find all the ones we want to transform
        List<ResourceWrapper> resourcesToTransform = params.findResourcesForType(resourceType);
        AbstractEnterpriseCsvWriter csvWriter = findCsvWriterForResourceType(resourceType, params);

        //transform in batches
        List<ResourceWrapper> batch = new ArrayList<>();
        for (ResourceWrapper resource: resourcesToTransform) {
            batch.add(resource);

            if (batch.size() >= params.getBatchSize()) {
                addBatchToThreadPool(resourceType, csvWriter, batch, threadPool, params);
                batch = new ArrayList<>();
            }
        }

        //don't forget to do any in the last batch
        if (!batch.isEmpty()) {
            addBatchToThreadPool(resourceType, csvWriter, batch, threadPool, params);
        }
    }

    private static void addBatchToThreadPool(ResourceType resourceType,
                                             AbstractEnterpriseCsvWriter csvWriter,
                                             List<ResourceWrapper> resources,
                                             ThreadPool threadPool,
                                             EnterpriseTransformHelper params) throws Exception {

        Callable callable = new TransformResourceCallable(resourceType, resources, csvWriter, params);
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


    static class TransformResourceCallable implements Callable {

        private ResourceType resourceType = null;
        private List<ResourceWrapper> resources = null;
        private AbstractEnterpriseCsvWriter csvWriter = null;
        private EnterpriseTransformHelper params = null;

        public TransformResourceCallable(ResourceType resourceType,
                                         List<ResourceWrapper> resources,
                                         AbstractEnterpriseCsvWriter csvWriter,
                                         EnterpriseTransformHelper params) {

            this.resourceType = resourceType;
            this.resources = resources;
            this.csvWriter = csvWriter;
            this.params = params;
        }

        @Override
        public Object call() throws Exception {

            AbstractEnterpriseTransformer transformer = createTransformerForResourceType(resourceType);
            if (transformer != null) {
                transformer.transformResources(resources, csvWriter, params);

            } else {
                //if no transformer (some resource types don't have one), then tell our helper that we've dealt with our resources
                //so we don't get an error for missing some
                for (ResourceWrapper resourceWrapper: resources) {
                    params.setResourceAsSkipped(resourceWrapper);
                }
            }
            return null;
        }
    }

}
