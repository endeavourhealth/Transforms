package org.endeavourhealth.transform.subscriber;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberInstanceMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberOrgMappingDalI;
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

public class FhirToSubscriberCsvTransformer extends FhirToXTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(FhirToSubscriberCsvTransformer.class);

    private static final PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();

    public static String transformFromFhir(UUID serviceId,
                                           UUID systemId,
                                           UUID exchangeId,
                                           UUID batchId,
                                           List<ResourceWrapper> resources,
                                           SubscriberConfig subscriberConfig,
                                           boolean isBulkDeleteFromSubscriber) throws Exception {

        LOG.info("Transforming batch " + batchId + " and " + resources.size() + " resources for service " + serviceId + " -> " + subscriberConfig.getSubscriberConfigName());

        validateResources(resources);

        SubscriberTransformHelper params = new SubscriberTransformHelper(serviceId, systemId, exchangeId, batchId, subscriberConfig, resources, isBulkDeleteFromSubscriber);

        Long enterpriseOrgId = findEnterpriseOrgId(serviceId, params, resources);
        params.setSubscriberOrganisationId(enterpriseOrgId);

        //sometimes we may fail to find an org id, so just return null as there's nothing to send yet
        if (enterpriseOrgId == null) {
            return null;
        }

        try {

            LOG.trace("Going to run transforms");
            runTransforms(params);

            //LOG.trace("Transform finished, will write to Base64");
            OutputContainer data = params.getOutputContainer();
            byte[] bytes = data.writeToZip();

            //update the state table so we know the datetime of each resource we just transformed, so the event log is maintained properly
            params.saveDtLastTransformedPatient();
            //updateSubscriberStateTable(params);

            LOG.trace("Transform complete, generating " + (bytes != null ? "" + bytes.length : null) + " bytes");
            if (bytes != null) {
                return Base64.getEncoder().encodeToString(bytes);
            } else {
                return null;
            }

        } catch (Exception ex) {
            throw new TransformException("Exception transforming batch " + batchId, ex);
        }
    }


    /**
     * performs validation on the resources to make sure everything is as expected
     */
    public static void validateResources(List<ResourceWrapper> resources) throws Exception {

        //validate all resources are for the same patient
        UUID patientId = null;
        for (ResourceWrapper w: resources) {
            UUID wPatientId = w.getPatientId();
            if (wPatientId != null) { //don't care about admin resources being mixed in with patient ones
                if (patientId == null) {
                    patientId = wPatientId;
                } else if (!patientId.equals(wPatientId)) {
                    throw new Exception("Resources for different patients found in batch");
                }
            }
        }

        //add any other required validation here
    }


    /*private static void updateSubscriberStateTable(SubscriberTransformHelper params) throws Exception {

        SubscriberResourceMappingDalI resourceMappingDal = DalProvider.factorySubscriberResourceMappingDal(params.getSubscriberConfigName());

        List<SubscriberId> map = params.getSubscriberIdsUpdated();

        List<SubscriberId> batch = new ArrayList<>();
        for (SubscriberId id: map) {

            //this field is ONLY ever used for patient resources, so will be moved to a separate table specific
            //for patient resources. In the meantime, minimise DB updates by only updating for patient resources
            if (id.getSubscriberTable() != SubscriberTableId.PATIENT.getId()) {
                continue;
            }

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
    }*/

    /**
     * finds or creates the ID for the organization that all this data is for
     */
    public static Long findEnterpriseOrgId(UUID serviceId, SubscriberTransformHelper params, List<ResourceWrapper> resources) throws Exception {

        //if we've previously transformed for our ODS code, then we'll have a mapping to the enterprise ID for that ODS code
        SubscriberOrgMappingDalI subscriberInstanceMappingDal = DalProvider.factorySubscriberOrgMappingDal(params.getSubscriberConfigName());
        Long enterpriseOrganisationId = subscriberInstanceMappingDal.findEnterpriseOrganisationId(serviceId.toString());
        if (enterpriseOrganisationId != null) {
            return enterpriseOrganisationId;
        }

        //if this is our first time transforming for our org, then we need to find the FHIR resource
        //that represents our organisation. Unfortunately, the very first batch for an org will
        //not contain enough info to work out which resource is our interesting one, so we need to
        //rely on there being a patient resource that tells us.
        Reference orgReference = SubscriberTransformHelper.findOrganisationReferenceForPublisher(serviceId);
        if (orgReference == null) {
            // if this happens, there is no patient organisation found so don't send anything to Subscriber,
            // as it'll all be sorted out when they do send patient data.
            return null;
        }

        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(orgReference);
        ResourceType resourceType = comps.getResourceType();
        UUID resourceId = UUID.fromString(comps.getId());
        //LOG.info("Managing organisation is " + resourceType + " " + resourceId);

        SubscriberInstanceMappingDalI instanceMapper = DalProvider.factorySubscriberInstanceMappingDal(params.getSubscriberConfigName());
        ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

        //we need to see if our organisation is mapped to another instance of the same place,
        //in which case we need to use the enterprise ID of that other instance
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
        enterpriseOrganisationId = new Long(subscriberId.getSubscriberId());
        //LOG.info("Created enterprise org ID " + enterpriseOrganisationId);

        //we also want to ensure that our organisation is transformed right now, so need to make sure it's in our list of resources
        ResourceWrapper resourceWrapper = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), resourceId);
        if (resourceWrapper == null) {
            LOG.warn("Null resource history returned for : " + resourceType.toString() + ":" + resourceId + ",org:" + enterpriseOrganisationId + ":" + sourceId);
        } else {
            params.addResourceToTransform(resourceWrapper);
        }
        //and store the organization's enterprise ID in a separate table so we don't have to repeat all this next time
        subscriberInstanceMappingDal.saveEnterpriseOrganisationId(serviceId.toString(), enterpriseOrganisationId);

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

    private static void runTransforms(SubscriberTransformHelper params) throws Exception {

        int resourceCount = params.getResourceCount();
        int threads = Math.min(10, resourceCount/10); //limit to 10 threads, but don't create too many unnecessarily if we only have a few resources
        threads = Math.max(threads, 1); //make sure we have a min of 1

        ThreadPool threadPool = new ThreadPool(threads, 1000, "FhirToSubscriber");
        try {

            //we detect whether we're doing an update or insert, based on whether we're previously mapped
            //a reference to a resource, so we need to transform the resources in a specific order, so
            //that we transform resources before we ones that refer to them
            transformResources(ResourceType.Organization, threadPool, params);
            transformResources(ResourceType.Location, threadPool, params);
            transformResources(ResourceType.Practitioner, threadPool, params);
            transformResources(ResourceType.Schedule, threadPool, params);
            transformResources(ResourceType.Patient, threadPool, params);

            //if we transformed a patient resource, we need to guarantee that the patient is fully transformed before continuing
            //so we need to let the threadpool empty before doing anything more
            List<ThreadPoolError> errors = threadPool.waitUntilEmpty();
            handleErrors(errors);

            //having done any patient resource in our batch, we should have created an enterprise patient ID and person ID that we can use for all remaining resources
            params.populatePatientAndPersonIds();

            //order of the transforms is generally in order of dependence
            transformResources(ResourceType.Appointment, threadPool, params);
            transformResources(ResourceType.EpisodeOfCare, threadPool, params);
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
            transformResources(ResourceType.Slot, threadPool, params);
            transformResources(ResourceType.QuestionnaireResponse, threadPool, params);

        } finally {

            //close the thread pool
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }

        //if there's anything left in the list, then we've missed a resource type
        params.checkForMissedResources();
    }


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
        } else if (resourceType == ResourceType.QuestionnaireResponse) {
            //no target table for questionnaire reponse
            return null;
        } else {
            throw new TransformException("Unhandled resource type " + resourceType);
        }
    }

    private static void transformResources(ResourceType resourceType, ThreadPool threadPool, SubscriberTransformHelper params) throws Exception {

        //find all the ones we want to transform
        List<ResourceWrapper> resourcesToTransform = params.findResourcesForType(resourceType);

        //transform in batches
        List<ResourceWrapper> batch = new ArrayList<>();
        for (ResourceWrapper resource: resourcesToTransform) {
            batch.add(resource);

            if (batch.size() >= params.getBatchSize()) {
                addBatchToThreadPool(resourceType, batch, threadPool, params);
                batch = new ArrayList<>();
            }
        }

        //don't forget to do any in the last batch
        if (!batch.isEmpty()) {
            addBatchToThreadPool(resourceType, batch, threadPool, params);
        }
    }

    private static void addBatchToThreadPool(ResourceType resourceType,
                                             List<ResourceWrapper> resources,
                                             ThreadPool threadPool,
                                             SubscriberTransformHelper params) throws Exception {

        Callable callable = new TransformResourceCallable(resourceType, resources, params);
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



    static class TransformResourceCallable implements Callable {

        private ResourceType resourceType = null;
        private List<ResourceWrapper> resources = null;
        private SubscriberTransformHelper params = null;

        public TransformResourceCallable(ResourceType resourceType,
                                         List<ResourceWrapper> resources,
                                         SubscriberTransformHelper params) {

            this.resourceType = resourceType;
            this.resources = resources;
            this.params = params;
        }

        @Override
        public Object call() throws Exception {

            AbstractSubscriberTransformer transformer = createTransformerForResourceType(resourceType);
            if (transformer != null) {
                transformer.transformResources(resources, params);

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
