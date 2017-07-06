package org.endeavourhealth.transform.fhirhl7v2;

import com.google.common.base.Strings;
import org.apache.commons.codec.binary.StringUtils;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.core.data.ehr.ExchangeBatchRepository;
import org.endeavourhealth.core.data.ehr.ResourceRepository;
import org.endeavourhealth.core.data.ehr.models.ExchangeBatch;
import org.endeavourhealth.core.data.ehr.models.ResourceByPatient;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.core.fhirStorage.FhirStorageService;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.ui.helpers.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class FhirHl7v2Filer {
    private static final Logger LOG = LoggerFactory.getLogger(FhirHl7v2Filer.class);

    private static final String ADT_A34 = "ADT^A34";
    private static final String ADT_A35 = "ADT^A35";
    private static final String ADT_A44 = "ADT^A44";

    public void file(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                     TransformError transformError, List<UUID> batchIds, TransformError previousErrors) throws Exception {

        final int maxFilingThreads = 1;
        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds, maxFilingThreads);

        Resource bundleResource = FhirResourceHelper.deserialiseResouce(exchangeBody);

        if (bundleResource.getResourceType() != ResourceType.Bundle)
            throw new Exception("Resource is not a bundle");

        Bundle bundle = (Bundle)bundleResource;

        saveAdminResources(fhirResourceFiler, bundle);
        savePatientResources(fhirResourceFiler, bundle);

        fhirResourceFiler.waitToFinish();

        //need to handle the parameters object being null since we're not receiving it yet in AIMES
        try {
            Parameters parameters = findParameters(bundle);
        } catch (TransformException ex) {
            //if we get an exception, there are no parameter, so just return out
            return;
        }

        //see if there's any special work we need to do for merging/moving
        String adtMessageType = findAdtMessageType(bundle);
        LOG.debug("Received ADT message type " + adtMessageType + " for exchange " + exchangeId);
        if (adtMessageType.equals(ADT_A34)) {
            LOG.debug("Processing A34");
            performA34PatientMerge(exchangeId, serviceId, systemId, batchIds, bundle);

        } else if (adtMessageType.equals(ADT_A35)) {
            LOG.debug("Processing A35");
            performA35EpisodeMerge(exchangeId, serviceId, systemId, batchIds, bundle);

        } else if (adtMessageType.equals(ADT_A44)) {
            LOG.debug("Processing A44");
            performA44EpisodeMove(exchangeId, serviceId, systemId, batchIds, bundle);

        } else {
            //nothing special
        }
    }

    //A44 messages move an entire episode and all its dependant data from one patient to another
    private void performA44EpisodeMove(UUID exchangeId, UUID serviceId, UUID systemId, List<UUID> batchIds, Bundle bundle) throws Exception {
        Parameters parameters = findParameters(bundle);

        String minorPatientId = findParameterValue(parameters, "MinorPatientUuid");
        String majorPatientId = findParameterValue(parameters, "MajorPatientUuid");

        LOG.debug("Doing A44 merge from minor patient " + minorPatientId + " to major patient " + majorPatientId);

        Map<String, String> idMappings = createIdMappings(parameters);

        LOG.debug("Id mappings are");
        for (String key: idMappings.keySet()) {
            String value = idMappings.get(key);
            LOG.debug(key + " -> " + value);
        }

        //add the minor and major patient IDs to the ID map, so we change the patient references in our resources too
        String minorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, minorPatientId.toString());
        String majorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, majorPatientId.toString());
        idMappings.put(minorPatientReference, majorPatientReference);

        UUID majorBatchId = findOrCreateBatchId(exchangeId, batchIds, majorPatientId);
        UUID minorBatchId = findOrCreateBatchId(exchangeId, batchIds, minorPatientId);

        ResourceRepository resourceRepository = new ResourceRepository();
        FhirStorageService storageService = new FhirStorageService(serviceId, systemId);

        List<ResourceByPatient> minorPatientResources = resourceRepository.getResourcesByPatient(serviceId, systemId, UUID.fromString(minorPatientId));

        for (ResourceByPatient minorPatientResource: minorPatientResources) {

            ResourceType resourceType = ResourceType.valueOf(minorPatientResource.getResourceType());
            String resourceReference = createResoourceReferenceValue(minorPatientResource);

            //we only want to change resources that we've been given new IDs for (but make sure to skip
            //the patient resource, as we've manually added its ID into the map)
            if (resourceType == ResourceType.Patient
                    || !idMappings.containsKey(resourceReference)) {
                LOG.debug("Skipping " + resourceType + " " + minorPatientResource.getResourceId());
                continue;
            }

            //if we make it here, we want to remap the resource and move to the new patient
            String json = minorPatientResource.getResourceData();
            Resource fhirOriginal = ParserPool.getInstance().parse(json);

            //copy and remap the resource, then save
            //FHIR copy functions don't copy the ID or Meta, so deserialise twice instead
            Resource fhirAmended = ParserPool.getInstance().parse(json);
            IdHelper.remapIds(fhirAmended, idMappings);

            storageService.exchangeBatchUpdate(exchangeId, majorBatchId, fhirAmended, true);

            //finally delete the resource from the old patient
            storageService.exchangeBatchDelete(exchangeId, minorBatchId, fhirOriginal);

            LOG.debug("Moved " + resourceType + " " + fhirOriginal.getId() + " -> " + fhirAmended.getId());
        }
    }

    //A35 messages merge the contents of one episode (minor) into another one (major) for the same patient
    private void performA35EpisodeMerge(UUID exchangeId, UUID serviceId, UUID systemId, List<UUID> batchIds, Bundle bundle) throws Exception {
        Parameters parameters = findParameters(bundle);

        String patientId = findParameterValue(parameters, "PatientUuid");

        String minorEpisodeOfCareId = findParameterValue(parameters, "MinorEpisodeOfCareUuid");
        String majorEpisodeOfCareId = findParameterValue(parameters, "MajorEpisodeOfCareUuid");

        LOG.debug("Doing A35 merge for patient " + patientId + " and minor episode " + minorEpisodeOfCareId + " to major episode " + majorEpisodeOfCareId);

        String majorEpisodeReference = ReferenceHelper.createResourceReference(ResourceType.Patient, majorEpisodeOfCareId);
        String minorEpisodeReference = ReferenceHelper.createResourceReference(ResourceType.Patient, minorEpisodeOfCareId);

        UUID batchId = findOrCreateBatchId(exchangeId, batchIds, patientId);

        ResourceRepository resourceRepository = new ResourceRepository();
        FhirStorageService storageService = new FhirStorageService(serviceId, systemId);

        List<ResourceByPatient> patientResources = resourceRepository.getResourcesByPatient(serviceId, systemId, UUID.fromString(patientId));

        for (ResourceByPatient patientResource: patientResources) {

            ResourceType resourceType = ResourceType.valueOf(patientResource.getResourceType());
            String json = patientResource.getResourceData();

            if (resourceType == ResourceType.Patient) {
                //don't do anything to the patient

            } else if (resourceType == ResourceType.EpisodeOfCare) {
                //we want to delete the old episode of care
                EpisodeOfCare episodeOfCare = (EpisodeOfCare)ParserPool.getInstance().parse(json);
                if (episodeOfCare.getId().equals(minorEpisodeOfCareId)) {
                    storageService.exchangeBatchDelete(exchangeId, batchId, episodeOfCare);
                    LOG.debug("Deleting episode " + episodeOfCare.getId());
                }

            } else if (resourceType == ResourceType.Encounter) {
                //we want to point encounters at the new episode of care
                Encounter encounter = (Encounter)ParserPool.getInstance().parse(json);

                boolean changed = false;
                if (encounter.hasEpisodeOfCare()) {
                    for (Reference reference: encounter.getEpisodeOfCare()) {
                        if (reference.getReference().equals(minorEpisodeReference)) {
                            reference.setReference(majorEpisodeReference);
                        }
                    }
                }

                if (changed) {
                    storageService.exchangeBatchUpdate(exchangeId, batchId, encounter, false);
                    LOG.debug("Moved Encounter " + encounter.getId() + " to point at " + majorEpisodeReference);
                }

            } else {
                //if we have any other resource type, then something is wrong
                throw new TransformException("Cannot perform A35 episode merge for " + resourceType + " " + patientResource.getResourceId());
            }
        }
    }

    //A34 messages merge all content from one patient (minor patient) to another (the major patient)
    private void performA34PatientMerge(UUID exchangeId, UUID serviceId, UUID systemId, List<UUID> batchIds, Bundle bundle) throws Exception {

        Parameters parameters = findParameters(bundle);

        String majorPatientId = findParameterValue(parameters, "MajorPatientUuid");
        String minorPatientId = findParameterValue(parameters, "MinorPatientUuid");

        LOG.debug("Doing A34 merge from minor patient " + minorPatientId + " to major patient " + majorPatientId);

        Map<String, String> idMappings = createIdMappings(parameters);

        LOG.debug("Id mappings are");
        for (String key: idMappings.keySet()) {
            String value = idMappings.get(key);
            LOG.debug(key + " -> " + value);
        }

        //add the minor and major patient IDs to the ID map, so we change the patient references in our resources too
        String majorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, majorPatientId);
        String minorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, minorPatientId);
        idMappings.put(minorPatientReference, majorPatientReference);

        UUID majorBatchId = findOrCreateBatchId(exchangeId, batchIds, majorPatientId);
        UUID minorBatchId = findOrCreateBatchId(exchangeId, batchIds, minorPatientId);

        ResourceRepository resourceRepository = new ResourceRepository();
        FhirStorageService storageService = new FhirStorageService(serviceId, systemId);

        List<ResourceByPatient> minorPatientResources = resourceRepository.getResourcesByPatient(serviceId, systemId, UUID.fromString(minorPatientId));

        //since we're moving ALL data from the minor to major patients, validate we have a new ID for every resource
        for (ResourceByPatient minorPatientResource: minorPatientResources) {
            String referenceValue = createResoourceReferenceValue(minorPatientResource);

            if (!idMappings.containsKey(referenceValue)) {
                throw new TransformException("Parameters doesn't contain new ID for " + referenceValue);
            }
        }

        //copy the resources to the major patient
        for (ResourceByPatient minorPatientResource: minorPatientResources) {

            String json = minorPatientResource.getResourceData();
            Resource fhirOriginal = ParserPool.getInstance().parse(json);

            //FHIR copy functions don't copy the ID or Meta, so deserialise twice instead
            Resource fhirAmended = ParserPool.getInstance().parse(json);

            if (fhirAmended instanceof Patient) {
                //we don't want to move patient resources, so do nothing and let the delete happen

            } else {
                //for all other resources, re-map the IDs and save to the DB
                try {
                    IdHelper.remapIds(fhirAmended, idMappings);
                    storageService.exchangeBatchUpdate(exchangeId, majorBatchId, fhirAmended, true);

                } catch (Exception ex) {
                    throw new Exception("Failed to save amended " + minorPatientResource.getResourceType() + " which originally had ID " + fhirOriginal.getId() + " and now has " + fhirAmended.getId());
                }
            }

            //finally delete the resource from the old patient
            storageService.exchangeBatchDelete(exchangeId, minorBatchId, fhirOriginal);

            LOG.debug("Moved " + fhirOriginal.getResourceType() + " " + fhirOriginal.getId() + " -> " + fhirAmended.getId());
        }
    }

    //returns a map of old Ids to new, formatted as FHIR references (e.g. Patient/<guid>)
    private static Map<String, String> createIdMappings(Parameters parameters) throws Exception {

        Map<String, String> referenceIdMap = new HashMap<>();

        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase("OldToNewResourceMap")) {

                    for (Parameters.ParametersParameterComponent part: component.getPart()) {
                        String name = part.getName();
                        String value = ((StringType)part.getValue()).toString();

                        referenceIdMap.put(name, value);
                    }
                }
            }
        }

        return referenceIdMap;
    }

    /*private static Map<String, String> createIdMappings(Parameters parameters, List<ResourceByPatient> patientResources) throws Exception {

        Map<String, String> rawIdMap = new HashMap<>();

        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase("ResourceMapping")) {
                    StringType value = (StringType)component.getValue();
                    String s = value.getValue();

                    String[] toks = s.split(":");
                    if (toks.length != 2) {
                        throw new TransformException("Parameter " + s + " doesn't contain exactly two IDs");
                    }

                    rawIdMap.put(toks[0], toks[1]);
                }
            }
        }

        Map<String, String> referenceIdMap = new HashMap<>();

        for (ResourceByPatient patientResource: patientResources) {
            String oldId = patientResource.getResourceId().toString();
            String newId = rawIdMap.get(oldId);
            if (!Strings.isNullOrEmpty(newId)) {

                String resourceTypeStr = patientResource.getResourceType();
                ResourceType resourceType = ResourceType.valueOf(resourceTypeStr);

                String oldReferenceStr = ReferenceHelper.createResourceReference(resourceType, oldId);
                String newReferenceStr = ReferenceHelper.createResourceReference(resourceType, newId);
                referenceIdMap.put(oldReferenceStr, newReferenceStr);
            }
        }

        return referenceIdMap;
    }*/

    private static String findParameterValue(Parameters parameters, String name) throws Exception {
        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase(name)) {
                    StringType value = (StringType)component.getValue();
                    return value.getValue();
                }
            }
        }

        throw new TransformException("Failed to find parameter [" + name + "] in Parameters resource");
    }

    private static String createResoourceReferenceValue(ResourceByPatient resourceByPatient) {
        UUID resourceId = resourceByPatient.getResourceId();
        String resourceTypeStr = resourceByPatient.getResourceType();
        ResourceType resourceType = ResourceType.valueOf(resourceTypeStr);
        return ReferenceHelper.createResourceReference(resourceType, resourceId.toString());
    }

    private UUID findOrCreateBatchId(UUID exchangeId, List<UUID> batchIds, String patientId) {

        //look for a batch ID that already exists for this exchange
        ExchangeBatchRepository exchangeBatchRepository = new ExchangeBatchRepository();
        List<ExchangeBatch> batches = exchangeBatchRepository.retrieveForExchangeId(exchangeId);
        for (ExchangeBatch batch: batches) {
            UUID batchPatientUuid = batch.getEdsPatientId();
            if (batchPatientUuid != null
                    && batchPatientUuid.toString().equals(patientId)) {
                return batch.getBatchId();
            }
        }

        //if we've not got an existing batch for this exchange and patient, then generate a new one
        ExchangeBatch exchangeBatch = FhirResourceFiler.createExchangeBatch(exchangeId);
        exchangeBatch.setEdsPatientId(UUID.fromString(patientId));
        exchangeBatchRepository.save(exchangeBatch);

        //make sure to add to the list of batch IDs created
        UUID batchId = exchangeBatch.getBatchId();
        batchIds.add(batchId);

        return batchId;
    }

    private static Parameters findParameters(Bundle bundle) throws Exception {
        for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
            Resource resource = entry.getResource();
            if (resource.getResourceType() == ResourceType.Parameters) {
                return (Parameters)resource;
            }
        }

        throw new TransformException("Failed to find Parameters resource in Bundle");
    }

    private static String findAdtMessageType(Bundle bundle) throws Exception {
        for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
            Resource resource = entry.getResource();
            if (resource.getResourceType() == ResourceType.MessageHeader) {
                MessageHeader header = (MessageHeader)resource;

                //log out the control ID for now, so we can cross-reference back to the HL7 Receiver
                Extension extension = ExtensionConverter.findExtension(header, FhirExtensionUri.EXTENSION_HL7V2_MESSAGE_CONTROL_ID);
                if (extension != null) {
                    LOG.debug("Received ADT message with control ID " + extension.getValue().toString());
                }

                Coding event = header.getEvent();
                if (event.getSystem().equals(FhirUri.CODE_SYSTEM_HL7V2_MESSAGE_TYPE)) {
                    return event.getCode();
                }
            }
        }

        throw new TransformException("Failed to find MessageHeader resource or valid coding in Bundle");
    }

    private void saveAdminResources(FhirResourceFiler fhirResourceFiler, Bundle bundle) throws Exception {
        List<Resource> adminResources = bundle
                .getEntry()
                .stream()
                .map(t -> t.getResource())
                .filter(t -> !FhirResourceFiler.isPatientResource(t))
                .filter(t -> t.getResourceType() != ResourceType.MessageHeader)
                .filter(t -> t.getResourceType() != ResourceType.Parameters)
                .collect(Collectors.toList());

        fhirResourceFiler.saveAdminResource(null, false, adminResources.toArray(new Resource[0]));
    }

    private void savePatientResources(FhirResourceFiler fhirResourceFiler, Bundle bundle) throws Exception {
        List<Resource> patientResources = bundle
                .getEntry()
                .stream()
                .map(t -> t.getResource())
                .filter(t -> FhirResourceFiler.isPatientResource(t))
                .collect(Collectors.toList());

        Patient patient = patientResources
                .stream()
                .filter(t -> t.getResourceType() == ResourceType.Patient)
                .map(t -> (Patient)t)
                .collect(StreamExtension.singleCollector());

        fhirResourceFiler.savePatientResource(null, false, patient.getId(), patientResources.toArray(new Resource[0]));
    }
}
