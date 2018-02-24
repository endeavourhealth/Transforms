package org.endeavourhealth.transform.fhirhl7v2;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.ExchangeBatchDalI;
import org.endeavourhealth.core.database.dal.audit.models.ExchangeBatch;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.core.fhirStorage.FhirStorageService;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.ResourceMergeMapHelper;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class FhirHl7v2Filer {
    private static final Logger LOG = LoggerFactory.getLogger(FhirHl7v2Filer.class);

    private static final String ADT_A34 = "ADT^A34";
    private static final String ADT_A35 = "ADT^A35";
    private static final String ADT_A44 = "ADT^A44";

    private static final ResourceDalI resourceRepository = DalProvider.factoryResourceDal();


    public void file(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                     TransformError transformError, List<UUID> batchIds, TransformError previousErrors) throws Exception {

        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

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

        LOG.debug("Doing A44 move episode from minor patient " + minorPatientId + " to major patient " + majorPatientId);

        Map<String, String> originalIdMappings = createIdMappings(parameters);

        LOG.debug("Id mappings are");
        for (String key: originalIdMappings.keySet()) {
            String value = originalIdMappings.get(key);
            LOG.debug(key + " -> " + value);
        }

        //copy the ID mappings map and add the patient to it. Note we keep the original map without the patient as
        //we use that to store in our resource merge map table
        Map<String, String> idMappings = new HashMap<>(originalIdMappings);

        //add the minor and major patient IDs to the ID map, so we change the patient references in our resources too
        String minorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, minorPatientId.toString());
        String majorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, majorPatientId.toString());
        idMappings.put(minorPatientReference, majorPatientReference);

        UUID majorBatchId = findOrCreateBatchId(exchangeId, batchIds, majorPatientId);
        UUID minorBatchId = findOrCreateBatchId(exchangeId, batchIds, minorPatientId);

        ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
        FhirStorageService storageService = new FhirStorageService(serviceId, systemId);

        List<ResourceWrapper> minorPatientResources = resourceRepository.getResourcesByPatient(serviceId, systemId, UUID.fromString(minorPatientId));

        for (ResourceWrapper minorPatientResource: minorPatientResources) {

            ResourceType resourceType = ResourceType.valueOf(minorPatientResource.getResourceType());
            String resourceReference = createResourceReferenceValue(minorPatientResource);

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
            IdHelper.applyExternalReferenceMappings(fhirAmended, idMappings, false);

            storageService.exchangeBatchUpdate(exchangeId, majorBatchId, fhirAmended, true);

            //finally delete the resource from the old patient
            storageService.exchangeBatchDelete(exchangeId, minorBatchId, fhirOriginal);

            LOG.debug("Moved " + resourceType + " " + fhirOriginal.getId() + " -> " + fhirAmended.getId());
        }

        //save these resource mappings for the future
        ResourceMergeMapHelper.saveResourceMergeMapping(serviceId, originalIdMappings);

    }

    //A35 messages merge the contents of one episode (minor) into another one (major) for the same patient
    private void performA35EpisodeMerge(UUID exchangeId, UUID serviceId, UUID systemId, List<UUID> batchIds, Bundle bundle) throws Exception {
        Parameters parameters = findParameters(bundle);

        String patientId = findParameterValue(parameters, "PatientUuid");

        String minorEpisodeOfCareId = findParameterValue(parameters, "MinorEpisodeOfCareUuid");
        String majorEpisodeOfCareId = findParameterValue(parameters, "MajorEpisodeOfCareUuid");

        LOG.debug("Doing A35 merge for patient " + patientId + " and minor episode " + minorEpisodeOfCareId + " to major episode " + majorEpisodeOfCareId);

        String majorEpisodeReference = ReferenceHelper.createResourceReference(ResourceType.EpisodeOfCare, majorEpisodeOfCareId);
        String minorEpisodeReference = ReferenceHelper.createResourceReference(ResourceType.EpisodeOfCare, minorEpisodeOfCareId);

        UUID batchId = findOrCreateBatchId(exchangeId, batchIds, patientId);

        ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
        FhirStorageService storageService = new FhirStorageService(serviceId, systemId);

        List<ResourceWrapper> patientResources = resourceRepository.getResourcesByPatient(serviceId, systemId, UUID.fromString(patientId));

        for (ResourceWrapper patientResource: patientResources) {

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
                            changed = true;
                        }
                    }
                }

                if (changed) {
                    storageService.exchangeBatchUpdate(exchangeId, batchId, encounter, false);
                    LOG.debug("Moved Encounter " + encounter.getId() + " to point at " + majorEpisodeReference);
                }

            } else {
                //if we have any other resource type, then something is wrong
                //in DSTU2, there are no other resources that seem to reference episodes of care, so removing this
                //now we've got additional Cerner resource types
                //throw new TransformException("Cannot perform A35 episode merge for " + resourceType + " " + patientResource.getResourceId());
            }
        }

        //save these resource mappings for the future
        ResourceMergeMapHelper.saveResourceMergeMapping(serviceId, majorEpisodeReference, minorEpisodeReference);
    }

    //A34 messages merge all content from one patient (minor patient) to another (the major patient)
    private void performA34PatientMerge(UUID exchangeId, UUID serviceId, UUID systemId, List<UUID> batchIds, Bundle bundle) throws Exception {

        Parameters parameters = findParameters(bundle);

        String majorPatientId = findParameterValue(parameters, "MajorPatientUuid");
        String minorPatientId = findParameterValue(parameters, "MinorPatientUuid");

        LOG.debug("Doing A34 merge from minor patient " + minorPatientId + " to major patient " + majorPatientId);

        Map<String, String> idMappings = createIdMappings(parameters);

        //add the minor and major patient IDs to the ID map, so we change the patient references in our resources too
        String majorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, majorPatientId);
        String minorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, minorPatientId);
        idMappings.put(minorPatientReference, majorPatientReference);

        LOG.debug("Id mappings are");
        for (String key: idMappings.keySet()) {
            String value = idMappings.get(key);
            LOG.debug(key + " -> " + value);
        }

        UUID majorBatchId = findOrCreateBatchId(exchangeId, batchIds, majorPatientId);
        UUID minorBatchId = findOrCreateBatchId(exchangeId, batchIds, minorPatientId);

        ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
        FhirStorageService storageService = new FhirStorageService(serviceId, systemId);

        List<ResourceWrapper> minorPatientResources = resourceRepository.getResourcesByPatient(serviceId, systemId, UUID.fromString(minorPatientId));

        //since we're moving ALL data from the minor to major patients, validate we have a new ID for every resource
        /*for (ResourceWrapper minorPatientResource: minorPatientResources) {
            String referenceValue = createResourceReferenceValue(minorPatientResource);

            if (!idMappings.containsKey(referenceValue)) {
                throw new TransformException("Parameters doesn't contain new ID for " + referenceValue);
            }
        }*/

        //copy the resources to the major patient
        for (ResourceWrapper minorPatientResource: minorPatientResources) {

            String json = minorPatientResource.getResourceData();
            Resource fhirOriginal = ParserPool.getInstance().parse(json);

            //FHIR copy functions don't copy the ID or Meta, so deserialise twice instead
            Resource fhirAmended = ParserPool.getInstance().parse(json);

            if (fhirAmended instanceof Patient) {
                //we don't want to move patient resources, so do nothing and let the delete happen

            } else {
                //for all other resources, re-map the IDs and save to the DB
                try {
                    IdHelper.applyExternalReferenceMappings(fhirAmended, idMappings, false);
                    storageService.exchangeBatchUpdate(exchangeId, majorBatchId, fhirAmended, true);

                } catch (Exception ex) {
                    throw new Exception("Failed to save amended " + minorPatientResource.getResourceType() + " which originally had ID " + fhirOriginal.getId() + " and now has " + fhirAmended.getId(), ex);
                }
            }

            //finally delete the resource from the old patient - do the delete AFTER, so any failure on the insert happens before we do the delete
            storageService.exchangeBatchDelete(exchangeId, minorBatchId, fhirOriginal);

            LOG.debug("Moved " + fhirOriginal.getResourceType() + " " + fhirOriginal.getId() + " -> " + fhirAmended.getId());
        }

        //save these resource mappings for the future
        ResourceMergeMapHelper.saveResourceMergeMapping(serviceId, idMappings);
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

    private static String createResourceReferenceValue(ResourceWrapper resourceByPatient) {
        UUID resourceId = resourceByPatient.getResourceId();
        String resourceTypeStr = resourceByPatient.getResourceType();
        ResourceType resourceType = ResourceType.valueOf(resourceTypeStr);
        return ReferenceHelper.createResourceReference(resourceType, resourceId.toString());
    }

    private UUID findOrCreateBatchId(UUID exchangeId, List<UUID> batchIds, String patientId) throws Exception {

        //look for a batch ID that already exists for this exchange
        ExchangeBatchDalI exchangeBatchRepository = DalProvider.factoryExchangeBatchDal();
        List<ExchangeBatch> batches = exchangeBatchRepository.retrieveForExchangeId(exchangeId);
        for (ExchangeBatch batch: batches) {
            UUID batchPatientUuid = batch.getEdsPatientId();
            if (batchPatientUuid != null
                    && batchPatientUuid.toString().equals(patientId)) {
                return batch.getBatchId();
            }
        }

        //if we've not got an existing batch for this exchange and patient, then generate a new one
        UUID edsPatientId = UUID.fromString(patientId);
        ExchangeBatch exchangeBatch = FhirResourceFiler.createExchangeBatch(exchangeId, edsPatientId);
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
                if (event.getSystem().equals(FhirCodeUri.CODE_SYSTEM_HL7V2_MESSAGE_TYPE)) {
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

        GenericBuilder[] builders = new GenericBuilder[adminResources.size()];
        for (int i=0; i<adminResources.size(); i++) {
            Resource resource = adminResources.get(i);
            GenericBuilder builder = new GenericBuilder(resource);
            builders[i] = builder;
        }

        fhirResourceFiler.saveAdminResource(null, false, builders);
    }

    /*private void savePatientResources(FhirResourceFiler fhirResourceFiler, Bundle bundle) throws Exception {
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
    }*/

    private void savePatientResources(FhirResourceFiler fhirResourceFiler, Bundle bundle) throws Exception {
        List<Resource> patientResources = bundle
                .getEntry()
                .stream()
                .map(t -> t.getResource())
                .filter(t -> FhirResourceFiler.isPatientResource(t))
                .collect(Collectors.toList());

        /*//need to find the Patient resource so we can find it's ID
        Patient patient = patientResources
                .stream()
                .filter(t -> t.getResourceType() == ResourceType.Patient)
                .map(t -> (Patient)t)
                .collect(StreamExtension.singleCollector());
        String patientId = patient.getId();*/

        for (Resource resource: patientResources) {

            //if the resource is an Encounter, then it may be an UPDATE to an existing one, so we need to make
            //sure that we don't lose any data in the old instance by just overwriting it
            if (resource instanceof Encounter) {
                Encounter oldEncounter = (Encounter)resourceRepository.getCurrentVersionAsResource(fhirResourceFiler.getServiceId(), resource.getResourceType(), resource.getId());
                resource = updateEncounter(oldEncounter, (Encounter)resource);
            }

            GenericBuilder builder = new GenericBuilder(resource);

            //and save the resource
            fhirResourceFiler.savePatientResource(null, false, builder);
        }
    }

    public static Resource updateEncounter(Encounter oldEncounter, Encounter newEncounter) throws Exception {

        if (oldEncounter == null) {
            return newEncounter;
        }

        //field got from http://hl7.org/fhir/DSTU2/encounter.html
        updateEncounterIdentifiers(oldEncounter, newEncounter);
        updateEncounterStatus(oldEncounter, newEncounter);
        updateEncounterStatusHistory(oldEncounter, newEncounter);
        updateEncounterClass(oldEncounter, newEncounter);
        updateEncounterType(oldEncounter, newEncounter);
        updateEncounterPriority(oldEncounter, newEncounter);
        updateEncounterPatient(oldEncounter, newEncounter);
        updateEncounterEpisode(oldEncounter, newEncounter);
        updateEncounterIncomingReferral(oldEncounter, newEncounter);
        updateEncounterParticipant(oldEncounter, newEncounter);
        updateEncounterAppointment(oldEncounter, newEncounter);
        updateEncounterPeriod(oldEncounter, newEncounter);
        updateEncounterLength(oldEncounter, newEncounter);
        updateEncounterReason(oldEncounter, newEncounter);
        updateEncounterIndication(oldEncounter, newEncounter);
        updateEncounterHospitalisation(oldEncounter, newEncounter);
        updateEncounterLocation(oldEncounter, newEncounter);
        updateEncounterServiceProvider(oldEncounter, newEncounter);
        updateEncounterPartOf(oldEncounter, newEncounter);
        updateExtensions(oldEncounter, newEncounter);

        return oldEncounter;
    }

    private static void updateExtensions(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasExtension()) {
            return;
        }

        for (Extension newExtension: newEncounter.getExtension()) {
            String newUrl = newExtension.getUrl();

            Extension oldExtension = ExtensionConverter.findExtension(oldEncounter, newUrl);
            if (oldExtension == null) {
                newExtension = newExtension.copy();
                oldEncounter.addExtension(newExtension);

            } else {
                Type newValue = newExtension.getValue();
                newValue = newValue.copy();
                oldExtension.setValue(newValue);
            }
        }
    }

    private static void updateEncounterPartOf(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasPartOf()) {
            Reference ref = newEncounter.getPartOf();
            ref = ref.copy();
            oldEncounter.setPartOf(ref);
        }
    }

    private static void updateEncounterServiceProvider(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasServiceProvider()) {
            Reference ref = newEncounter.getServiceProvider();
            ref = ref.copy();
            oldEncounter.setServiceProvider(ref);
        }
    }

    private static void updateEncounterLocation(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasLocation()) {
            return;
        }

        for (Encounter.EncounterLocationComponent newLocation: newEncounter.getLocation()) {

            //find any locations on the old encounter that match the same location reference
            List<Encounter.EncounterLocationComponent> oldLocationsForSamePlace = new ArrayList<>();

            if (oldEncounter.hasLocation()) {
                for (Encounter.EncounterLocationComponent oldLocation: oldEncounter.getLocation()) {
                    if (ReferenceHelper.equals(oldLocation.getLocation(), newLocation.getLocation())) {
                        oldLocationsForSamePlace.add(oldLocation);
                    }
                }
            }

            boolean addNewLocation;

            if (oldLocationsForSamePlace.isEmpty()) {
                //if this is the first time we've heard of this location, just add it
                addNewLocation = true;

            } else {

                if (!newLocation.hasPeriod()
                        && !newLocation.hasStatus()) {
                    //if the new participant doesn't have a status or period, there's no new info, so don't add it
                    addNewLocation = false;

                } else {
                    addNewLocation = true;

                    for (Encounter.EncounterLocationComponent oldLocation: oldLocationsForSamePlace) {

                         if (newLocation.hasPeriod()) {
                            Period newPeriod = newLocation.getPeriod();

                            Period oldPeriod = null;
                            if (oldLocation.hasPeriod()) {
                                oldPeriod = oldLocation.getPeriod();
                            }

                            //see if we can merge the old and new periods
                            Period mergedPeriod = compareAndMergePeriods(oldPeriod, newPeriod);
                            if (mergedPeriod != null) {
                                oldLocation.setPeriod(mergedPeriod);
                                addNewLocation = false;

                            } else {
                                //if we couldn't merge the period, then skip this location
                                //and don't try to set the new status on this old location
                                continue;
                            }
                        }

                        if (newLocation.hasStatus()) {
                            Encounter.EncounterLocationStatus newStatus = newLocation.getStatus();
                            oldLocation.setStatus(newStatus);
                            addNewLocation = false;
                        }

                        if (!addNewLocation ) {
                            break;
                        }
                    }
                }
            }

            if (addNewLocation) {
                newLocation = newLocation.copy();
                oldEncounter.getLocation().add(newLocation);
            }
        }
    }

    private static void updateEncounterHospitalisation(Encounter oldEncounter, Encounter newEncounter) {

        if (newEncounter.hasHospitalization()) {
            Encounter.EncounterHospitalizationComponent h = newEncounter.getHospitalization();
            h = h.copy();
            oldEncounter.setHospitalization(h);
        }
    }

    private static void updateEncounterIndication(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasIndication()) {
            for (Reference newIndication: newEncounter.getIndication()) {

                if (!oldEncounter.hasIndication()
                    || !ReferenceHelper.contains(oldEncounter.getIndication(), newIndication)) {
                    newIndication = newIndication.copy();
                    oldEncounter.addIndication(newIndication);
                }
            }
        }
    }

    private static void updateEncounterReason(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasReason()) {
            return;
        }

        for (CodeableConcept newConcept: newEncounter.getReason()) {

            boolean add = true;
            if (oldEncounter.hasReason()) {
                for (CodeableConcept oldConcept: oldEncounter.getReason()) {
                    if (oldConcept.equalsDeep(newConcept)) {
                        add = false;
                        break;
                    }
                }
            }

            if (add) {
                newConcept = newConcept.copy();
                oldEncounter.addReason(newConcept);
            }
        }
    }

    private static void updateEncounterLength(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasLength()) {
            Duration d = newEncounter.getLength();
            d = d.copy();
            oldEncounter.setLength(d);
        }
    }

    private static void updateEncounterPeriod(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasPeriod()) {
            return;
        }

        Period oldPeriod = null;
        if (oldEncounter.hasPeriod()) {
            oldPeriod = oldEncounter.getPeriod();
        } else {
            oldPeriod = new Period();
            oldEncounter.setPeriod(oldPeriod);
        }

        Period newPeriod = newEncounter.getPeriod();

        if (newPeriod.hasStart()) {
            Date d = newPeriod.getStart();
            oldPeriod.setStart(d);
        }

        if (newPeriod.hasEnd()) {
            Date d = newPeriod.getEnd();
            oldPeriod.setEnd(d);
        }
    }

    private static void updateEncounterAppointment(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasAppointment()) {
            return;
        }

        Reference ref = newEncounter.getAppointment();
        ref = ref.copy();
        oldEncounter.setAppointment(ref);
    }

    private static void updateEncounterParticipant(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasParticipant()) {
            return;
        }

        for (Encounter.EncounterParticipantComponent newParticipant: newEncounter.getParticipant()) {

            //find all old participants for the same person and type
            List<Encounter.EncounterParticipantComponent> oldParticipantsSamePersonAndType = new ArrayList<>();

            for (Encounter.EncounterParticipantComponent oldParticipant : oldEncounter.getParticipant()) {

                if (oldParticipant.hasIndividual() != newParticipant.hasIndividual()) {
                    continue;
                }

                if (oldParticipant.hasIndividual()
                        && !ReferenceHelper.equals(oldParticipant.getIndividual(), newParticipant.getIndividual())) {
                    continue;
                }

                if (oldParticipant.hasType() != newParticipant.hasType()) {
                    continue;
                }

                if (oldParticipant.hasType()) {
                    int oldCount = oldParticipant.getType().size();
                    int newCount = newParticipant.getType().size();
                    if (oldCount != newCount) {
                        continue;
                    }

                    boolean typesMatch = true;
                    for (CodeableConcept oldType: oldParticipant.getType()) {
                        boolean foundType = false;
                        for (CodeableConcept newType: newParticipant.getType()) {
                            if (oldType.equalsDeep(newType)) {
                                foundType = true;
                                break;
                            }
                        }
                        if (!foundType) {
                            typesMatch = false;
                            break;
                        }
                    }
                    if (!typesMatch) {
                        continue;
                    }
                }

                //if we finally make it here, this old participant matches the new one on person and type
                oldParticipantsSamePersonAndType.add(oldParticipant);
            }

            boolean addNewParticipant;

            if (oldParticipantsSamePersonAndType.isEmpty()) {
                //if there are no old participants with the same type and person, then just add the new one
                addNewParticipant = true;

            } else {

                if (!newParticipant.hasPeriod()) {
                    //if the new participant doesn't have a period, there's no new info, so don't add it
                    addNewParticipant = false;

                } else {
                    addNewParticipant = true;
                    Period newPeriod = newParticipant.getPeriod();

                    for (Encounter.EncounterParticipantComponent oldParticipant : oldParticipantsSamePersonAndType) {

                        Period oldPeriod = null;
                        if (oldParticipant.hasPeriod()) {
                            oldPeriod = oldParticipant.getPeriod();
                        }

                        //see if we can merge the old and new periods
                        Period mergedPeriod = compareAndMergePeriods(oldPeriod, newPeriod);
                        if (mergedPeriod != null) {
                            oldParticipant.setPeriod(mergedPeriod);
                            addNewParticipant = false;
                            break;
                        }
                    }
                }
            }

            if (addNewParticipant) {
                newParticipant = newParticipant.copy();
                oldEncounter.getParticipant().add(newParticipant);
            }
        }
    }

    /**
     * compares old and new periods, returning a new merged one if they can be merged, null if they can't
     */
    private static Period compareAndMergePeriods(Period oldPeriod, Period newPeriod) {

        //if the nwe period is empty, just return the old period
        if (newPeriod == null
            || (!newPeriod.hasStart() && !newPeriod.hasEnd())) {
            return oldPeriod.copy();
        }

        //if the old period is empty, just return the new period
        if (oldPeriod == null
            || (!oldPeriod.hasStart() && !oldPeriod.hasEnd())) {
            return newPeriod.copy();
        }

        //if here, then we have start, end or both on our new period and start, end of both on our old period too
        Date newStart = null;
        Date newEnd = null;
        if (newPeriod.hasStart()) {
            newStart = newPeriod.getStart();
        }
        if (newPeriod.hasEnd()) {
            newEnd = newPeriod.getEnd();
        }

        Date oldStart = null;
        Date oldEnd = null;
        if (oldPeriod.hasStart()) {
            oldStart = oldPeriod.getStart();
        }
        if (oldPeriod.hasEnd()) {
            oldEnd = oldPeriod.getEnd();
        }

        //if we have old and new starts, and they don't match, then skip this participant
        if (newStart != null
                && oldStart != null
                && !oldStart.equals(newStart)) {
            return null;
        }

        //if we have a new start date, make sure it's not after the existing end date, if we have one
        if (oldEnd != null
                && newStart != null
                && newStart.after(oldEnd)) {
            return null;
        }

        //if we have old and new end dates, and they don't match, then skip this participant
        if (newEnd != null
                && oldEnd != null
                && !oldEnd.equals(newEnd)) {
            return null;
        }

        //if we have a new end date, make sure it's not before the start date, if we have one
        if (oldStart != null
                && newEnd != null
                && newEnd.before(oldStart)) {
            return null;
        }

        //if we make it here, set the new dates in the old period
        Period merged = oldPeriod.copy();

        if (newStart != null) {
            merged.setStart(newStart);
        }
        if (newEnd != null) {
            merged.setEnd(newEnd);
        }

        return merged;
    }

    private static void updateEncounterIncomingReferral(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasIncomingReferral()) {
            return;
        }

        for (Reference newReference: newEncounter.getIncomingReferral()) {

            if (!oldEncounter.hasIncomingReferral()
                || !ReferenceHelper.contains(oldEncounter.getIncomingReferral(), newReference)) {
                newReference = newReference.copy();
                oldEncounter.addIncomingReferral(newReference);
            }
        }
    }

    private static void updateEncounterEpisode(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasEpisodeOfCare()) {
            return;
        }

        for (Reference newReference: newEncounter.getEpisodeOfCare()) {

            if (!oldEncounter.hasEpisodeOfCare()
                || !ReferenceHelper.contains(oldEncounter.getEpisodeOfCare(), newReference)) {
                newReference = newReference.copy();
                oldEncounter.addEpisodeOfCare(newReference);
            }
        }
    }

    private static void updateEncounterPatient(Encounter oldEncounter, Encounter newEncounter) throws Exception {
        //the subject (i.e. patient) should never change, so check this and validate rather than apply changes
        if (!oldEncounter.hasPatient()) {
            throw new TransformException("No patient on OLD " + oldEncounter.getResourceType() + " " + oldEncounter.getId());
        }
        if (!newEncounter.hasPatient()) {
            throw new TransformException("No patient on NEW " + newEncounter.getResourceType() + " " + newEncounter.getId());
        }

        //this validation doesn't work now that we persist merge mappings and automatically apply them
        /*String oldPatientRef = oldEncounter.getPatient().getReference();
        String newPatientRef = newEncounter.getPatient().getReference();
        if (!oldPatientRef.equals(newPatientRef)) {
            throw new TransformException("Old " + oldEncounter.getResourceType() + " " + oldEncounter.getId() + " links to " + oldPatientRef + " but new version to " + newPatientRef);
        }*/
    }

    private static void updateEncounterPriority(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasPriority()) {
            CodeableConcept codeableConcept = newEncounter.getPriority();
            codeableConcept = codeableConcept.copy(); //not strictly necessary, but can't hurt
            oldEncounter.setPriority(codeableConcept);
        }
    }

    private static void updateEncounterType(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasType()) {
            return;
        }

        for (CodeableConcept newConcept: newEncounter.getType()) {

            boolean add = true;
            if (oldEncounter.hasType()) {
                for (CodeableConcept oldConcept: oldEncounter.getType()) {
                    if (oldConcept.equalsDeep(newConcept)) {
                        add = false;
                        break;
                    }
                }
            }

            if (add) {
                newConcept = newConcept.copy();
                oldEncounter.addType(newConcept);
            }
        }
    }

    private static void updateEncounterClass(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasClass_()) {
            return;
        }

        Encounter.EncounterClass cls = newEncounter.getClass_();
        oldEncounter.setClass_(cls);
    }

    private static void updateEncounterStatusHistory(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasStatusHistory()) {
            return;
        }

        for (Encounter.EncounterStatusHistoryComponent newStatus: newEncounter.getStatusHistory()) {

            //see if we can find a matching status element that we can update with new info (i.e. dates)
            boolean add = true;

            if (oldEncounter.hasStatusHistory()) {

                //status and period are mandatory on the status history, so we don't need to mess about checking the has... fns
                Encounter.EncounterState newState = newStatus.getStatus();
                Period newPeriod = newStatus.getPeriod();

                for (Encounter.EncounterStatusHistoryComponent oldStatus: oldEncounter.getStatusHistory()) {

                    Encounter.EncounterState oldState = oldStatus.getStatus();
                    Period oldPeriod = oldStatus.getPeriod();
                    if (oldState != newState) {
                        continue;
                    }

                    Period mergedPeriod = compareAndMergePeriods(oldPeriod, newPeriod);
                    if (mergedPeriod == null) {
                        continue;
                    }

                    //if we make it here, we can set the new merged period on the old status and skip adding it as a new status entirely
                    oldStatus.setPeriod(mergedPeriod);
                    add = false;
                }
            }

            if (add) {
                newStatus = newStatus.copy();
                oldEncounter.getStatusHistory().add(newStatus);
            }
        }
    }

    private static void updateEncounterStatus(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasStatus()) {
            return;
        }

        Encounter.EncounterState oldStatus = null;
        if (oldEncounter.hasStatus()) {
            oldStatus = oldEncounter.getStatus();
        }

        //set the new status
        Encounter.EncounterState newStatus = newEncounter.getStatus();
        oldEncounter.setStatus(newStatus);

        //if we actually changed the status, see if we need to move the old status into the status history
        if (oldStatus != null
                && oldStatus != newStatus) {

            boolean add = true;

            //see if we have a status history record with the old status in
            if (oldEncounter.hasStatusHistory()) {
                for (Encounter.EncounterStatusHistoryComponent oldStatusHistory: oldEncounter.getStatusHistory()) {
                    if (oldStatusHistory.getStatus() == oldStatus) {
                        add = false;
                        break;
                    }
                }
            }

            if (add) {
                Encounter.EncounterStatusHistoryComponent newStatusHistory = oldEncounter.addStatusHistory();
                newStatusHistory.setStatus(oldStatus);
                newStatusHistory.setPeriod(new Period()); //period is mandatory, but we don't have any dates to go in there
            }
        }
    }



    private static void updateEncounterIdentifiers(Encounter oldEncounter, Encounter newEncounter) {

        if (!newEncounter.hasIdentifier()) {
            return;
        }

        for (Identifier newIdentifier: newEncounter.getIdentifier()) {

            boolean add = true;

            if (oldEncounter.hasIdentifier()) {
                for (Identifier oldIdentifier: oldEncounter.getIdentifier()) {

                    if (oldIdentifier.hasUse() != newIdentifier.hasUse()) {
                        continue;
                    }
                    if (oldIdentifier.hasUse()
                            && oldIdentifier.getUse() != newIdentifier.getUse()) {
                        continue;
                    }
                    if (oldIdentifier.hasType() != newIdentifier.hasType()) {
                        continue;
                    }
                    if (oldIdentifier.hasType()
                            && !oldIdentifier.getType().equalsDeep(newIdentifier.getType())) {
                        continue;
                    }
                    if (oldIdentifier.hasSystem() != newIdentifier.hasSystem()) {
                        continue;
                    }
                    if (oldIdentifier.hasSystem()
                            && !oldIdentifier.getSystem().equals(newIdentifier.getSystem())) {
                        continue;
                    }
                    if (oldIdentifier.hasValue() != newIdentifier.hasValue()) {
                        continue;
                    }
                    if (oldIdentifier.hasValue()
                            && !oldIdentifier.getValue().equals(newIdentifier.getValue())) {
                        continue;
                    }
                    if (oldIdentifier.hasPeriod() != newIdentifier.hasPeriod()) {
                        continue;
                    }
                    if (oldIdentifier.hasPeriod()
                            && !oldIdentifier.getPeriod().equalsDeep(newIdentifier.getPeriod())) {
                        continue;
                    }
                    if (oldIdentifier.hasAssigner() != newIdentifier.hasAssigner()) {
                        continue;
                    }
                    if (oldIdentifier.hasAssigner()
                            && !oldIdentifier.getAssigner().equalsDeep(newIdentifier.getAssigner())) {
                        continue;
                    }

                    //if we make it here, the old identifier matches the new one, so we don't need to add it
                    add = false;
                    break;
                }
            }

            if (add) {
                newIdentifier = newIdentifier.copy();
                oldEncounter.addIdentifier(newIdentifier);
            }
        }
    }
}
