package org.endeavourhealth.transform.fhirhl7v2;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.ResourceMergeMapHelper;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.endeavourhealth.transform.fhirhl7v2.transforms.EncounterTransformer;
import org.endeavourhealth.transform.fhirhl7v2.transforms.PatientTransformer;
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


    public void file(String exchangeBody, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        Resource bundleResource = FhirResourceHelper.deserialiseResouce(exchangeBody);

        if (bundleResource.getResourceType() != ResourceType.Bundle) {
            throw new Exception("Resource is not a bundle");
        }
        Bundle bundle = (Bundle)bundleResource;

        Date adtDate = findAdtMessageDate(bundle);

        saveAdminResources(fhirResourceFiler, bundle);
        savePatientResources(fhirResourceFiler, bundle, adtDate);


        //ensure everything is saved before we try and do any of the merging stuff
        fhirResourceFiler.waitUntilEverythingIsSaved();


        //need to handle the parameters object being null since we're not receiving it yet in AIMES
        try {
            Parameters parameters = findParameters(bundle);

            //see if there's any special work we need to do for merging/moving
            String adtMessageType = findAdtMessageType(bundle);
            LOG.debug("Received ADT message type " + adtMessageType + " for exchange " + fhirResourceFiler.getExchangeId());
            if (adtMessageType.equals(ADT_A34)) {
                LOG.debug("Processing A34");
                performA34PatientMerge(bundle, fhirResourceFiler);

            } else if (adtMessageType.equals(ADT_A35)) {
                LOG.debug("Processing A35");
                performA35EpisodeMerge(bundle, fhirResourceFiler);

            } else if (adtMessageType.equals(ADT_A44)) {
                LOG.debug("Processing A44");
                performA44EpisodeMove(bundle, fhirResourceFiler);

            } else {
                //nothing special
            }

        } catch (TransformException ex) {
            //if we get an exception, there are no parameter, so nothing special to do
        }
    }

    /**
     * A44 messages move an entire episode and all its dependant data from one patient to another
     */
    private void performA44EpisodeMove(Bundle bundle, FhirResourceFiler fhirResourceFiler) throws Exception {
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

        List<ResourceWrapper> minorPatientResources = resourceRepository.getResourcesByPatient(fhirResourceFiler.getServiceId(), UUID.fromString(minorPatientId));

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

            fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(fhirAmended));

            //finally delete the resource from the old patient
            fhirResourceFiler.deletePatientResource(null, false, new GenericBuilder(fhirOriginal));

            LOG.debug("Moved " + resourceType + " " + fhirOriginal.getId() + " -> " + fhirAmended.getId());
        }

        //save these resource mappings for the future
        ResourceMergeMapHelper.saveResourceMergeMapping(fhirResourceFiler.getServiceId(), originalIdMappings);
    }

    /**
     * A35 messages merge the contents of one episode (minor) into another one (major) for the same patient
     */
    private void performA35EpisodeMerge(Bundle bundle, FhirResourceFiler fhirResourceFiler) throws Exception {
        Parameters parameters = findParameters(bundle);

        String patientId = findParameterValue(parameters, "PatientUuid");

        String minorEpisodeOfCareId = findParameterValue(parameters, "MinorEpisodeOfCareUuid");
        String majorEpisodeOfCareId = findParameterValue(parameters, "MajorEpisodeOfCareUuid");

        LOG.debug("Doing A35 merge for patient " + patientId + " and minor episode " + minorEpisodeOfCareId + " to major episode " + majorEpisodeOfCareId);

        String majorEpisodeReference = ReferenceHelper.createResourceReference(ResourceType.EpisodeOfCare, majorEpisodeOfCareId);
        String minorEpisodeReference = ReferenceHelper.createResourceReference(ResourceType.EpisodeOfCare, minorEpisodeOfCareId);

        List<ResourceWrapper> patientResources = resourceRepository.getResourcesByPatient(fhirResourceFiler.getServiceId(), UUID.fromString(patientId));

        for (ResourceWrapper patientResource: patientResources) {

            ResourceType resourceType = ResourceType.valueOf(patientResource.getResourceType());
            String json = patientResource.getResourceData();

            if (resourceType == ResourceType.Patient) {
                //don't do anything to the patient

            } else if (resourceType == ResourceType.EpisodeOfCare) {
                //we want to delete the old episode of care
                EpisodeOfCare episodeOfCare = (EpisodeOfCare)ParserPool.getInstance().parse(json);
                if (episodeOfCare.getId().equals(minorEpisodeOfCareId)) {
                    fhirResourceFiler.deletePatientResource(null, false, new GenericBuilder(episodeOfCare));
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
                    fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(encounter));
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
        ResourceMergeMapHelper.saveResourceMergeMapping(fhirResourceFiler.getServiceId(), majorEpisodeReference, minorEpisodeReference);
    }

    /**
     * A34 messages merge all content from one patient (minor patient) to another (the major patient)
     */
    private void performA34PatientMerge(Bundle bundle, FhirResourceFiler fhirResourceFiler) throws Exception {

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

        List<ResourceWrapper> minorPatientResources = resourceRepository.getResourcesByPatient(fhirResourceFiler.getServiceId(), UUID.fromString(minorPatientId));

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
                    fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(fhirAmended));

                } catch (Exception ex) {
                    throw new Exception("Failed to save amended " + minorPatientResource.getResourceType() + " which originally had ID " + fhirOriginal.getId() + " and now has " + fhirAmended.getId(), ex);
                }
            }

            //finally delete the resource from the old patient - do the delete AFTER, so any failure on the insert happens before we do the delete
            fhirResourceFiler.deletePatientResource(null, false, new GenericBuilder(fhirOriginal));

            LOG.debug("Moved " + fhirOriginal.getResourceType() + " " + fhirOriginal.getId() + " -> " + fhirAmended.getId());
        }

        //save these resource mappings for the future
        ResourceMergeMapHelper.saveResourceMergeMapping(fhirResourceFiler.getServiceId(), idMappings);
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


    private static Parameters findParameters(Bundle bundle) throws Exception {
        for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
            Resource resource = entry.getResource();
            if (resource.getResourceType() == ResourceType.Parameters) {
                return (Parameters)resource;
            }
        }

        throw new TransformException("Failed to find Parameters resource in Bundle");
    }

    private static Date findAdtMessageDate(Bundle bundle) throws Exception {
        for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
            Resource resource = entry.getResource();
            if (resource.getResourceType() == ResourceType.MessageHeader) {
                MessageHeader header = (MessageHeader)resource;
                if (header.hasTimestamp()) {
                    return header.getTimestamp();
                }
            }
        }

        throw new TransformException("Failed to find MessageHeader resource or timestamp in Bundle");
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

        //some resources are shared between the HL7 feed and Data Warehouse feed (e.g. Organizations)
        //so we want to avoid overwriting content from the DW feed, which is richer than the HL7 feed.
        //Do this by retrieving the existing resource and checking if the systemId matches our own
        for (Resource resource: adminResources) {

            if (!isNewOrCurrentVersionSameSystem(resource, fhirResourceFiler)) {
                LOG.debug("Not saving " + resource.getResourceType() + " " + resource.getId() + " as been taken over by another system");
                continue;
            }

            LOG.debug("Saving " + resource.getResourceType() + " " + resource.getId());
            GenericBuilder builder = new GenericBuilder(resource);
            fhirResourceFiler.saveAdminResource(null, false, builder);
        }
    }

    private void savePatientResources(FhirResourceFiler fhirResourceFiler, Bundle bundle, Date adtDate) throws Exception {
        List<Resource> patientResources = bundle
                .getEntry()
                .stream()
                .map(t -> t.getResource())
                .filter(t -> FhirResourceFiler.isPatientResource(t))
                .collect(Collectors.toList());

        //we get three types of patient resources from the HL7 feed. Only the Patient resource is shared between
        //the HL7 feed and Data Warehouse feed, whereas EpisodeOfCare and Encounter are not. We need to handle
        //each type differently so these two feeds can co-exist.
        for (Resource resource: patientResources) {

            if (resource instanceof Patient) {

                //updated to merge ADT changes into the existing Patient resource, similar to how we handle Encounter,
                //so that both the ADT and DW feeds can co-exist
                resource = PatientTransformer.updatePatient((Patient)resource, fhirResourceFiler, adtDate, bundle);
                fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(resource));

                //Patient resources ARE shared between HL7 and DW feeds, so check to see if the systemId of the
                //current version matches that of the HL7 feed. If not, it means the DW feed has taken over the patient
                //in which case don't apply any updates to it. This means that changes to the patient (e.g. new address)
                //won't be applied to the resource until we get the next DW update through, but merging the changes into the
                //resource without breaking it seems very difficult.
                //NOTE: it could be possible to merge the new data into the existing FHIR patient, but
                //there's a lot of weirdness in the HL7 data (e.g. email addresses showing as proper addresses)
                //that would need to be investigated and coded for.
                /*if (isNewOrCurrentVersionSameSystem(resource, fhirResourceFiler)) {
                    tidyNhsNumbers((Patient)resource);

                    LOG.debug("Saving " + resource.getResourceType() + " " + resource.getId());
                    fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(resource));

                } else {
                    LOG.debug("Not saving " + resource.getResourceType() + " " + resource.getId() + " as been taken over by DW feed");
                }*/

            } else if (resource instanceof EpisodeOfCare) {
                //EpisodeOfCare resources ARE shared between Hl7 and DW feeds where possible, and the DW feed will delete
                //non-shared Episodes created by the HL7 feed since it can create better resources from the richer data.
                //So only save our EpisodeOfCare resource if it's brand new or hasn't been deleted by the DW feed
                if (!hasBeenDeletedByDataWarehouseFeed(resource, fhirResourceFiler)) {

                    if (isNewOrCurrentVersionSameSystem(resource, fhirResourceFiler)) {
                        LOG.debug("Saving " + resource.getResourceType() + " " + resource.getId());
                        fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(resource));

                    } else {
                        LOG.debug("Not saving " + resource.getResourceType() + " " + resource.getId() + " as has been taken over by DW feed");
                    }

                } else {
                    LOG.debug("Not saving " + resource.getResourceType() + " " + resource.getId() + " as has been deleted by DW feed");
                }

            } else if (resource instanceof Encounter) {
                //Encounter resources ARE shared between Hl7 and DW feeds where possible, and the DW feed will delete
                //non-shared Encounters created by the HL7 feed since it can create better resources from the richer data.
                //So we need to NOT re-create deleted Encounters, and to only update certain fields if the DW feed has taken over the Encounter

                if (!hasBeenDeletedByDataWarehouseFeed(resource, fhirResourceFiler)) {
                    Encounter oldEncounter = (Encounter)resourceRepository.getCurrentVersionAsResource(fhirResourceFiler.getServiceId(), resource.getResourceType(), resource.getId());

                    if (isNewOrCurrentVersionSameSystem(resource, fhirResourceFiler)) {
                        //fully merge the new HL7 encounter into the existing HL7 one
                        LOG.debug("Saving " + resource.getResourceType() + " " + resource.getId() + " after merging into HL7 Encounter");
                        resource = EncounterTransformer.updateHl7Encounter(oldEncounter, (Encounter)resource);

                    } else {
                        //do a limited merge of the HL7 encounter into the DW one
                        LOG.debug("Saving " + resource.getResourceType() + " " + resource.getId() + " after merging into DW Encounter");
                        resource = EncounterTransformer.updateDwEncounter(oldEncounter, (Encounter)resource);
                    }

                    fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(resource));

                } else {
                    LOG.debug("Not saving " + resource.getResourceType() + " " + resource.getId() + " as has been deleted by DW feed");
                }

            } else {
                throw new TransformException("Unsupported patient resource type in HL7 feed: " + resource.getResourceType());
            }
        }
    }


    private boolean hasBeenDeletedByDataWarehouseFeed(Resource resource, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID serviceId = fhirResourceFiler.getServiceId();
        UUID systemId = fhirResourceFiler.getSystemId();

        String resourceType = resource.getResourceType().toString();
        UUID resourceId = UUID.fromString(resource.getId());
        List<ResourceWrapper> history = resourceRepository.getResourceHistory(serviceId, resourceType, resourceId);

        //if we've never heard of this resource, it hasn't been deleted by the DW feed
        if (history.isEmpty()) {
            return false;
        }

        //most recent is first
        ResourceWrapper latestHistory = history.get(0);

        //if the latest history isn't deleted, then it wasn't deleted by the DW feed
        if (!latestHistory.isDeleted()) {
            return false;
        }

        //if the delete was by the HL7 feed, then it wasn't deleted by the DW feed
        UUID latestSystemId = latestHistory.getSystemId();
        if (latestSystemId.equals(systemId)) {
            return false;
        }

        return true;
    }

    private boolean isNewOrCurrentVersionSameSystem(Resource resource, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID serviceId = fhirResourceFiler.getServiceId();
        UUID systemId = fhirResourceFiler.getSystemId();

        String resourceType = resource.getResourceType().toString();
        UUID resourceId = UUID.fromString(resource.getId());
        ResourceWrapper wrapper = resourceRepository.getCurrentVersion(serviceId, resourceType, resourceId);

        if (wrapper != null) {
            UUID currentSystemId = wrapper.getSystemId();
            if (!currentSystemId.equals(systemId)) {
                return false;
            }
        }

        return true;
    }

}
