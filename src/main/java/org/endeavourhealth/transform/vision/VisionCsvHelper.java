package org.endeavourhealth.transform.vision;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.EmisTransformDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.EmisCsvCodeMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class VisionCsvHelper {
    private static final Logger LOG = LoggerFactory.getLogger(VisionCsvHelper.class);

    private static final String CODEABLE_CONCEPT = "CodeableConcept";
    private static final String ID_DELIMITER = ":";
    private static final String CONTAINED_LIST_ID = "Items";

    private static final ParserPool PARSER_POOL = new ParserPool();

    private String dataSharingAgreementGuid = null;

    //metadata, not relating to patients
    private Map<Long, CodeableConcept> clinicalCodes = new ConcurrentHashMap<>();
    private Map<Long, ClinicalCodeType> clinicalCodeTypes = new ConcurrentHashMap<>();
    private Map<Long, CodeableConcept> medication = new ConcurrentHashMap<>();
    private EmisTransformDalI mappingRepository = DalProvider.factoryEmisTransformDal();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    //some resources are referred to by others, so we cache them here for when we need them
    private Map<String, String> problemMap = new HashMap<>(); //changed to cache the conditions as JSON strings
    private Map<String, ReferralRequest> referralMap = new HashMap<>();
    private Map<String, List<String>> observationChildMap = new HashMap<>();
    private Map<String, List<String>> problemChildMap = new HashMap<>();
    private Map<String, List<String>> consultationChildMap = new HashMap<>();
    private Map<String, DateType> drugRecordLastIssueDateMap = new HashMap<>();
    private Map<String, DateType> drugRecordFirstIssueDateMap = new HashMap<>();
    private Map<String, List<Observation.ObservationComponentComponent>> bpComponentMap = new HashMap<>();
    private Map<String, SessionPractitioners> sessionPractitionerMap = new HashMap<>();
    private Map<String, List<String>> organisationLocationMap = new HashMap<>();
    private Map<String, DateAndCode> ethnicityMap = new HashMap<>();
    private Map<String, DateAndCode> maritalStatusMap = new HashMap<>();
    private Map<String, String> problemReadCodes = new HashMap<>();

    public VisionCsvHelper() {
    }

    /**
     * to ensure globally unique IDs for all resources, a new ID is created
     * from the patientGuid and sourceGuid (e.g. observationGuid)
     */
    public static String createUniqueId(String patientGuid, String sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid;
        } else {
            return patientGuid + ID_DELIMITER + sourceGuid;
        }
    }
    private static String getPatientGuidFromUniqueId(String uniqueId) {
        String[] toks = uniqueId.split(ID_DELIMITER);
        if (toks.length == 1
                || toks.length == 2) {
            return toks[0];
        } else {
            throw new IllegalArgumentException("Invalid unique ID string [" + uniqueId + "] - expect one or two tokens delimited with " + ID_DELIMITER);
        }
    }

    public static void setUniqueId(Resource resource, String patientGuid, String sourceGuid) {
        resource.setId(createUniqueId(patientGuid, sourceGuid));
    }

    private void retrieveClinicalCode(Long codeId) throws Exception {
        EmisCsvCodeMap mapping = mappingRepository.getMostRecentCode(dataSharingAgreementGuid, false, codeId);
        if (mapping == null) {
            //until we move to AWS, and Emis actually fix this, substitute a dummy codeable concept
            LOG.error("Failed to find clincal codeable concept for code ID " + codeId);

            Coding coding = new Coding();
            coding.setSystem(FhirUri.CODE_SYSTEM_READ2);
            coding.setCode("?????");
            coding.setDisplay("Unknown code");

            CodeableConcept codeableConcept = new CodeableConcept();
            codeableConcept.setText("Missing Clinical Code (Emis ECR 9953529)");
            codeableConcept.addCoding(coding);

            clinicalCodes.put(codeId, codeableConcept);

            clinicalCodeTypes.put(codeId, ClinicalCodeType.Conditions_Operations_Procedures);
            return;
            //throw new ClinicalCodeNotFoundException(dataSharingAgreementGuid, false, codeId);
        }

        String json = mapping.getCodeableConcept();

        CodeableConcept codeableConcept = (CodeableConcept)PARSER_POOL.parseType(json, CODEABLE_CONCEPT);
        clinicalCodes.put(codeId, codeableConcept);

        ClinicalCodeType type = ClinicalCodeType.fromValue(mapping.getCodeType());
        clinicalCodeTypes.put(codeId, type);
    }


    /**
     * admin-type resources just use the EMIS CSV GUID as their reference
     */
    public Reference createLocationReference(String locationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid);
    }
    public Reference createOrganisationReference(String organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid);
    }
    public Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }

    /**
     * patient-type resources must include the patient GUID are part of the unique ID in the reference
     * because the EMIS GUIDs for things like Obs are only unique within that patient record itself
     */
    public Reference createPatientReference(String patientGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Patient, createUniqueId(patientGuid, null));
    }

    public Reference createEncounterReference(String encounterGuid, String patientGuid) throws Exception {
        if (Strings.isNullOrEmpty(encounterGuid)) {
            throw new IllegalArgumentException("Missing Encounter ID");
        }
        return ReferenceHelper.createReference(ResourceType.Encounter, createUniqueId(patientGuid, encounterGuid));
    }
    public Reference createObservationReference(String observationGuid, String patientGuid) throws Exception {
        if (Strings.isNullOrEmpty(observationGuid)) {
            throw new IllegalArgumentException("Missing observationGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Observation, createUniqueId(patientGuid, observationGuid));
    }
    public Reference createMedicationStatementReference(String medicationStatementGuid, String patientGuid) throws Exception {
        if (Strings.isNullOrEmpty(medicationStatementGuid)) {
            throw new IllegalArgumentException("Missing MedicationStatement ID");
        }
        return ReferenceHelper.createReference(ResourceType.MedicationStatement, createUniqueId(patientGuid, medicationStatementGuid));
    }


    public void cacheReferral(String observationGuid, String patientGuid, ReferralRequest fhirReferral) {
        referralMap.put(createUniqueId(patientGuid, observationGuid), fhirReferral);
    }

    public void cacheProblem(String observationGuid, String patientGuid, Condition fhirCondition) throws Exception {
        //the Condition java objects are huge in memory, so save some by caching as a JSON string
        String conditionJson = FhirSerializationHelper.serializeResource(fhirCondition);
        problemMap.put(createUniqueId(patientGuid, observationGuid), conditionJson);
        //problemMap.put(createUniqueId(patientGuid, observationGuid), fhirCondition);
    }

    public boolean existsProblem(String observationGuid, String patientGuid) {
        return problemMap.get(createUniqueId(patientGuid, observationGuid)) != null;
    }

    public Condition findProblem(String observationGuid, String patientGuid) throws Exception {
        String conditionJson = problemMap.remove(createUniqueId(patientGuid, observationGuid));
        if (conditionJson != null) {
            return (Condition)FhirSerializationHelper.deserializeResource(conditionJson);
        } else {
            return null;
        }
        //return problemMap.remove(createUniqueId(patientGuid, observationGuid));
    }

    public List<String> getAndRemoveObservationParentRelationships(String parentObservationGuid, String patientGuid) {
        return observationChildMap.remove(createUniqueId(patientGuid, parentObservationGuid));
    }

    public boolean hasChildObservations(String parentObservationGuid, String patientGuid) {
        return observationChildMap.containsKey(createUniqueId(patientGuid, parentObservationGuid));
    }

    public void cacheObservationParentRelationship(String parentObservationGuid, String patientGuid, String observationGuid) {

        List<String> list = observationChildMap.get(createUniqueId(patientGuid, parentObservationGuid));
        if (list == null) {
            list = new ArrayList<>();
            observationChildMap.put(createUniqueId(patientGuid, parentObservationGuid), list);
        }
        list.add(ReferenceHelper.createResourceReference(ResourceType.Observation, createUniqueId(patientGuid, observationGuid)));
    }


    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(),
                fhirResourceFiler.getSystemId(),
                resourceType,
                locallyUniqueId);
        if (globallyUniqueId == null) {
            return null;
        }

        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(resourceType.toString(), globallyUniqueId);
        if (resourceHistory == null) {
            return null;
        }

        if (resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        return PARSER_POOL.parse(json);
    }

    public List<Resource> retrieveAllResourcesForPatient(String patientGuid, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID edsPatientId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(),
                fhirResourceFiler.getSystemId(),
                ResourceType.Patient,
                patientGuid);
        if (edsPatientId == null) {
            return null;
        }

        UUID serviceId = fhirResourceFiler.getServiceId();
        UUID systemId = fhirResourceFiler.getSystemId();
        List<ResourceWrapper> resourceWrappers = resourceRepository.getResourcesByPatient(serviceId, systemId, edsPatientId);

        List<Resource> ret = new ArrayList<>();

        for (ResourceWrapper resourceWrapper: resourceWrappers) {
            String json = resourceWrapper.getResourceData();
            Resource resource = PARSER_POOL.parse(json);
            ret.add(resource);
        }

        return ret;
    }

    /**
     * as the end of processing all CSV files, there may be some new observations that link
     * to past parent observations. These linkages are saved against the parent observation,
     * so we need to retrieve them off the main repository, amend them and save them
     */
    public void processRemainingObservationParentChildLinks(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (Map.Entry<String, List<String>> entry : observationChildMap.entrySet())
            updateExistingObservationWithNewChildLinks(entry.getKey(), entry.getValue(), fhirResourceFiler);
    }


    private void updateExistingObservationWithNewChildLinks(String locallyUniqueObservationId,
                                                            List<String> childResourceRelationships,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        Observation fhirObservation = (Observation)retrieveResource(locallyUniqueObservationId, ResourceType.Observation, fhirResourceFiler);
        if (fhirObservation == null) {
            //if the resource can't be found, it's because that EMIS observation record was saved as something other
            //than a FHIR Observation (example in the CSV test files is an Allergy that is linked to another Allergy)
            return;
        }

        //the EMIS patient GUID is part of the locallyUnique Id of the observation, to extract from that
        String patientGuid = getPatientGuidFromUniqueId(locallyUniqueObservationId);

        boolean changed = false;

        for (String referenceValue : childResourceRelationships) {

            Reference reference = ReferenceHelper.createReference(referenceValue);
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);

            //check if the parent observation doesn't already have our ob linked to it
            boolean alreadyLinked = false;
            for (Observation.ObservationRelatedComponent related: fhirObservation.getRelated()) {
                if (related.getType() == Observation.ObservationRelationshipType.HASMEMBER
                        && related.getTarget().equalsShallow(globallyUniqueReference)) {
                    alreadyLinked = true;
                    break;
                }
            }

            if (!alreadyLinked) {
                Observation.ObservationRelatedComponent fhirRelation = fhirObservation.addRelated();
                fhirRelation.setType(Observation.ObservationRelationshipType.HASMEMBER);
                fhirRelation.setTarget(globallyUniqueReference);

                changed = true;
            }
        }

        if (changed) {
            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
            fhirResourceFiler.savePatientResource(null, false, patientGuid, fhirObservation);
        }
    }

    public List<String> getAndRemoveProblemRelationships(String problemGuid, String patientGuid) {
        return problemChildMap.remove(createUniqueId(patientGuid, problemGuid));
    }


    public Map<String, List<String>> getProblemChildMap() {
        return problemChildMap;
    }

    public void cacheProblemRelationship(String problemObservationGuid,
                                         String patientGuid,
                                         String resourceGuid,
                                         ResourceType resourceType) {

        if (Strings.isNullOrEmpty(problemObservationGuid)) {
            return;
        }

        List<String> list = problemChildMap.get(createUniqueId(patientGuid, problemObservationGuid));
        if (list == null) {
            list = new ArrayList<>();
            problemChildMap.put(createUniqueId(patientGuid, problemObservationGuid), list);
        }

        String resourceReference = ReferenceHelper.createResourceReference(resourceType, createUniqueId(patientGuid, resourceGuid));
        list.add(resourceReference);
    }

    /**
     * called at the end of the transform, to update pre-existing Problem resources with references to new
     * clinical resources that are in those problems
     */
    public void processRemainingProblemRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Map.Entry<String, List<String>> entry : problemChildMap.entrySet())
            addRelationshipsToExistingResource(entry.getKey(), ResourceType.Condition, entry.getValue(), fhirResourceFiler, FhirExtensionUri.PROBLEM_ASSOCIATED_RESOURCE);
    }

    /**
     * adds linked references to a FHIR problem, that may or may not already have linked references
     * returns true if any change was actually made, false otherwise
     */
    public static boolean addLinkedItemsToResource(DomainResource resource, List<Reference> references, String extensionUrl) {

        //see if we already have a list in the problem
        List_ list = null;

        if (resource.hasContained()) {
            for (Resource contained: resource.getContained()) {
                if (contained.getId().equals(CONTAINED_LIST_ID)) {
                    list = (List_)contained;
                }
            }
        }

        //if the list wasn't there before, create and add it
        if (list == null) {
            list = new List_();
            list.setId(CONTAINED_LIST_ID);
            resource.getContained().add(list);
        }

        //add the extension, unless it's already there
        boolean addExtension = !ExtensionConverter.hasExtension(resource, extensionUrl);
        if (addExtension) {
            Reference listReference = ReferenceHelper.createInternalReference(CONTAINED_LIST_ID);
            resource.addExtension(ExtensionConverter.createExtension(extensionUrl, listReference));
        }

        boolean changed = false;

        for (Reference reference : references) {

            //check to see if this resource is already linked to the problem
            boolean alreadyLinked = false;
            for (List_.ListEntryComponent entry: list.getEntry()) {
                Reference entryReference = entry.getItem();
                if (entryReference.getReference().equals(reference.getReference())) {
                    alreadyLinked = true;
                    break;
                }
            }

            if (!alreadyLinked) {
                list.addEntry().setItem(reference);
                changed = true;
            }
        }

        return changed;
    }

    private void addRelationshipsToExistingResource(String locallyUniqueResourceId,
                                                   ResourceType resourceType,
                                                   List<String> childResourceRelationships,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   String extensionUrl) throws Exception {

        DomainResource fhirResource = (DomainResource)retrieveResource(locallyUniqueResourceId, resourceType, fhirResourceFiler);
        if (fhirResource == null) {
            //it's possible to create medication items that are linked to non-existent problems in Emis Web,
            //so ignore any data
            return;
        }

        //our resource is already ID mapped, so we need to manually map all the references in our list
        List<Reference> references = new ArrayList<>();

        for (String referenceValue : childResourceRelationships) {
            Reference reference = ReferenceHelper.createReference(referenceValue);
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
            references.add(globallyUniqueReference);
        }

        if (addLinkedItemsToResource(fhirResource, references, extensionUrl)) {

            String patientGuid = getPatientGuidFromUniqueId(locallyUniqueResourceId);

            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
            fhirResourceFiler.savePatientResource(null, false, patientGuid, fhirResource);
        }
    }

    public static List<Reference> findPreviousLinkedReferences(VisionCsvHelper csvHelper,
                                                               FhirResourceFiler fhirResourceFiler,
                                                               String locallyUniqueId,
                                                               ResourceType resourceType) throws Exception {

        DomainResource previousVersion = (DomainResource)csvHelper.retrieveResource(locallyUniqueId, resourceType, fhirResourceFiler);
        if (previousVersion == null) {
            //if this is the first time, then we'll have a null resource
            return null;
        }
        List<Reference> ret = new ArrayList<>();

        if (previousVersion.hasContained()) {
            for (Resource contained: previousVersion.getContained()) {
                if (contained instanceof List_) {
                    List_ list = (List_)contained;
                    for (List_.ListEntryComponent entry: list.getEntry()) {
                        Reference previousReference = entry.getItem();

                        //the reference we have has already been mapped to an EDS ID, so we need to un-map it
                        //back to the source ID, so the ID mapper can safely map it when we save the resource
                        Reference unmappedReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(previousReference);
                        if (unmappedReference != null) {
                            ret.add(unmappedReference);
                        }
                    }
                }
            }
        }

        return ret;
    }

    public void cacheDrugRecordDate(String drugRecordGuid, String patientGuid, DateTimeType dateTime) {
        String uniqueId = createUniqueId(patientGuid, drugRecordGuid);

        Date date = dateTime.getValue();

        DateType previous = drugRecordFirstIssueDateMap.get(uniqueId);
        if (previous == null
                || date.before(previous.getValue())) {
            drugRecordFirstIssueDateMap.put(uniqueId, new DateType(date));
        }

        previous = drugRecordLastIssueDateMap.get(uniqueId);
        if (previous == null
                || date.after(previous.getValue())) {
            drugRecordLastIssueDateMap.put(uniqueId, new DateType(date));
        }
    }

    public DateType getDrugRecordFirstIssueDate(String drugRecordId, String patientGuid) {
        return drugRecordFirstIssueDateMap.remove(createUniqueId(patientGuid, drugRecordId));
    }

    public DateType getDrugRecordLastIssueDate(String drugRecordId, String patientGuid) {
        return drugRecordLastIssueDateMap.remove(createUniqueId(patientGuid, drugRecordId));
    }



    public void cacheBpComponent(String parentObservationGuid, String patientGuid, Observation.ObservationComponentComponent component) {
        String key = createUniqueId(patientGuid, parentObservationGuid);
        List<Observation.ObservationComponentComponent> list = bpComponentMap.get(key);
        if (list == null) {
            list = new ArrayList<>();
            bpComponentMap.put(key, list);
        }
        list.add(component);
    }

    public List<Observation.ObservationComponentComponent> findBpComponents(String observationGuid, String patientGuid) {
        String key = createUniqueId(patientGuid, observationGuid);
        return bpComponentMap.remove(key);
    }

    public void cacheSessionPractitionerMap(String sessionGuid, String emisUserGuid, boolean isDeleted) {

        SessionPractitioners obj = sessionPractitionerMap.get(sessionGuid);
        if (obj == null) {
            obj = new SessionPractitioners();
            sessionPractitionerMap.put(sessionGuid, obj);
        }

        if (isDeleted) {
            obj.getEmisUserGuidsToDelete().add(emisUserGuid);
        } else {
            obj.getEmisUserGuidsToSave().add(emisUserGuid);
        }
    }

    public List<String> findSessionPractionersToSave(String sessionGuid, boolean creatingScheduleResource) {
        //unlike the other maps, we don't remove from this map, since we need to be able to look up
        //the staff for a session when creating Schedule resources and Appointment ones
        SessionPractitioners obj = sessionPractitionerMap.get(sessionGuid);
        if (obj == null) {
            return new ArrayList<>();
        } else {
            //since we're not removing from the map, like elsewhere, we need to set a flag to say we've
            //used this entry when creating a schedule resource, so we know to not process it at the end of the transform
            if (creatingScheduleResource) {
                obj.setProcessedSession(true);
            }

            return obj.getEmisUserGuidsToSave();
        }
    }

    /**
     * called at the end of the transform. If the sessionPractitionerMap contains any entries that haven't been processed
     * then we have changes to the staff in a previously saved FHIR Schedule, so we need to amend that Schedule
     */
    public void processRemainingSessionPractitioners(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Map.Entry<String, SessionPractitioners> entry : sessionPractitionerMap.entrySet()) {
            if (!entry.getValue().isProcessedSession()) {
                updateExistingScheduleWithNewPractitioners(entry.getKey(), entry.getValue(), fhirResourceFiler);
            }
        }
    }

    private void updateExistingScheduleWithNewPractitioners(String sessionGuid, SessionPractitioners practitioners, FhirResourceFiler fhirResourceFiler) throws Exception {

        Schedule fhirSchedule = (Schedule)retrieveResource(sessionGuid, ResourceType.Schedule, fhirResourceFiler);
        if (fhirSchedule == null) {
            //because the SessionUser file doesn't have an OrganisationGuid column, we can't split that file
            //so we will be trying to update the practitioners on sessions that don't exist. So if we get
            //an exception here, just return out
            return;
        }

        //get the references from the existing schedule, removing them as we go
        List<Reference> references = new ArrayList<>();

        if (fhirSchedule.hasActor()) {
            references.add(fhirSchedule.getActor());
            fhirSchedule.setActor(null);
        }
        if (fhirSchedule.hasExtension()) {
            List<Extension> extensions = fhirSchedule.getExtension();
            for (int i=extensions.size()-1; i>=0; i--) {
                Extension extension = extensions.get(i);
                if (extension.getUrl().equals(FhirExtensionUri.SCHEDULE_ADDITIONAL_ACTOR)) {
                    references.add((Reference)extension.getValue());
                    extensions.remove(i);
                }
            }
        }

        //add any new practitioner references
        for (String emisUserGuid: practitioners.getEmisUserGuidsToSave()) {

            //we're updating an existing FHIR resource, so need to explicitly map the EMIS user GUID to an EDS ID
            String globallyUniqueId = IdHelper.getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(),
                    fhirResourceFiler.getSystemId(),
                    ResourceType.Practitioner,
                    emisUserGuid);
            Reference referenceToAdd = ReferenceHelper.createReference(ResourceType.Practitioner, globallyUniqueId);

            if (!ReferenceHelper.contains(references, referenceToAdd)) {
                references.add(referenceToAdd);
            }
        }

        for (String emisUserGuid: practitioners.getEmisUserGuidsToDelete()) {

            //we're updating an existing FHIR resource, so need to explicitly map the EMIS user GUID to an EDS ID
            String globallyUniqueId = IdHelper.getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(),
                    fhirResourceFiler.getSystemId(),
                    ResourceType.Practitioner,
                    emisUserGuid);

            Reference referenceToDelete = ReferenceHelper.createReference(ResourceType.Practitioner, globallyUniqueId);
            ReferenceHelper.remove(references, referenceToDelete);
        }

        //save the references back into the schedule, treating the first as the main practitioner
        if (!references.isEmpty()) {

            Reference first = references.get(0);
            fhirSchedule.setActor(first);

            //add any additional references as additional actors
            for (int i = 1; i < references.size(); i++) {
                Reference additional = references.get(i);
                fhirSchedule.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.SCHEDULE_ADDITIONAL_ACTOR, additional));
            }
        }

        fhirResourceFiler.saveAdminResource(null, false, fhirSchedule);
    }

    public void cacheOrganisationLocationMap(String locationGuid, String orgGuid, boolean mainLocation) {

        List<String> orgGuids = organisationLocationMap.get(locationGuid);
        if (orgGuids == null) {
            orgGuids = new ArrayList<>();
            organisationLocationMap.put(locationGuid, orgGuids);
        }

        //if this location link is for the main location of an organisation, then insert that
        //org at the start of the list, so it's used as the managing organisation for the location
        if (mainLocation) {
            orgGuids.add(0, orgGuid);
        } else {
            orgGuids.add(orgGuid);
        }

    }

    public List<String> findOrganisationLocationMapping(String locationGuid) {
        return organisationLocationMap.remove(locationGuid);
    }

    /**
     * called at the end of the transform to handle any changes to location/organisation mappings
     * that weren't handled when we went through the Location file (i.e. changes in the OrganisationLocation file
     * with no corresponding changes in the Location file)
     */
    public void processRemainingOrganisationLocationMappings(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Map.Entry<String, List<String>> entry : organisationLocationMap.entrySet()) {

            Location fhirLocation = (Location) retrieveResource(entry.getKey(), ResourceType.Location, fhirResourceFiler);
            if (fhirLocation == null) {
                //if the location has been deleted, it doesn't matter, and the emis data integrity issues
                //mean we may have references to unknown locations
                continue;
            }

            String organisationGuid = entry.getValue().get(0);

            //the resource has already been through the ID mapping process, so we need to manually map the organisation ID
            String globallyUniqueId = IdHelper.getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(),
                    fhirResourceFiler.getSystemId(),
                    ResourceType.Organization,
                    organisationGuid);

            Reference reference = ReferenceHelper.createReference(ResourceType.Organization, globallyUniqueId);
            fhirLocation.setManagingOrganization(reference);

            fhirResourceFiler.saveAdminResource(null, false, fhirLocation);
        }
    }

    public void cacheEthnicity(String patientGuid, DateTimeType fhirDate, EthnicCategory ethnicCategory) {
        DateAndCode dc = ethnicityMap.get(createUniqueId(patientGuid, null));
        if (dc == null
            || dc.isBefore(fhirDate)) {
            ethnicityMap.put(createUniqueId(patientGuid, null), new DateAndCode(fhirDate, CodeableConceptHelper.createCodeableConcept(ethnicCategory)));
        }
    }

    public CodeableConcept findEthnicity(String patientGuid) {
        DateAndCode dc = ethnicityMap.remove(createUniqueId(patientGuid, null));
        if (dc != null) {
            return dc.getCodeableConcept();
        } else {
            return null;
        }
    }

    public void cacheMaritalStatus(String patientGuid, DateTimeType fhirDate, MaritalStatus maritalStatus) {
        DateAndCode dc = maritalStatusMap.get(createUniqueId(patientGuid, null));
        if (dc == null
                || dc.isBefore(fhirDate)) {
            maritalStatusMap.put(createUniqueId(patientGuid, null), new DateAndCode(fhirDate, CodeableConceptHelper.createCodeableConcept(maritalStatus)));
        }
    }

    public CodeableConcept findMaritalStatus(String patientGuid) {
        DateAndCode dc = maritalStatusMap.remove(createUniqueId(patientGuid, null));
        if (dc != null) {
            return dc.getCodeableConcept();
        } else {
            return null;
        }
    }

    /**
     * when the transform is complete, if there's any values left in the ethnicity and marital status maps,
     * then we need to update pre-existing patients with new data
     */
    public void processRemainingEthnicitiesAndMartialStatuses(FhirResourceFiler fhirResourceFiler) throws Exception {

        HashSet<String> patientGuids = new HashSet<>(ethnicityMap.keySet());
        patientGuids.addAll(new HashSet<>(maritalStatusMap.keySet()));

        for (String patientGuid: patientGuids) {

            DateAndCode ethnicity = ethnicityMap.get(patientGuid);
            DateAndCode maritalStatus = maritalStatusMap.get(patientGuid);

            Patient fhirPatient = (Patient) retrieveResource(createUniqueId(patientGuid, null), ResourceType.Patient, fhirResourceFiler);
            if (fhirPatient == null) {
                //if we try to update the ethnicity on a deleted patient, or one we've never received, we'll get this exception, which is fine to ignore
                continue;
            }

            if (ethnicity != null) {

                //make to use the extension if it's already present
                boolean done = false;
                for (Extension extension : fhirPatient.getExtension()) {
                    if (extension.getUrl().equals(FhirExtensionUri.PATIENT_ETHNICITY)) {
                        extension.setValue(ethnicity.getCodeableConcept());
                        done = true;
                        break;
                    }
                }
                if (!done) {
                    fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_ETHNICITY, ethnicity.getCodeableConcept()));
                }
            }

            if (maritalStatus != null) {
                fhirPatient.setMaritalStatus(maritalStatus.getCodeableConcept());
            }

            fhirResourceFiler.savePatientResource(null, false, patientGuid, fhirPatient);
        }
    }

    public Type createConditionReference(String problemGuid, String patientGuid) {
        if (Strings.isNullOrEmpty(problemGuid)) {
            throw new IllegalArgumentException("Missing problemGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Condition, createUniqueId(patientGuid, problemGuid));
    }

    public void cacheConsultationRelationship(String consultationGuid, String patientGuid, String resourceGuid, ResourceType resourceType) {
        if (Strings.isNullOrEmpty(consultationGuid)) {
            return;
        }

        List<String> list = consultationChildMap.get(createUniqueId(patientGuid, consultationGuid));
        if (list == null) {
            list = new ArrayList<>();
            consultationChildMap.put(createUniqueId(patientGuid, consultationGuid), list);
        }

        String resourceReference = ReferenceHelper.createResourceReference(resourceType, createUniqueId(patientGuid, resourceGuid));
        list.add(resourceReference);
    }

    public List<String> getAndRemoveConsultationRelationships(String consultationGuid, String patientGuid) {
        return consultationChildMap.remove(createUniqueId(patientGuid, consultationGuid));
    }

    public void processRemainingConsultationRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (Map.Entry<String, List<String>> entry : consultationChildMap.entrySet())
            addRelationshipsToExistingResource(entry.getKey(), ResourceType.Encounter, entry.getValue(), fhirResourceFiler, FhirExtensionUri.ENCOUNTER_COMPONENTS);
    }

    public Reference createEpisodeReference(String patientGuid) {
        //the episode of care just uses the patient GUID as its ID, so that's all we need to refer to it too
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, patientGuid);
    }

    public void cacheProblemObservationGuid(String patientGuid, String problemGuid, String readCode) {
        problemReadCodes.put(createUniqueId(patientGuid, problemGuid), readCode);
    }

    public boolean isProblemObservationGuid(String patientGuid, String problemGuid) {
        return problemReadCodes.containsKey(createUniqueId(patientGuid, problemGuid));
    }

    public String findProblemObservationReadCode(String patientGuid, String problemGuid, FhirResourceFiler fhirResourceFiler) throws Exception {

        String locallyUniqueId = createUniqueId(patientGuid, problemGuid);

        //if we've already cached our problem code, then just return it
        if (problemReadCodes.containsKey(locallyUniqueId)) {
            return problemReadCodes.get(locallyUniqueId);
        }

        //if we've not cached our problem code, then the problem itself isn't part of this extract,
        //so we'll need to retrieve it from the DB and cache the code
        String readCode = null;

        Condition fhirProblem = (Condition)retrieveResource(locallyUniqueId, ResourceType.Condition, fhirResourceFiler);

        //we've had cases of data referring to non-existent problems, so check for null
        if (fhirProblem != null) {
            CodeableConcept codeableConcept = fhirProblem.getCode();
            readCode = CodeableConceptHelper.findOriginalCode(codeableConcept);
        }

        problemReadCodes.put(locallyUniqueId, readCode);
        return readCode;
    }

    /**
     * when we process the IssueRecord file, we build up a map of first and last issue dates for the medication.
     * If we have any entries in the maps at the end of the transform, then we need to update the existing
     * resources on the DB with these new dates
     */
    public void processRemainingMedicationIssueDates(FhirResourceFiler fhirResourceFiler) throws Exception {

        //both maps (first and last issue dates) will have the same key set
        for (String medicationStatementLocalId: drugRecordLastIssueDateMap.keySet()) {

            DateType lastIssueDate = drugRecordLastIssueDateMap.get(medicationStatementLocalId);
            DateType firstIssueDate = drugRecordFirstIssueDateMap.get(medicationStatementLocalId);

            MedicationStatement fhirMedicationStatement = (MedicationStatement)retrieveResource(medicationStatementLocalId, ResourceType.MedicationStatement, fhirResourceFiler);
            if (fhirMedicationStatement == null) {
                //if the medication statement doesn't exist or has been deleted, then just skip it
                continue;
            }
            boolean changed = false;

            if (firstIssueDate != null) {
                Extension extension = ExtensionConverter.findExtension(fhirMedicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_FIRST_ISSUE_DATE);
                if (extension == null) {
                    fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_FIRST_ISSUE_DATE, firstIssueDate));
                    changed = true;

                } else {
                    Date newDate = firstIssueDate.getValue();

                    DateType existingValue = (DateType)extension.getValue();
                    Date existingDate = existingValue.getValue();

                    if (newDate.before(existingDate)) {
                        extension.setValue(firstIssueDate);
                        changed = true;
                    }
                }
            }

            if (lastIssueDate != null) {
                Extension extension = ExtensionConverter.findExtension(fhirMedicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_MOST_RECENT_ISSUE_DATE);
                if (extension == null) {
                    fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_MOST_RECENT_ISSUE_DATE, lastIssueDate));
                    changed = true;

                } else {
                    Date newDate = lastIssueDate.getValue();

                    DateType existingValue = (DateType)extension.getValue();
                    Date existingDate = existingValue.getValue();

                    if (newDate.after(existingDate)) {
                        extension.setValue(lastIssueDate);
                        changed = true;
                    }
                }
            }

            //if we've made any changes then save to the DB, making sure to skip ID mapping (since it's already mapped)
            if (changed) {
                String patientGuid = getPatientGuidFromUniqueId(medicationStatementLocalId);
                fhirResourceFiler.savePatientResource(null, false, patientGuid, fhirMedicationStatement);
            }
        }
    }

    /**
     * temporary storage class for changes to the practitioners involved in a session
     */
    public class SessionPractitioners {
        private List<String> emisUserGuidsToSave = new ArrayList<>();
        private List<String> emisUserGuidsToDelete = new ArrayList<>();
        private boolean processedSession = false;

        public List<String> getEmisUserGuidsToSave() {
            return emisUserGuidsToSave;
        }

        public void setEmisUserGuidsToSave(List<String> emisUserGuidsToSave) {
            this.emisUserGuidsToSave = emisUserGuidsToSave;
        }

        public List<String> getEmisUserGuidsToDelete() {
            return emisUserGuidsToDelete;
        }

        public void setEmisUserGuidsToDelete(List<String> emisUserGuidsToDelete) {
            this.emisUserGuidsToDelete = emisUserGuidsToDelete;
        }

        public boolean isProcessedSession() {
            return processedSession;
        }

        public void setProcessedSession(boolean processedSession) {
            this.processedSession = processedSession;
        }
    }

    /**
     * temporary storage class for a CodeableConcept and Date
     */
    public class DateAndCode {
        private DateTimeType date = null;
        private CodeableConcept codeableConcept = null;

        public DateAndCode(DateTimeType date, CodeableConcept codeableConcept) {
            this.date = date;
            this.codeableConcept = codeableConcept;
        }

        public DateTimeType getDate() {
            return date;
        }

        public CodeableConcept getCodeableConcept() {
            return codeableConcept;
        }

        public boolean isBefore(DateTimeType other) {
            if (date == null) {
                return true;
            } else if (other == null) {
                return false;
            } else {
                return date.before(other);
            }

        }
    }
}
