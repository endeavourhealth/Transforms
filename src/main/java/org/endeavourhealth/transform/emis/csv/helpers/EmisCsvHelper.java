package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.ProblemRelationshipType;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisTransformDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisAdminResourceCache;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EmisCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCsvHelper.class);

    //private static final String CODEABLE_CONCEPT = "CodeableConcept";
    private static final String ID_DELIMITER = ":";

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String dataSharingAgreementGuid;
    private final boolean processPatientData;

    //metadata, not relating to patients
    private Map<Long, EmisCsvCodeMap> clinicalCodes = new ConcurrentHashMap<>();
    private Map<Long, EmisCsvCodeMap> medication = new ConcurrentHashMap<>();
    private EmisTransformDalI mappingRepository = DalProvider.factoryEmisTransformDal();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    //some resources are referred to by others, so we cache them here for when we need them
    private Map<String, String> problemMap = new HashMap<>(); //ideally would cache ConditionBuilders, but resources are too memory hungry
    private Map<String, ResourceFieldMappingAudit> problemAuditMap = new HashMap<>();
    private Map<String, ReferralRequestBuilder> referralMap = new HashMap<>();
    private Map<String, ReferenceList> observationChildMap = new HashMap<>();
    private Map<String, ReferenceList> newProblemChildren = new HashMap<>();
    private Map<String, ReferenceList> consultationNewChildMap = new HashMap<>();
    private Map<String, ReferenceList> consultationExistingChildMap = new ConcurrentHashMap<>(); //written to by many threads
    private Map<String, IssueRecordIssueDate> drugRecordLastIssueDateMap = new HashMap<>();
    private Map<String, IssueRecordIssueDate> drugRecordFirstIssueDateMap = new HashMap<>();
    private Map<String, List<BpComponent>> bpComponentMap = new HashMap<>();
    private Map<String, SessionPractitioners> sessionPractitionerMap = new HashMap<>();
    private Map<String, List<CsvCell>> locationOrganisationMap = new HashMap<>();
    private Map<String, CodeAndDate> ethnicityMap = new HashMap<>();
    private Map<String, CodeAndDate> maritalStatusMap = new HashMap<>();
    private Map<String, String> problemReadCodes = new HashMap<>();
    private Map<String, ResourceType> parentObservationResourceTypes = new HashMap<>();

    private Map<String, ReferenceList> problemPreviousLinkedResources = new ConcurrentHashMap<>(); //written to by many threads

    public EmisCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId, String dataSharingAgreementGuid, boolean processPatientData) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.dataSharingAgreementGuid = dataSharingAgreementGuid;
        this.processPatientData = processPatientData;
    }


    @Override
    public UUID getServiceId() {
        return serviceId;
    }

    @Override
    public UUID getSystemId() {
        return systemId;
    }

    @Override
    public UUID getExchangeId() {
        return exchangeId;
    }

    public String getDataSharingAgreementGuid() {
        return dataSharingAgreementGuid;
    }

    public boolean isProcessPatientData() {
        return processPatientData;
    }

    /**
     * to ensure globally unique IDs for all resources, a new ID is created
     * from the patientGuid and sourceGuid (e.g. observationGuid)
     */
    public static String createUniqueId(CsvCell patientGuid, CsvCell sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid.getString();
        } else {
            return patientGuid.getString() + ID_DELIMITER + sourceGuid.getString();
        }
    }

    /*public static String createUniqueId(String patientGuid, String sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid;
        } else {
            return patientGuid + ID_DELIMITER + sourceGuid;
        }
    }*/

    /*private static String getPatientGuidFromUniqueId(String uniqueId) {
        String[] toks = uniqueId.split(ID_DELIMITER);
        if (toks.length == 1
                || toks.length == 2) {
            return toks[0];
        } else {
            throw new IllegalArgumentException("Invalid unique ID string [" + uniqueId + "] - expect one or two tokens delimited with " + ID_DELIMITER);
        }
    }*/

    public static void setUniqueId(ResourceBuilderBase resourceBuilder, CsvCell patientGuid, CsvCell sourceGuid) {
        String resourceId = createUniqueId(patientGuid, sourceGuid);
        resourceBuilder.setId(resourceId, patientGuid, sourceGuid);
    }

    /*public static void setUniqueId(Resource resource, String patientGuid, String sourceGuid) {
        resource.setId(createUniqueId(patientGuid, sourceGuid));
    }*/

    public void saveClinicalOrDrugCode(EmisCsvCodeMap mapping) throws Exception {
        //this is only needed for Cassandra backwards compatbility and can be removed in the future
        mapping.setDataSharingAgreementGuid(dataSharingAgreementGuid);

        mappingRepository.save(mapping);
    }

    public EmisCsvCodeMap findClinicalCode(CsvCell codeIdCell) throws Exception {
        EmisCsvCodeMap ret = clinicalCodes.get(codeIdCell.getLong());
        if (ret == null) {
            ret = mappingRepository.getMostRecentCode(dataSharingAgreementGuid, false, codeIdCell.getLong());
            if (ret == null) {
                if (TransformConfig.instance().isEmisAllowMissingCodes()) {
                    LOG.error("Failed to find clincal codeable concept for code ID " + codeIdCell.getLong());

                    Coding coding = new Coding();
                    coding.setSystem(FhirCodeUri.CODE_SYSTEM_READ2);
                    coding.setCode("?????");
                    coding.setDisplay("Unknown code");

                    CodeableConcept codeableConcept = new CodeableConcept();
                    codeableConcept.setText("Missing Clinical Code (Emis ECR 9953529)");
                    codeableConcept.addCoding(coding);

                    ret = new EmisCsvCodeMap();
                    ret.setCodeableConceptObject(codeableConcept);
                    ret.setCodeType(ClinicalCodeType.Conditions_Operations_Procedures.getValue());

                } else {
                    throw new Exception("Failed to find clinical code for codeId " + codeIdCell.getLong());
                }
            }
            clinicalCodes.put(codeIdCell.getLong(), ret);
        }

        return ret;
    }

    public ClinicalCodeType findClinicalCodeType(CsvCell codeIdCell) throws Exception {

        EmisCsvCodeMap ret = findClinicalCode(codeIdCell);
        String typeStr = ret.getCodeType();
        return ClinicalCodeType.fromValue(typeStr);
    }

    public EmisCsvCodeMap findMedication(CsvCell codeIdCell) throws Exception {

        EmisCsvCodeMap ret = medication.get(codeIdCell.getLong());
        if (ret == null) {
            ret = mappingRepository.getMostRecentCode(dataSharingAgreementGuid, true, codeIdCell.getLong());
            if (ret == null) {
                if (TransformConfig.instance().isEmisAllowMissingCodes()) {
                    //until we move to AWS, and Emis actually fix this, substitute a dummy codeable concept
                    LOG.error("Failed to find medication codeable concept for code ID " + codeIdCell.getLong());

                    CodeableConcept codeableConcept = new CodeableConcept();
                    codeableConcept.setText("Missing Drug Code (Emis ECR 9953529)");

                    ret = new EmisCsvCodeMap();
                    ret.setCodeableConceptObject(codeableConcept);

                } else {
                    throw new Exception("Failed to find drug code for codeId " + codeIdCell.getLong());
                }
            }
            medication.put(codeIdCell.getLong(), ret);
        }

        return ret;
    }

    /**
     * admin-type resources just use the EMIS CSV GUID as their reference
     */
    public Reference createPractitionerReference(CsvCell practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid.getString());
    }
    public Reference createOrganisationReference(CsvCell organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid.getString());
    }
    public Reference createLocationReference(CsvCell locationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid.getString());
    }
    public Reference createScheduleReference(CsvCell scheduleGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Schedule, scheduleGuid.getString());
    }
    public Reference createSlotReference(CsvCell slotGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Slot, slotGuid.getString());
    }


    /*public Reference createLocationReference(String locationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid);
    }
    public Reference createOrganisationReference(String organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid);
    }
    public Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }
    public Reference createScheduleReference(String scheduleGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Schedule, scheduleGuid);
    }
    public Reference createSlotReference(String slotGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Slot, slotGuid);
    }*/

    /**
     * patient-type resources must include the patient GUID are part of the unique ID in the reference
     * because the EMIS GUIDs for things like Obs are only unique within that patient record itself
     */
    public Reference createPatientReference(CsvCell patientGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Patient, createUniqueId(patientGuid, null));
    }
    public Reference createAppointmentReference(CsvCell appointmentGuid, CsvCell patientGuid) throws Exception {
        if (appointmentGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing appointmentGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Appointment, createUniqueId(patientGuid, appointmentGuid));
    }
    public Reference createEncounterReference(CsvCell encounterGuid, CsvCell patientGuid) throws Exception {
        if (encounterGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing encounterGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Encounter, createUniqueId(patientGuid, encounterGuid));
    }
    public Reference createConditionReference(CsvCell problemGuid, CsvCell patientGuid) {
        if (problemGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing problemGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Condition, createUniqueId(patientGuid, problemGuid));
    }
    public Reference createMedicationStatementReference(CsvCell medicationStatementGuid, CsvCell patientGuid) throws Exception {
        if (medicationStatementGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing medicationStatementGuid");
        }
        return ReferenceHelper.createReference(ResourceType.MedicationStatement, createUniqueId(patientGuid, medicationStatementGuid));
    }
    public Reference createProblemReference(CsvCell problemGuid, CsvCell patientGuid) throws Exception {
        if (problemGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing problemGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Condition, createUniqueId(patientGuid, problemGuid));
    }
    public Reference createEpisodeReference(CsvCell patientGuid) {
        //the episode of care just uses the patient GUID as its ID, so that's all we need to refer to it too
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, patientGuid.getString());
    }


    public void cacheReferral(CsvCell observationGuid, CsvCell patientGuid, ReferralRequestBuilder referralRequestBuilder) {
        referralMap.put(createUniqueId(patientGuid, observationGuid), referralRequestBuilder);
    }

    public ReferralRequestBuilder findReferral(CsvCell observationGuid, CsvCell patientGuid) {
        return referralMap.remove(createUniqueId(patientGuid, observationGuid));
    }

    public void cacheProblem(CsvCell observationGuid, CsvCell patientGuid, ConditionBuilder conditionBuilder) throws Exception {
        //cache the resource as a JSON string, as the FHIR conditions take about 4KB memory EACH
        String uid = createUniqueId(patientGuid, observationGuid);
        Resource resource = conditionBuilder.getResource();
        String json = FhirSerializationHelper.serializeResource(resource);
        ResourceFieldMappingAudit audit = conditionBuilder.getAuditWrapper();
        problemMap.put(uid, json);
        problemAuditMap.put(uid, audit);

        //problemMap.put(createUniqueId(patientGuid, observationGuid), conditionBuilder);
    }

    public boolean existsProblem(CsvCell observationGuid, CsvCell patientGuid) {
        return problemMap.get(createUniqueId(patientGuid, observationGuid)) != null;
    }

    public ConditionBuilder findProblem(CsvCell observationGuid, CsvCell patientGuid) throws Exception {
        String uid = createUniqueId(patientGuid, observationGuid);
        return findProblem(uid);
    }

    private ConditionBuilder findProblem(String uid) throws Exception {

        //cache as a JSON string, as the FHIR conditions take about 4KB memory EACH
        String json = problemMap.remove(uid);
        if (json != null) {
            ResourceFieldMappingAudit audit = problemAuditMap.remove(uid);
            Condition condition = (Condition) FhirSerializationHelper.deserializeResource(json);
            return new ConditionBuilder(condition, audit);
        } else {
            return null;
        }
        //return problemMap.remove(problemLocalUniqueId);
    }

    public ReferenceList getAndRemoveObservationParentRelationships(String parentObservationSourceId) {
        return observationChildMap.remove(parentObservationSourceId);
    }

    /*public boolean hasChildObservations(String parentObservationSourceId) {
        return observationChildMap.containsKey(parentObservationSourceId);
    }*/

    public boolean hasChildObservations(CsvCell parentObservationGuid, CsvCell patientGuid) {
        return observationChildMap.containsKey(createUniqueId(patientGuid, parentObservationGuid));
    }

    public void cacheObservationParentRelationship(CsvCell parentObservationGuid,
                                                   CsvCell patientGuid,
                                                   CsvCell observationGuid,
                                                   ResourceType resourceType) {

        String parentObservationUniqueId = createUniqueId(patientGuid, parentObservationGuid);
        ReferenceList list = observationChildMap.get(parentObservationUniqueId);
        if (list == null) {
            list = new ReferenceList();
            observationChildMap.put(parentObservationUniqueId, list);
        }

        String childObservationUniqueId = createUniqueId(patientGuid, observationGuid);
        Reference reference = ReferenceHelper.createReference(resourceType, childObservationUniqueId);
        list.add(reference, observationGuid);
    }


    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, resourceType, locallyUniqueId);

        //if we've never mapped the local ID to a EDS UI, then we've never heard of this resource before
        if (globallyUniqueId == null) {
            return null;
        }

        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), globallyUniqueId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        try {
            return FhirSerializationHelper.deserializeResource(json);
        } catch (Throwable t) {
            throw new Exception("Error deserialising " + resourceType + " " + globallyUniqueId + " (raw ID " + locallyUniqueId + ")", t);
        }

    }

    public List<Resource> retrieveAllResourcesForPatient(String patientGuid, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID edsPatientId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, patientGuid);
        if (edsPatientId == null) {
            return null;
        }

        UUID serviceId = fhirResourceFiler.getServiceId();
        UUID systemId = fhirResourceFiler.getSystemId();
        List<ResourceWrapper> resourceWrappers = resourceRepository.getResourcesByPatient(serviceId, edsPatientId);

        List<Resource> ret = new ArrayList<>();

        for (ResourceWrapper resourceWrapper: resourceWrappers) {
            String json = resourceWrapper.getResourceData();
            Resource resource = FhirSerializationHelper.deserializeResource(json);
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
        for (Map.Entry<String, ReferenceList> entry : observationChildMap.entrySet())
            updateExistingObservationWithNewChildLinks(entry.getKey(), entry.getValue(), fhirResourceFiler);
    }


    private void updateExistingObservationWithNewChildLinks(String locallyUniqueId,
                                                            ReferenceList childResourceRelationships,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        //the parent "observation" may have been saved as an observation OR a diagnostic report, so try both
        ResourceBuilderBase resourceBuilder = null;

        DiagnosticReport fhirDiagnosticReport = (DiagnosticReport)retrieveResource(locallyUniqueId, ResourceType.DiagnosticReport);
        if (fhirDiagnosticReport != null) {
            resourceBuilder = new DiagnosticReportBuilder(fhirDiagnosticReport);
        } else {
            Observation fhirObservation = (Observation)retrieveResource(locallyUniqueId, ResourceType.Observation);
            if (fhirObservation != null) {
                resourceBuilder = new ObservationBuilder(fhirObservation);
            } else {
                //if the resource can't be found (or isn't an Observation or DiagnosticReport), then we can't update it
                return;
            }
        }

        boolean changed = false;

        for (int i=0; i<childResourceRelationships.size(); i++) {
            Reference reference = childResourceRelationships.getReference(i);
            CsvCell[] sourceCells = childResourceRelationships.getSourceCells(i);

            //the resource has already been ID mapped, so we need to manually convert our local reference to a global one
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);

            if (resourceBuilder instanceof DiagnosticReportBuilder) {
                DiagnosticReportBuilder diagnosticReportBuilder = (DiagnosticReportBuilder)resourceBuilder;
                if (diagnosticReportBuilder.addResult(globallyUniqueReference, sourceCells)) {
                    changed = true;
                }

            } else {
                ObservationBuilder observationBuilder = (ObservationBuilder)resourceBuilder;
                if (observationBuilder.addChildObservation(globallyUniqueReference, sourceCells)) {
                    changed = true;
                }
            }
        }

        if (changed) {
            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
            fhirResourceFiler.savePatientResource(null, false, resourceBuilder);
        }
    }

    public ReferenceList getAndRemoveNewProblemChildren(CsvCell problemGuid, CsvCell patientGuid) {
        return newProblemChildren.remove(createUniqueId(patientGuid, problemGuid));
    }

    public void cacheProblemRelationship(CsvCell problemObservationGuid,
                                         CsvCell patientGuid,
                                         CsvCell resourceGuid,
                                         ResourceType resourceType) {

        if (problemObservationGuid.isEmpty()) {
            return;
        }

        String problemLocalUniqueId = createUniqueId(patientGuid, problemObservationGuid);
        ReferenceList referenceList = newProblemChildren.get(problemLocalUniqueId);
        if (referenceList == null) {
            referenceList = new ReferenceList();
            newProblemChildren.put(problemLocalUniqueId, referenceList);
        }

        String resourceLocalUniqueId = createUniqueId(patientGuid, resourceGuid);
        Reference reference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        referenceList.add(reference, problemObservationGuid);
    }

    /**
     * called at the end of the transform, to update pre-existing Problem resources with references to new
     * clinical resources that are in those problems
     */
    public void processRemainingProblemRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (String locallyUniqueResourceProblemId: newProblemChildren.keySet()) {
            ReferenceList newLinkedItems = newProblemChildren.get(locallyUniqueResourceProblemId);

            Condition existingCondition = (Condition)retrieveResource(locallyUniqueResourceProblemId, ResourceType.Condition);
            if (existingCondition == null) {
                //if the problem has been deleted, just skip it
                return;
            }

            ConditionBuilder conditionBuilder = new ConditionBuilder(existingCondition);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

            for (int i=0; i<newLinkedItems.size(); i++) {
                Reference reference = newLinkedItems.getReference(i);
                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);

                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
                containedListBuilder.addContainedListItem(globallyUniqueReference, sourceCells);
            }

            fhirResourceFiler.savePatientResource(null, false, conditionBuilder);
        }
    }



    /*public List<Reference> findPreviousLinkedReferences(FhirResourceFiler fhirResourceFiler,
                                                       String locallyUniqueId,
                                                       ResourceType resourceType) throws Exception {

        DomainResource previousVersion = (DomainResource)retrieveResource(locallyUniqueId, resourceType, fhirResourceFiler);
        if (previousVersion == null) {
            //if this is the first time, then we'll have a null resource
            return null;
        }
        List<Reference> edsReferences = new ArrayList<>();

        if (previousVersion.hasContained()) {
            for (Resource contained: previousVersion.getContained()) {
                if (contained instanceof List_) {
                    List_ list = (List_)contained;

                    for (List_.ListEntryComponent entry: list.getEntry()) {
                        Reference previousReference = entry.getItem();
                        edsReferences.add(previousReference);
                    }
                }
            }
        }

        UUID serviceId = fhirResourceFiler.getServiceId();

        //the reference we have has already been mapped to an EDS ID, so we need to un-map it
        //back to the source ID, so the ID mapper can safely map it when we save the resource
        return IdHelper.convertEdsReferencesToLocallyUniqueReferences(serviceId, edsReferences);
    }*/

    public void cacheDrugRecordDate(CsvCell drugRecordGuid, CsvCell patientGuid, IssueRecordIssueDate newIssueDate) {
        String uniqueId = createUniqueId(patientGuid, drugRecordGuid);

        IssueRecordIssueDate previous = drugRecordFirstIssueDateMap.get(uniqueId);
        if (newIssueDate.beforeOrOtherIsNull(previous)) {
            drugRecordFirstIssueDateMap.put(uniqueId, newIssueDate);
        }

        previous = drugRecordLastIssueDateMap.get(uniqueId);
        if (newIssueDate.afterOrOtherIsNull(previous)) {
            drugRecordLastIssueDateMap.put(uniqueId, newIssueDate);
        }
    }

    public IssueRecordIssueDate getDrugRecordFirstIssueDate(CsvCell drugRecordId, CsvCell patientGuid) {
        return drugRecordFirstIssueDateMap.remove(createUniqueId(patientGuid, drugRecordId));
    }

    public IssueRecordIssueDate getDrugRecordLastIssueDate(CsvCell drugRecordId, CsvCell patientGuid) {
        return drugRecordLastIssueDateMap.remove(createUniqueId(patientGuid, drugRecordId));
    }



    public void cacheBpComponent(CsvCell parentObservationGuid, CsvCell patientGuid, BpComponent bpComponent) {
        String key = createUniqueId(patientGuid, parentObservationGuid);
        List<BpComponent> list = bpComponentMap.get(key);
        if (list == null) {
            list = new ArrayList<>();
            bpComponentMap.put(key, list);
        }
        list.add(bpComponent);
    }

    public List<BpComponent> findBpComponents(CsvCell observationGuid, CsvCell patientGuid) {
        String key = createUniqueId(patientGuid, observationGuid);
        return bpComponentMap.remove(key);
    }

    public void cacheSessionPractitionerMap(CsvCell sessionCell, CsvCell emisUserGuid, boolean isDeleted) {

        String sessionGuid = sessionCell.getString();

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

    public List<CsvCell> findSessionPractionersToSave(CsvCell sessionCell) {

        String sessionGuid = sessionCell.getString();

        //unlike the other maps, we don't remove from this map, since we need to be able to look up
        //the staff for a session when creating Schedule resources and Appointment ones
        SessionPractitioners obj = sessionPractitionerMap.get(sessionGuid);
        if (obj == null) {
            return new ArrayList<>();

        } else {
            return obj.getEmisUserGuidsToSave();
        }
    }

    public List<CsvCell> findSessionPractionersToDelete(CsvCell sessionCell) {

        String sessionGuid = sessionCell.getString();

        //unlike the other maps, we don't remove from this map, since we need to be able to look up
        //the staff for a session when creating Schedule resources and Appointment ones
        SessionPractitioners obj = sessionPractitionerMap.get(sessionGuid);
        if (obj == null) {
            return new ArrayList<>();

        } else {
            return obj.getEmisUserGuidsToDelete();
        }
    }

    /**
     * called at the end of the transform. If the sessionPractitionerMap contains any entries that haven't been processed
     * then we have changes to the staff in a previously saved FHIR Schedule, so we need to amend that Schedule
     */
    /*public void processRemainingSessionPractitioners(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Map.Entry<String, SessionPractitioners> entry : sessionPractitionerMap.entrySet()) {
            if (!entry.getValue().isProcessedSession()) {
                updateExistingScheduleWithNewPractitioners(entry.getKey(), entry.getValue(), fhirResourceFiler);
            }
        }
    }

    private void updateExistingScheduleWithNewPractitioners(String sessionGuid, SessionPractitioners practitioners, FhirResourceFiler fhirResourceFiler) throws Exception {

        Schedule fhirSchedule = (Schedule)retrieveResource(sessionGuid, ResourceType.Schedule, fhirResourceFiler);
        if (fhirSchedule == null) {
            //if a session user record has been updated for a deleted schedule, we'll have a null here, so just safely ignore it
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

        ScheduleBuilder scheduleBuilder = new ScheduleBuilder(fhirSchedule);

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
            for ()

            Reference first = references.get(0);
            fhirSchedule.setActor(first);

            //add any additional references as additional actors
            for (int i = 1; i < references.size(); i++) {
                Reference additional = references.get(i);
                fhirSchedule.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.SCHEDULE_ADDITIONAL_ACTOR, additional));
            }
        }

        fhirResourceFiler.saveAdminResource(null, false, scheduleBuilder);
    }*/

    public void cacheOrganisationLocationMap(CsvCell locationCell, CsvCell orgCell, boolean mainLocation) {

        String locationGuid = locationCell.getString();

        List<CsvCell> orgGuids = locationOrganisationMap.get(locationGuid);
        if (orgGuids == null) {
            orgGuids = new ArrayList<>();
            locationOrganisationMap.put(locationGuid, orgGuids);
        }

        //if this location link is for the main location of an organisation, then insert that
        //org at the start of the list, so it's used as the managing organisation for the location
        if (mainLocation) {
            orgGuids.add(0, orgCell);
        } else {
            orgGuids.add(orgCell);
        }
    }

    public List<CsvCell> findOrganisationLocationMapping(CsvCell locationCell) {
        String locationGuid = locationCell.getString();
        return locationOrganisationMap.remove(locationGuid);
    }

    /**
     * called at the end of the transform to handle any changes to location/organisation mappings
     * that weren't handled when we went through the Location file (i.e. changes in the OrganisationLocation file
     * with no corresponding changes in the Location file)
     */
    public void processRemainingOrganisationLocationMappings(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Map.Entry<String, List<CsvCell>> entry : locationOrganisationMap.entrySet()) {

            Location fhirLocation = (Location)retrieveResource(entry.getKey(), ResourceType.Location);
            if (fhirLocation == null) {
                //if the location has been deleted, it doesn't matter, and the emis data integrity issues
                //mean we may have references to unknown locations
                continue;
            }

            LocationBuilder locationBuilder = new LocationBuilder(fhirLocation);

            CsvCell organisationCell = entry.getValue().get(0);
            String organisationGuid = organisationCell.getString();

            //the resource has already been through the ID mapping process, so we need to manually map the organisation GUID to discovery UUID
            String globallyUniqueId = IdHelper.getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(),
                    ResourceType.Organization,
                    organisationGuid);

            Reference reference = ReferenceHelper.createReference(ResourceType.Organization, globallyUniqueId);
            locationBuilder.setManagingOrganisation(reference, organisationCell);

            fhirResourceFiler.saveAdminResource(null, false, locationBuilder);
        }
    }

    public void cacheEthnicity(CsvCell patientGuid, CodeAndDate codeAndDate) {
        String localUniquePatientId = createUniqueId(patientGuid, null);
        CodeAndDate existing = ethnicityMap.get(localUniquePatientId);
        if (codeAndDate.isAfterOrOtherIsNull(existing)) {
            ethnicityMap.put(localUniquePatientId, codeAndDate);
        }
    }

    public CodeAndDate findEthnicity(CsvCell patientGuid) {
        String localUniquePatientId = createUniqueId(patientGuid, null);
        return ethnicityMap.remove(localUniquePatientId);
    }

    public void cacheMaritalStatus(CsvCell patientGuid, CodeAndDate codeAndDate) {
        String localUniquePatientId = createUniqueId(patientGuid, null);
        CodeAndDate existing = maritalStatusMap.get(localUniquePatientId);
        if (codeAndDate.isAfterOrOtherIsNull(existing)) {
            maritalStatusMap.put(localUniquePatientId, codeAndDate);
        }
    }

    public CodeAndDate findMaritalStatus(CsvCell patientGuid) {
        String localUniquePatientId = createUniqueId(patientGuid, null);
        return maritalStatusMap.remove(localUniquePatientId);
    }

    /**
     * when the transform is complete, if there's any values left in the ethnicity and marital status maps,
     * then we need to update pre-existing patients with new data
     */
    public void processRemainingEthnicitiesAndMartialStatuses(FhirResourceFiler fhirResourceFiler) throws Exception {

        //get a combined list of the keys (patientGuids) from both maps
        HashSet<String> patientGuids = new HashSet<>(ethnicityMap.keySet());
        patientGuids.addAll(new HashSet<>(maritalStatusMap.keySet()));

        for (String patientGuid: patientGuids) {

            CodeAndDate newEthnicity = ethnicityMap.get(patientGuid);
            CodeAndDate newMaritalStatus = maritalStatusMap.get(patientGuid);

            Patient fhirPatient = (Patient)retrieveResource(patientGuid, ResourceType.Patient);
            if (fhirPatient == null) {
                //if we try to update the ethnicity on a deleted patient, or one we've never received, we'll get this exception, which is fine to ignore
                continue;
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);

            if (newEthnicity != null) {
                EmisCsvCodeMap codeMapping = newEthnicity.getCodeMapping();
                CsvCell[] additionalSourceCells = newEthnicity.getAdditionalSourceCells();
                EmisCodeHelper.applyEthnicity(patientBuilder, codeMapping, additionalSourceCells);
            }

            if (newMaritalStatus != null) {
                EmisCsvCodeMap codeMapping = newMaritalStatus.getCodeMapping();
                CsvCell[] additionalSourceCells = newMaritalStatus.getAdditionalSourceCells();
                EmisCodeHelper.applyMaritalStatus(patientBuilder, codeMapping, additionalSourceCells);
            }

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
    }



    /**
     * when we receive the first extract for an organisation, we need to copy all the contents of the admin
     * resource cache and save them against the new organisation. This is because EMIS only send most Organisations,
     * Locations and Staff once, with the very first organisation, and when a second organisation is added to
     * the extract, none of that data is re-sent, so we have to create those resources for the new org
     */
    public void applyAdminResourceCache(FhirResourceFiler fhirResourceFiler) throws Exception {

        List<EmisAdminResourceCache> cachedResources = mappingRepository.getCachedResources(dataSharingAgreementGuid);
        LOG.trace("Got to apply " + cachedResources.size() + " admin resources");

        int count = 0;

        for (EmisAdminResourceCache cachedResource: cachedResources) {

            //wrap the resource and audit trail in a generic resource builder for saving
            Resource fhirResource = FhirSerializationHelper.deserializeResource(cachedResource.getResourceData());
            ResourceFieldMappingAudit audit = cachedResource.getAudit();
            GenericBuilder genericBuilder = new GenericBuilder(fhirResource, audit);

            fhirResourceFiler.saveAdminResource(null, genericBuilder);

            //to cut memory usage, clear out the JSON field on each object as we pass it. Due to weird
            //Emis org/practitioner hierarchy, we've got 500k practitioners to save, so this is quite a lot of memory
            cachedResource.setDataSharingAgreementGuid(null);
            cachedResource.setEmisGuid(null);
            cachedResource.setResourceType(null);
            cachedResource.setResourceData(null);
            cachedResource.setAudit(null);

            //log progress
            count ++;
            if (count % 50000 == 0) {
                LOG.trace("Done " + count + " / " + cachedResources.size());
            }
        }
    }

    /**
     * in some cases, we get a row in the CareRecord_Problem file but not in the CareRecord_Observation file,
     * when something about the problem only has changed (e.g. ending a problem). This is called at the end
     * of the transform to handle those changes to problems that weren't handled when we processed the Observation file.
     */
    public void processRemainingProblems(FhirResourceFiler fhirResourceFiler) throws Exception {

        //the findProblem removes from the map, so we can't iterate directly over the keySet and need to copy that into a separate collection
        //for (String uid: problemMap.keySet()) {
        List<String> uids = new ArrayList<>(problemMap.keySet());
        for (String uid: uids) {
            ConditionBuilder conditionBuilder = findProblem(uid);

            if (conditionBuilder == null) {
                throw new TransformException("Null ConditionBuilder for UID " + uid);
            }

            //if the resource has the Condition profile URI, then it means we have a pre-existing problem
            //that's now been deleted from being a problem, but the root Observation itself has not (i.e.
            //the problem has been down-graded from being a problem to just an observation)
            if (conditionBuilder.isProblem()) {
                updateExistingProblem(conditionBuilder, fhirResourceFiler);

            } else {
                downgradeExistingProblemToCondition(uid, fhirResourceFiler);
            }
        }
    }

    /**
     * updates an existing problem with new data we've received, when we didn't also get an update in the Observation file
     */
    private void updateExistingProblem(ConditionBuilder newConditionBuilder, FhirResourceFiler fhirResourceFiler) throws Exception {

        String locallyUniqueId = newConditionBuilder.getResourceId();

        Condition existingResource = (Condition)retrieveResource(locallyUniqueId, ResourceType.Condition);
        if (existingResource == null) {
            //emis seem to send bulk data containing deleted records, so ignore any attempt to downgrade
            //a problem that doesn't actually exist
            return;
        }

        //create a new condition builder around the old problem instance, so we can copy the new content into it
        //we also need to carry over all the audits from the part-completed problem to the new condition builder
        ResourceFieldMappingAudit newAudit = newConditionBuilder.getAuditWrapper();
        ConditionBuilder conditionBuilder = new ConditionBuilder(existingResource, newAudit);

        //we may be upgrading an existing condition to a problem, so make sure we change the profile URL
        conditionBuilder.setAsProblem(true);

        //then carry over all the new data from the updated resource
        Type endDateOrBoolean = newConditionBuilder.getEndDateOrBoolean();
        conditionBuilder.setEndDateOrBoolean(endDateOrBoolean);

        Integer duration = newConditionBuilder.getExpectedDuration();
        conditionBuilder.setExpectedDuration(duration);

        DateType lastReviewDate = newConditionBuilder.getProblemLastReviewDate();
        conditionBuilder.setProblemLastReviewDate(lastReviewDate);

        Reference lastReviewedBy = newConditionBuilder.getProblemLastReviewedBy();
        if (lastReviewedBy != null) {
            //the reference will be using an Emis guid, so map to a Discovery UUID
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(lastReviewedBy, fhirResourceFiler);
            conditionBuilder.setProblemLastReviewedBy(globallyUniqueReference);
        } else {
            conditionBuilder.setProblemLastReviewedBy(null);
        }

        ProblemSignificance problemSignificance = newConditionBuilder.getProblemSignificance();
        conditionBuilder.setProblemSignificance(problemSignificance);

        Reference parentProblem = newConditionBuilder.getParentProblem();
        if (parentProblem != null) {
            //the reference will be using an Emis guid, so map to a Discovery UUID
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentProblem, fhirResourceFiler);
            conditionBuilder.setParentProblem(globallyUniqueReference);
        } else {
            conditionBuilder.setParentProblem(null);
        }

        ProblemRelationshipType parentProblemRelationship = newConditionBuilder.getParentProblemRelationship();
        conditionBuilder.setParentProblemRelationship(parentProblemRelationship);

        String additionalNotes = newConditionBuilder.getAdditionalNotes();
        conditionBuilder.setAdditionalNotes(additionalNotes);

        ContainedListBuilder newContainedListBuilder = new ContainedListBuilder(newConditionBuilder);
        List<Reference> newLinkedItems = newContainedListBuilder.getContainedListItems();
        List<Reference> newLinkedItemsIdMapped = new ArrayList<>();
        for (Reference reference: newLinkedItems) {
            Reference mappedReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
            newLinkedItemsIdMapped.add(mappedReference);
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);
        containedListBuilder.addReferencesNoAudit(newLinkedItems);

        //make sure to pass in FALSE to bypass ID mapping, since we're saving an already ID mapped instance
        fhirResourceFiler.savePatientResource(null, false, conditionBuilder);
    }

    /**
     * down-grades an existing problem to a regular condition, by changing the profile URI and removing all
     * the problem-specific data, leaving just the original condition
     */
    private void downgradeExistingProblemToCondition(String locallyUniqueId, FhirResourceFiler fhirResourceFiler) throws Exception {

        Condition existingProblem = (Condition)retrieveResource(locallyUniqueId, ResourceType.Condition);
        if (existingProblem == null) {
            //emis seem to send bulk data containing deleted records, so ignore any attempt to downgrade
            //a problem that doesn't actually exist
            return;
        }

        ConditionBuilder conditionBuilder = new ConditionBuilder(existingProblem);

        //change its profile URL to say it's not a problem
        conditionBuilder.setAsProblem(false);

        //clear down all the problem-specific condition fields
        conditionBuilder.setEndDateOrBoolean(null);
        conditionBuilder.setExpectedDuration(null);
        conditionBuilder.setProblemLastReviewDate(null);
        conditionBuilder.setProblemLastReviewedBy(null);
        conditionBuilder.setProblemSignificance(null);
        conditionBuilder.setParentProblem(null);
        conditionBuilder.setParentProblemRelationship(null);
        conditionBuilder.setAdditionalNotes(null);

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);
        containedListBuilder.removeContainedList();

        fhirResourceFiler.savePatientResource(null, false, conditionBuilder);
    }
    /*private void removeAllProblemSpecificFields(ConditionBuilder conditionBuilder) {

        Condition fhirProblem = (Condition)conditionBuilder.getResource();
        if (fhirProblem.hasAbatement()) {
            fhirProblem.setAbatement(null);
        }

        if (fhirProblem.hasExtension()) {
            List<Extension> extensions = fhirProblem.getExtension();

            //iterate backwards, so we can safely remove as we go
            for (int i=extensions.size()-1; i>=0; i--) {
                Extension extension = extensions.get(i);
                String url = extension.getUrl();
                if (url.equals(FhirExtensionUri.PROBLEM_EXPECTED_DURATION)
                        || url.equals(FhirExtensionUri.PROBLEM_LAST_REVIEWED)
                        || url.equals(FhirExtensionUri.PROBLEM_SIGNIFICANCE)
                        || url.equals(FhirExtensionUri.PROBLEM_RELATED)
                        || url.equals(FhirExtensionUri.PROBLEM_ASSOCIATED_RESOURCE)) {
                    extensions.remove(i);
                }
            }
        }

        if (fhirProblem.hasContained()) {
            for (Resource contained: fhirProblem.getContained()) {
                if (contained.getId().equals(CONTAINED_LIST_ID)) {
                    fhirProblem.getContained().remove(contained);
                    break;
                }
            }
        }
    }*/

    private static boolean isCondition(Condition condition) {

        Meta meta = condition.getMeta();
        for (UriType profileUri: meta.getProfile()) {
            if (profileUri.getValue().equals(FhirProfileUri.PROFILE_URI_CONDITION)) {
                return true;
            }
        }

        return false;
    }



    public void cacheNewConsultationChildRelationship(CsvCell consultationGuid,
                                                      CsvCell patientGuid,
                                                      CsvCell resourceGuid,
                                                      ResourceType resourceType) {

        if (consultationGuid.isEmpty()) {
            return;
        }

        String consultationLocalUniqueId = createUniqueId(patientGuid, consultationGuid);
        ReferenceList list = consultationNewChildMap.get(consultationLocalUniqueId);
        if (list == null) {
            list = new ReferenceList();
            consultationNewChildMap.put(consultationLocalUniqueId, list);
        }

        String resourceLocalUniqueId = createUniqueId(patientGuid, resourceGuid);
        Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        list.add(resourceReference, consultationGuid);
    }

    public ReferenceList getAndRemoveNewConsultationRelationships(String encounterSourceId) {
        return consultationNewChildMap.remove(encounterSourceId);
    }

    public void processRemainingNewConsultationRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (String locallyUniqueResourceEncounterId: consultationNewChildMap.keySet()) {
            ReferenceList newLinkedItems = consultationNewChildMap.get(locallyUniqueResourceEncounterId);

            Encounter existingEncounter = (Encounter)retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (existingEncounter == null) {
                //if the problem has been deleted, just skip it
                return;
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(existingEncounter);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

            for (int i=0; i<newLinkedItems.size(); i++) {
                Reference reference = newLinkedItems.getReference(i);
                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);

                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
                containedListBuilder.addContainedListItem(globallyUniqueReference, sourceCells);
            }

            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
        }
    }



    public void cacheProblemObservationGuid(CsvCell patientGuid, CsvCell problemGuid, String readCode) {
        problemReadCodes.put(createUniqueId(patientGuid, problemGuid), readCode);
    }

    public boolean isProblemObservationGuid(CsvCell patientGuid, CsvCell problemGuid) {
        return problemReadCodes.containsKey(createUniqueId(patientGuid, problemGuid));
    }

    public String findProblemObservationReadCode(CsvCell patientGuid, CsvCell problemGuid, FhirResourceFiler fhirResourceFiler) throws Exception {

        String locallyUniqueId = createUniqueId(patientGuid, problemGuid);

        //if we've already cached our problem code, then just return it
        if (problemReadCodes.containsKey(locallyUniqueId)) {
            return problemReadCodes.get(locallyUniqueId);
        }

        //if we've not cached our problem code, then the problem itself isn't part of this extract,
        //so we'll need to retrieve it from the DB and cache the code
        String readCode = null;

        Condition fhirPproblem = (Condition)retrieveResource(locallyUniqueId, ResourceType.Condition);

        //we've had cases of data referring to non-existent problems, so check for null
        if (fhirPproblem != null) {
            CodeableConcept codeableConcept = fhirPproblem.getCode();
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

            MedicationStatement fhirMedicationStatement = (MedicationStatement)retrieveResource(medicationStatementLocalId, ResourceType.MedicationStatement);
            if (fhirMedicationStatement == null) {
                //if the medication statement doesn't exist or has been deleted, then just skip it
                continue;
            }

            IssueRecordIssueDate newFirstIssueDate = drugRecordFirstIssueDateMap.get(medicationStatementLocalId);
            IssueRecordIssueDate newLastIssueDate = drugRecordLastIssueDateMap.get(medicationStatementLocalId);

            MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder(fhirMedicationStatement);
            boolean changed = false;

            //only set if new one is earlier
            DateType existingFirstIssueDate = ExtensionConverter.findExtensionValueDate(fhirMedicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_FIRST_ISSUE_DATE);
            if (newFirstIssueDate.beforeOrOtherIsNull(existingFirstIssueDate)) {
                medicationStatementBuilder.setFirstIssueDate(newFirstIssueDate.getIssueDateType(), newFirstIssueDate.getSourceCells());
                changed = true;
            }

            //only set if new one is later
            DateType existingLastIssueDate = ExtensionConverter.findExtensionValueDate(fhirMedicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_MOST_RECENT_ISSUE_DATE);
            if (newFirstIssueDate.afterOrOtherIsNull(existingLastIssueDate)) {
                medicationStatementBuilder.setLastIssueDate(newLastIssueDate.getIssueDateType(), newLastIssueDate.getSourceCells());
                changed = true;
            }

            //save, making sure to skip ID mapping (since it's already mapped)
            if (changed) {
                //String patientGuid = getPatientGuidFromUniqueId(medicationStatementLocalId);
                fhirResourceFiler.savePatientResource(null, false, medicationStatementBuilder);
            }
        }
    }

    public void cacheConsultationPreviousLinkedResources(String encounterSourceId, List<Reference> previousReferences) {

        if (previousReferences == null
                || previousReferences.isEmpty()) {
            return;
        }

        ReferenceList obj = new ReferenceList();
        obj.add(previousReferences);

        consultationExistingChildMap.put(encounterSourceId, obj);
    }

    public ReferenceList findConsultationPreviousLinkedResources(String encounterSourceId) {
        return consultationExistingChildMap.remove(encounterSourceId);
    }


    public void cacheProblemPreviousLinkedResources(String problemSourceId, List<Reference> previousReferences) {
        if (previousReferences == null
                || previousReferences.isEmpty()) {
            return;
        }

        ReferenceList obj = new ReferenceList();
        obj.add(previousReferences);

        problemPreviousLinkedResources.put(problemSourceId, obj);
    }

    public ReferenceList findProblemPreviousLinkedResources(String problemSourceId) {
        return problemPreviousLinkedResources.remove(problemSourceId);
    }


    public ResourceType getCachedParentObservationResourceType(CsvCell patientGuidCell, CsvCell parentObservationCell) {
        String locallyUniqueId = createUniqueId(patientGuidCell, parentObservationCell);
        return parentObservationResourceTypes.get(locallyUniqueId);
    }

    public void cacheParentObservationResourceType(CsvCell patientGuidCell, CsvCell observationGuidCell, ResourceType resourceType) {
        String locallyUniqueId = createUniqueId(patientGuidCell, observationGuidCell);
        parentObservationResourceTypes.put(locallyUniqueId, resourceType);
    }

    /**
     * we've cached the resource type of EVERY observation, but only need those with a parent-child link,
     * so call this to remove the unnecessary ones and free up some memory
     */
    public void pruneUnnecessaryParentObservationResourceTypes() {

        Set<String> idsToRemove = new HashSet<>(parentObservationResourceTypes.keySet());
        idsToRemove.removeAll(observationChildMap.keySet());

        for (String idToRemove: idsToRemove) {
            parentObservationResourceTypes.remove(idToRemove);
        }
    }
}
