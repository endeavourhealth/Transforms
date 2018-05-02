package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.common.fhir.schema.ImmunizationStatus;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.core.terminology.Read2;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.exceptions.FieldNotEmptyException;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.endeavourhealth.core.terminology.Read2.isBPCode;


public class JournalTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(JournalTransformer.class);

    private static ResourceIdTransformDalI idMapRepository = DalProvider.factoryResourceIdTransformDal();

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Journal.class);
        while (parser.nextRecord()) {

            try {
                //depending whether deleting or saving, we go through a different path to find what
                //the target resource type should be
                Journal journalParser = (Journal)parser;

                if (journalParser.getAction().getString().equalsIgnoreCase("D")) {
                    deleteResource(journalParser, fhirResourceFiler, csvHelper, version);
                } else {
                    createResource(journalParser, fhirResourceFiler, csvHelper, version);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void deleteResource(Journal parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper,
                                       String version) throws Exception {

        ResourceType resourceType = findOriginalTargetResourceType(fhirResourceFiler, parser);
        if (resourceType != null) {
            switch (resourceType) {
                case Observation:
                    createOrDeleteObservation(parser, fhirResourceFiler, csvHelper);
                    break;
                case Condition:
                    createOrDeleteCondition(parser, fhirResourceFiler, csvHelper);
                    break;
                case Procedure:
                    createOrDeleteProcedure(parser, fhirResourceFiler, csvHelper);
                    break;
                case AllergyIntolerance:
                    createOrDeleteAllergy(parser, fhirResourceFiler, csvHelper);
                    break;
                case FamilyMemberHistory:
                    createOrDeleteFamilyMemberHistory(parser, fhirResourceFiler, csvHelper);
                    break;
                case Immunization:
                    createOrDeleteImmunization(parser, fhirResourceFiler, csvHelper);
                    break;
                case MedicationStatement:
                    createOrDeleteMedicationStatement(parser, fhirResourceFiler, csvHelper);
                    break;
                case MedicationOrder:
                    createOrDeleteMedicationIssue(parser, fhirResourceFiler, csvHelper);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
            }
        }
    }



    /**
     * finds out what resource type an observation was previously saved as
     */
    private static ResourceType findOriginalTargetResourceType(FhirResourceFiler fhirResourceFiler, Journal parser) throws Exception {

        List<ResourceType> potentialResourceTypes = new ArrayList<>();
        potentialResourceTypes.add(ResourceType.Observation);
        potentialResourceTypes.add(ResourceType.Condition);
        potentialResourceTypes.add(ResourceType.Procedure);
        potentialResourceTypes.add(ResourceType.AllergyIntolerance);
        potentialResourceTypes.add(ResourceType.FamilyMemberHistory);
        potentialResourceTypes.add(ResourceType.Immunization);
        potentialResourceTypes.add(ResourceType.MedicationStatement);
        potentialResourceTypes.add(ResourceType.MedicationOrder);

        for (ResourceType resourceType: potentialResourceTypes) {
            if (wasSavedAsResourceType(fhirResourceFiler, parser, resourceType)) {
                return resourceType;
            }
        }
        return null;
    }

    private static boolean wasSavedAsResourceType(FhirResourceFiler fhirResourceFiler, Journal parser, ResourceType resourceType) throws Exception {
        String sourceId = VisionCsvHelper.createUniqueId(parser.getPatientID(), parser.getObservationID());
        Reference sourceReference = ReferenceHelper.createReference(resourceType, sourceId);
        Reference edsReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(sourceReference, fhirResourceFiler);
        return edsReference != null;
    }

    public static void createResource(Journal parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper,
                                       String version) throws Exception {

        //the coded elements should NEVER all be null, adding this to handle those rows gracefully
        if (Strings.isNullOrEmpty(parser.getReadCode().getString())
                && (Strings.isNullOrEmpty(parser.getDrugDMDCode().getString()))
                && (Strings.isNullOrEmpty(parser.getSnomedCode().getString()))) {
            TransformWarnings.log(LOG, parser, "Journal ID: {} contains no coded items", parser.getObservationID());
            return;
        }

        ResourceType resourceType = getTargetResourceType(parser);
        switch (resourceType) {
            case Observation:
                createOrDeleteObservation(parser, fhirResourceFiler, csvHelper);
                break;
            case Condition:
                createOrDeleteCondition(parser, fhirResourceFiler, csvHelper);
                break;
            case Procedure:
                createOrDeleteProcedure(parser, fhirResourceFiler, csvHelper);
                break;
            case AllergyIntolerance:
                createOrDeleteAllergy(parser, fhirResourceFiler, csvHelper);
                break;
            case FamilyMemberHistory:
                createOrDeleteFamilyMemberHistory(parser, fhirResourceFiler, csvHelper);
                break;
            case Immunization:
                createOrDeleteImmunization(parser, fhirResourceFiler, csvHelper);
                break;
            case MedicationStatement:
                createOrDeleteMedicationStatement(parser, fhirResourceFiler, csvHelper);
                break;
            case MedicationOrder:
                createOrDeleteMedicationIssue(parser, fhirResourceFiler, csvHelper);
                break;
            default:
                throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        }
    }

    private static void createOrDeleteMedicationStatement(Journal parser,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 VisionCsvHelper csvHelper) throws Exception {

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();
        CsvCell drugRecordID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(medicationStatementBuilder, patientID, drugRecordID);
        medicationStatementBuilder.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationStatementBuilder);
            return;
        }

        CsvCell clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID.getString())) {
            String cleanClinicianID = csvHelper.cleanUserId(clinicianID.getString());
            medicationStatementBuilder.setInformationSource(csvHelper.createPractitionerReference(cleanClinicianID));
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        DateTimeType date = EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision);
        if (date != null) {
            medicationStatementBuilder.setAssertedDate(date, effectiveDate);
        }

        if (parser.getEndDate() == null) {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE);
        } else {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationStatementBuilder, null);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
        CsvCell dmdId = parser.getDrugDMDCode();
        if (!dmdId.isEmpty()) {
            codeableConceptBuilder.setCodingCode(dmdId.getString(), dmdId);
        }
        CsvCell term = parser.getRubric();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            codeableConceptBuilder.setText(term.getString(), term);
        }

        CsvCell quantity = parser.getValue1();
        if (!quantity.isEmpty()) {
            medicationStatementBuilder.setQuantityValue(quantity.getDouble(), quantity);
        }

        CsvCell quantityUnit = parser.getDrugPrep();
        if (!quantityUnit.isEmpty()) {
            medicationStatementBuilder.setQuantityUnit(quantityUnit.getString(), quantityUnit);
        }

        CsvCell dose = parser.getAssociatedText();
        if (!dose.isEmpty()) {
            medicationStatementBuilder.setDose(dose.getString(), dose);
        }

        CsvCell authorisationType = parser.getDrugPrescriptionType();
        MedicationAuthorisationType fhirAuthorisationType = getMedicationAuthType(authorisationType.getString());
        if (fhirAuthorisationType != null) {
            medicationStatementBuilder.setAuthorisationType(fhirAuthorisationType, authorisationType);
        }

        DateType firstIssueDate = csvHelper.getDrugRecordFirstIssueDate(drugRecordID, patientID);
        if (firstIssueDate != null) {
            medicationStatementBuilder.setFirstIssueDate(firstIssueDate); //, firstIssueDate.getSourceCells());
        }

        DateType mostRecentDate = csvHelper.getDrugRecordLastIssueDate(drugRecordID, patientID);
        if (mostRecentDate != null) {
            medicationStatementBuilder.setLastIssueDate(mostRecentDate); //, mostRecentDate.getSourceCells());
        }

        CsvCell enteredByID = parser.getClinicianUserID();
        if (!enteredByID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(enteredByID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            medicationStatementBuilder.setRecordedBy(reference, enteredByID);
        }

        CsvCell enteredDateTime = parser.getEnteredDateTime();
        if (!enteredDateTime.isEmpty()) {
            medicationStatementBuilder.setRecordedDate(enteredDateTime.getDate(), enteredDateTime);
        }

        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            Reference reference = csvHelper.createEncounterReference(consultationID, patientID.getString());
            medicationStatementBuilder.setEncounter(reference, parser.getLinks());
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }

    private static void createOrDeleteMedicationIssue  (Journal parser,
                                                       FhirResourceFiler fhirResourceFiler,
                                                       VisionCsvHelper csvHelper) throws Exception {

        MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder();
        CsvCell issueRecordID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(medicationOrderBuilder, patientID, issueRecordID);

        medicationOrderBuilder.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationOrderBuilder);
            return;
        }

        CsvCell clinicianID = parser.getClinicianUserID();
        if (!clinicianID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            medicationOrderBuilder.setPrescriber(csvHelper.createPractitionerReference(cleanUserId));
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision);
        medicationOrderBuilder.setDateWritten(dateTime, effectiveDate);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationOrderBuilder, null);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
        CsvCell dmdId = parser.getDrugDMDCode();
        if (!dmdId.isEmpty()) {
            codeableConceptBuilder.setCodingCode(dmdId.getString(), dmdId);
        }
        CsvCell term = parser.getRubric();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
        }

        CsvCell quantity = parser.getValue1();
        if (!quantity.isEmpty()) {
            medicationOrderBuilder.setQuantityValue(quantity.getDouble(), quantity);
        }

        CsvCell quantityUnit = parser.getDrugPrep();
        if (!quantityUnit.isEmpty()) {
            medicationOrderBuilder.setQuantityUnit(quantityUnit.getString(), quantityUnit);
        }

        CsvCell dose = parser.getAssociatedText();
        medicationOrderBuilder.setDose(dose.getString(), dose);

        String links = parser.getLinks().getString();
        if (!Strings.isNullOrEmpty(links)) {
            String drugRecordID = extractDrugRecordLinkID (links, patientID.getString(), csvHelper);
            if (!Strings.isNullOrEmpty(drugRecordID)) {
                 Reference medicationStatementReference = csvHelper.createMedicationStatementReference(drugRecordID, patientID.getString());
                 medicationOrderBuilder.setMedicationStatementReference(medicationStatementReference, parser.getLinks());
            }
        }

        CsvCell enteredByID = parser.getClinicianUserID();
        if (!enteredByID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(enteredByID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            medicationOrderBuilder.setRecordedBy(reference, enteredByID);
        }

        CsvCell enteredDateTime = parser.getEnteredDateTime();
        if (enteredDateTime != null) {
            medicationOrderBuilder.setRecordedDate(enteredDateTime.getDate(), enteredDateTime);
        }

        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            Reference reference = csvHelper.createEncounterReference(consultationID, patientID.getString());
            medicationOrderBuilder.setEncounter(reference, parser.getLinks());
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationOrderBuilder);
    }

    private static void createOrDeleteAllergy(Journal parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              VisionCsvHelper csvHelper) throws Exception {

        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();
        CsvCell observationID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(allergyIntoleranceBuilder, patientID, observationID);

        allergyIntoleranceBuilder.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
            return;
        }

        CsvCell clinicianID = parser.getClinicianUserID();
        if (!clinicianID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            allergyIntoleranceBuilder.setClinician(csvHelper.createPractitionerReference(cleanUserId));
        }

        CsvCell enteredDate = parser.getEnteredDateTime();
        allergyIntoleranceBuilder.setRecordedDate(enteredDate.getDate(), enteredDate);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(allergyIntoleranceBuilder, null);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell snomedCode = parser.getSnomedCode();
        if (!snomedCode.isEmpty()) {
            codeableConceptBuilder.setCodingCode(snomedCode.getString(), snomedCode);
        }
        CsvCell term = parser.getRubric();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            codeableConceptBuilder.setText(term.getString(), term);
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        allergyIntoleranceBuilder.setOnsetDate(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision),effectiveDate);

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            allergyIntoleranceBuilder.setNote(associatedText.getString(), associatedText);
        }

        CsvCell severity = parser.getAllergySeverity();
        if (!severity.isEmpty()) {
            AllergyIntolerance.AllergyIntoleranceSeverity allergyIntoleranceSeverity = convertSnomedToAllergySeverity(severity.getString());
            if (allergyIntoleranceSeverity != null) {
                allergyIntoleranceBuilder.setSeverity(allergyIntoleranceSeverity, severity);
            }
        }

        CsvCell certainty = parser.getAllergyCertainty();
        if (!certainty.isEmpty()) {
            AllergyIntolerance.AllergyIntoleranceCertainty allergyIntoleranceCertainty = convertSnomedToAllergyCertainty(certainty.getString());
            if (allergyIntoleranceCertainty != null) {
                allergyIntoleranceBuilder.setCertainty(allergyIntoleranceCertainty, certainty);
            }
        }

        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            Reference reference = csvHelper.createEncounterReference(consultationID, patientID.getString());
            allergyIntoleranceBuilder.setEncounter(reference, parser.getLinks());
        }

        CsvCell enteredByID = parser.getClinicianUserID();
        if (!enteredByID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            allergyIntoleranceBuilder.setRecordedBy(reference, enteredByID);
        }

        String documentId = getDocumentId(parser);
        if (!Strings.isNullOrEmpty(documentId)) {
            Identifier fhirDocIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
            allergyIntoleranceBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(allergyIntoleranceBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
    }

    private static void createOrDeleteProcedure(Journal parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                VisionCsvHelper csvHelper) throws Exception {

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();

        CsvCell observationID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(procedureBuilder, patientID, observationID);

        procedureBuilder.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
            return;
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder,  ProcedureBuilder.TAG_CODEABLE_CONCEPT_CODE);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell snomedCode = parser.getSnomedCode();
        if (!snomedCode.isEmpty()) {
            codeableConceptBuilder.setCodingCode(snomedCode.getString(), snomedCode);
        }
        CsvCell term = parser.getRubric();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            codeableConceptBuilder.setText(term.getString(), term);
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        procedureBuilder.setPerformed(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision));

        CsvCell clinicianID = parser.getClinicianUserID();
        if (!clinicianID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            procedureBuilder.addPerformer(reference, clinicianID);
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            procedureBuilder.addNotes(associatedText.getString());
        }

        //set linked encounter
        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            procedureBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
        }

        CsvCell getEnteredDateTime = parser.getEnteredDateTime();
        procedureBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        CsvCell enteredByID = parser.getClinicianUserID();
        if (!enteredByID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            procedureBuilder.setRecordedBy(reference, enteredByID);
        }

        String documentId = getDocumentId(parser);
        if (!Strings.isNullOrEmpty(documentId)) {
            Identifier fhirDocIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
            procedureBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(procedureBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }


    private static void createOrDeleteCondition(Journal parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                VisionCsvHelper csvHelper) throws Exception {

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        CsvCell observationID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(conditionBuilder, patientID, observationID);

        conditionBuilder.setPatient(csvHelper.createPatientReference(patientID));

        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            return;
        }

        //set the category on the condition, so we know it's a problem
        conditionBuilder.setCategory("complaint", observationID);
        conditionBuilder.setAsProblem(true);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, ConditionBuilder.TAG_CODEABLE_CONCEPT_CODE);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell snomedCode = parser.getSnomedCode();
        if (!snomedCode.isEmpty()) {
            codeableConceptBuilder.setCodingCode(snomedCode.getString(), snomedCode);
        }
        CsvCell term = parser.getRubric();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            codeableConceptBuilder.setText(term.getString(), term);
        }

        CsvCell comments = parser.getAssociatedText();
        if (!comments.isEmpty()) {
            conditionBuilder.setNotes(comments.getString(),comments);
        }

        CsvCell recordedDate = parser.getEnteredDateTime();
        conditionBuilder.setRecordedDate(recordedDate.getDate(), recordedDate);

        CsvCell endDate = parser.getEndDate();
        if (endDate != null) {
            String endDatePrecision = "YMD";
            DateType dateType = EmisDateTimeHelper.createDateType(endDate.getDate(), endDatePrecision);
            conditionBuilder.setEndDateOrBoolean(dateType, endDate);
        }

        CsvCell episodicityCode = parser.getProblemEpisodicity();
        if (!episodicityCode.isEmpty()) {
            String episodicity = convertEpisodicityCode(episodicityCode.getString());
            conditionBuilder.setEpisodicity(episodicity, episodicityCode);
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        conditionBuilder.setOnset(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision), effectiveDate);

        CsvCell clinicianID = parser.getClinicianUserID();
        if (!clinicianID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            conditionBuilder.setClinician(reference, clinicianID);
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

        //carry over linked items from any previous instance of this problem
        ReferenceList previousReferences = csvHelper.findProblemPreviousLinkedResources(conditionBuilder.getResourceId());
        containedListBuilder.addReferences(previousReferences);

        //apply any linked items from this extract
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewProblemChildren(observationID, patientID);
        containedListBuilder.addReferences(newLinkedResources);

        CsvCell enteredByID = parser.getClinicianUserID();
        if (!enteredByID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            conditionBuilder.setRecordedBy(reference, enteredByID);
        }

        String documentId = getDocumentId(parser);
        if (!Strings.isNullOrEmpty(documentId)) {
            Identifier fhirDocIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
            conditionBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }

    private static void createOrDeleteObservation(Journal parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  VisionCsvHelper csvHelper) throws Exception {

        ObservationBuilder observationBuilder = new ObservationBuilder();

        CsvCell observationID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(observationBuilder, patientID, observationID);

        observationBuilder.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), observationBuilder);
            return;
        }

        //status is mandatory, so set the only value we can
        observationBuilder.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.UNKNOWN);

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        observationBuilder.setEffectiveDate(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision),effectiveDate);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, ObservationBuilder.TAG_MAIN_CODEABLE_CONCEPT);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell snomedCode = parser.getSnomedCode();
        if (!snomedCode.isEmpty()) {
            codeableConceptBuilder.setCodingCode(snomedCode.getString(), snomedCode);
        }
        CsvCell term = parser.getRubric();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            codeableConceptBuilder.setText(term.getString(), term);
        }

        CsvCell clinicianID = parser.getClinicianUserID();
        if (!clinicianID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            observationBuilder.setClinician(reference, clinicianID);
        }

        Double value1 = null;
        String units1 = null;
        Double value2 = null;
        String units2 = null;
        CsvCell value1Name = parser.getValue1Name();
        CsvCell value1AsText = parser.getValue1AsText();
        CsvCell associatedText = parser.getAssociatedText();
        String associatedTextAsStr = associatedText.getString();

        // medication review has text in the value field, so append to associated text
        if (value1Name.getString().equalsIgnoreCase("REVIEW_DAT")) {
            associatedTextAsStr = "Review date: "+value1AsText.getString() + ". " + associatedTextAsStr;
        }
        else {
            //get the numeric values and units
            if (!parser.getValue1().isEmpty()) {
                value1 = parser.getValue1().getDouble();
            }
            units1 = parser.getValue1NumericUnit().getString();

            if (!parser.getValue2().isEmpty()) {
                value2 = parser.getValue2().getDouble();
            }
            units2 = parser.getValue2NumericUnit().getString();
        }

        //BP is a special case - create systolic and diastolic coded components
        if (isBPCode (parser.getReadCode().getString()) && value1 != null && value2 != null) {

            observationBuilder.addComponent();
            observationBuilder.setComponentValue(value1, parser.getValue1());
            observationBuilder.setComponentUnit(units1, parser.getValue1NumericUnit());
            CodeableConceptBuilder comOneCodeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, ObservationBuilder.TAG_COMPONENT_CODEABLE_CONCEPT);
            comOneCodeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            comOneCodeableConceptBuilder.setCodingCode("163030003");
            comOneCodeableConceptBuilder.setCodingDisplay("Systolic blood pressure reading");
            comOneCodeableConceptBuilder.setText("Systolic blood pressure reading");

            observationBuilder.addComponent();
            observationBuilder.setComponentValue(value2, parser.getValue2());
            observationBuilder.setComponentUnit(units2, parser.getValue2NumericUnit());
            CodeableConceptBuilder comTwoCodeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, ObservationBuilder.TAG_COMPONENT_CODEABLE_CONCEPT);
            comTwoCodeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            comTwoCodeableConceptBuilder.setCodingCode("163031004");
            comTwoCodeableConceptBuilder.setCodingDisplay("Diastolic blood pressure reading");
            comTwoCodeableConceptBuilder.setText("Diastolic blood pressure reading");
        }
        else {
            //otherwise, add in the 1st value if it exists
            if (value1 != null) {
                observationBuilder.setValueNumber(value1, parser.getValue1());
            } else if (!value1AsText.isEmpty()){
                observationBuilder.setValueString(value1AsText.getString(), parser.getValue1());
            }

            if (!Strings.isNullOrEmpty(units1)) {
                observationBuilder.setValueNumberUnits(units1, parser.getValue1NumericUnit());
            }

            //the 2nd value only exists if another special case, so add appended to associated text
            String value2NarrativeText = convertSpecialCaseValues(parser);
            if (!Strings.isNullOrEmpty(value2NarrativeText)) {
                associatedTextAsStr = value2NarrativeText.concat(associatedTextAsStr);
            }
        }
        observationBuilder.setNotes(associatedTextAsStr,associatedText);

        //set linked encounter
        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            observationBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
        }

        CsvCell getEnteredDateTime = parser.getEnteredDateTime();
        observationBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        CsvCell enteredByID = parser.getClinicianUserID();
        if (!enteredByID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            observationBuilder.setRecordedBy(reference, enteredByID);
        }

        String documentId = getDocumentId(parser);
        if (!Strings.isNullOrEmpty(documentId)) {
            Identifier fhirDocIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
            observationBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
    }

    private static void createOrDeleteFamilyMemberHistory(Journal parser,
                                                          FhirResourceFiler fhirResourceFiler,
                                                          VisionCsvHelper csvHelper) throws Exception {

        FamilyMemberHistoryBuilder familyMemberHistoryBuilder = new FamilyMemberHistoryBuilder();

        CsvCell observationID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(familyMemberHistoryBuilder, patientID, observationID);

        familyMemberHistoryBuilder.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
            return;
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        familyMemberHistoryBuilder.setDate(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision),effectiveDate);

        //status is mandatory, so set the only possible status we can
        familyMemberHistoryBuilder.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        //most of the codes are just "FH: xxx" so can't be mapped to a definite family member relationship,
        //so just use the generic family member term
        familyMemberHistoryBuilder.setRelationship(FamilyMember.FAMILY_MEMBER);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(familyMemberHistoryBuilder, null);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell snomedCode = parser.getSnomedCode();
        if (!snomedCode.isEmpty()) {
            codeableConceptBuilder.setCodingCode(snomedCode.getString(), snomedCode);
        }
        CsvCell term = parser.getRubric();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            codeableConceptBuilder.setText(term.getString(), term);
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            familyMemberHistoryBuilder.setNotes(associatedText.getString(), associatedText);
        }

        CsvCell clinicianID = parser.getClinicianUserID();
        if (!clinicianID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            familyMemberHistoryBuilder.setClinician(reference, clinicianID);
        }

        //set linked encounter
        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
        if (!consultationID.isEmpty()) {
            familyMemberHistoryBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
        }

        CsvCell getEnteredDateTime = parser.getEnteredDateTime();
        familyMemberHistoryBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        CsvCell enteredByID = parser.getClinicianUserID();
        if (!enteredByID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            familyMemberHistoryBuilder.setRecordedBy(reference, enteredByID);
        }

        String documentId = getDocumentId(parser);
        if (!Strings.isNullOrEmpty(documentId)) {
            Identifier fhirDocIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
            familyMemberHistoryBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(familyMemberHistoryBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
    }

    private static void createOrDeleteImmunization(Journal parser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   VisionCsvHelper csvHelper) throws Exception {

        ImmunizationBuilder immunizationBuilder = new ImmunizationBuilder();

        CsvCell observationID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(immunizationBuilder, patientID, observationID);

        immunizationBuilder.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), immunizationBuilder);
            return;
        }

        //CsvCell status = parser.getImmsStatus();
        //these fields are mandatory so set to what we know
        immunizationBuilder.setStatus(ImmunizationStatus.COMPLETED.getCode()); //we know it was given
        immunizationBuilder.setWasNotGiven(false); //we know it was given
        immunizationBuilder.setReported(false); //assume it was adminsitered by the practice

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        immunizationBuilder.setPerformedDate(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision),effectiveDate);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, ImmunizationBuilder.TAG_VACCINE_CODEABLE_CONCEPT);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell snomedCode = parser.getSnomedCode();
        if (!snomedCode.isEmpty()) {
            codeableConceptBuilder.setCodingCode(snomedCode.getString(), snomedCode);
        }
        CsvCell term = parser.getRubric();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            codeableConceptBuilder.setText(term.getString(), term);
        }

        CsvCell clinicianID = parser.getClinicianUserID();
        if (!clinicianID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            immunizationBuilder.setPerformer(reference, clinicianID);
        }

        //TODO:// analyse test data to set the following if present:
        CsvCell immsSource = parser.getImmsSource();
        CsvCell immsCompound = parser.getImmsCompound();

        CsvCell immsMethod = parser.getImmsMethod();
        CodeableConceptBuilder immsMethodCodeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, ImmunizationBuilder.TAG_ROUTE_CODEABLE_CONCEPT);
        immsMethodCodeableConceptBuilder.setText(immsMethod.getString(), immsMethod);

        CsvCell immsSite = parser.getImmsSite();
        CodeableConceptBuilder immsSiteCodeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, ImmunizationBuilder.TAG_SITE_CODEABLE_CONCEPT);
        immsSiteCodeableConceptBuilder.setText(immsSite.getString(), immsSite);

        CsvCell immsBatch = parser.getImmsBatch();
        immunizationBuilder.setLotNumber(immsBatch.getString(), immsBatch);

        CsvCell immsReason = parser.getImmsReason();
        if (!immsReason.isEmpty()) {
            immunizationBuilder.setReason(immsReason.getString(), immsReason);
        }

        //set linked encounter
        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            immunizationBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
        }

        CsvCell associatedText = parser.getAssociatedText();
        immunizationBuilder.setNote(associatedText.getString(), associatedText);

        CsvCell getEnteredDateTime = parser.getEnteredDateTime();
        immunizationBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        CsvCell enteredByID = parser.getClinicianUserID();
        if (!enteredByID.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            immunizationBuilder.setRecordedBy(reference, enteredByID);
        }

        String documentId = getDocumentId(parser);
        if (!Strings.isNullOrEmpty(documentId)) {
            Identifier fhirDocIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
            immunizationBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(immunizationBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), immunizationBuilder);
    }

    // the consultation encounter link value is pre-fixed with E
    public static String extractEncounterLinkID(String links) {
        if (!Strings.isNullOrEmpty(links)) {
            String[] linkIDs = links.split("[|]");
            for (String link : linkIDs) {
                if (link.startsWith("E")) {
                    return link.replace("E", "");
                }
            }
        }
        return null;
    }

    public static String extractDrugRecordLinkID(String links, String patientID, VisionCsvHelper csvHelper) {
        if (!Strings.isNullOrEmpty(links)) {
            String[] linkIDs = links.split("[|]");
            for (String linkID : linkIDs) {
                if (!linkID.startsWith("E")) {
                    //check if link is an actual drug record previously cached in Drug Pre-transformer
                    if (csvHelper.isDrugRecordGuid(patientID, linkID)) {
                        return linkID;
                    }
                }
            }
        }
        return null;
    }

    // problem links are NOT pre-fixed with an E and exist in the problem observation cache
    public static String extractProblemLinkIDs(String links, String patientID, VisionCsvHelper csvHelper) {
        String problemLinkIDs = "";
        if (!Strings.isNullOrEmpty(links)) {
            String[] linkIDs = links.split("[|]");
            for (String linkID : linkIDs) {
                if (!linkID.startsWith("E")) {
                    //check if link is an actual problem previously cached in Problem Pre-transformer
                    if (csvHelper.isProblemObservationGuid(patientID, linkID)) {
                        problemLinkIDs = problemLinkIDs.concat(linkID + "|");
                    }
                }
            }
        }
        return problemLinkIDs;
    }

    private static void assertValueEmpty(ResourceBuilderBase resourceBuilder, Journal parser) throws Exception {
        if (!Strings.isNullOrEmpty(parser.getValue1AsText().getString())
                && !parser.getValue1Name().getString().equalsIgnoreCase("REVIEW_DAT")) {
            throw new FieldNotEmptyException("Value", resourceBuilder.getResource());
        }
    }

    private static String getDocumentId(Journal parser) {
        String documentIDLinks = parser.getDocumentID().getString();
        if (!Strings.isNullOrEmpty(documentIDLinks)) {
            String[] documentIDs = documentIDLinks.split("[|]");
            if (documentIDs.length > 1) {
                String documentGuid = documentIDs[1];
                if (Strings.isNullOrEmpty(documentGuid)) {
                    return null;
                }
                return documentGuid;
            }
        }
        return null;
    }

    // convert coded item from Read2 or Snomed to full Snomed codeable concept
    private static CodeableConcept createCodeableConcept (Journal parser) throws Exception {
        CodeableConcept codeableConcept = null;
        String snomedCode = parser.getSnomedCode().getString();
        String term = parser.getRubric().getString();
        //if the Snomed code exists with no term, pass through the translator to create a full coded concept
        if (!Strings.isNullOrEmpty(snomedCode)) {
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, term, snomedCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        //otherwise, perform a READ to Snomed translation
        else {
            // after conversation with Vision, if no Snomed code exists, then it's a non coded item, so discard
            return null;
        }
        return codeableConcept;
    }

    // implements Appendix B - Special Cases for observations with two values
    private static String convertSpecialCaseValues(Journal parser) {
        String readCode = parser.getReadCode().getString();
        Double value2 = parser.getValue2().getDouble();

        if (!Strings.isNullOrEmpty(readCode)) {
            //alcohol consumption
            if (readCode.startsWith("136")) {
                if (value2 == null || value2 == 0)
                    return "lifetime teetotaller. ";
                if (value2 == 1)
                    return "current drinker. ";
                if (value2 == 2)
                    return "ex-drinker. ";
            }
            //smoking status
            if (readCode.startsWith("137")) {
                if (value2 == null || value2 == 0)
                    return "never smoked. ";
                if (value2 == 1)
                    return "smoker. ";
                if (value2 == 2)
                    return "ex-smoker. ";
            }
            //weight
            if (readCode.startsWith("22A") && value2 != null) {
                return "BMI: " + value2 + ". ";
            }
            //Alpha Fetoprotein
            if (readCode.startsWith("44B") && value2 != null) {
                return "Weeks Pregnant: " + value2 + ". ";
            }
            //Parity status (could have seperate value 1 and 2 or both)
            if (readCode.startsWith("152")) {
                String out = "";
                Double value1 = parser.getValue1().getDouble();
                if (value1 != null) {
                    out = "Births: " + value1 + ". ";
                }
                if (value2 != null) {
                    out = out.concat("Miscarriages: " + value2 + ". ");
                }
                return out;
            }
        }
        return null;
    }

    // map Snomed code to FHIR allergy severity
    private static AllergyIntolerance.AllergyIntoleranceSeverity convertSnomedToAllergySeverity(String severitySnomedCode) {
        switch (severitySnomedCode) {
            case "255604002" : return AllergyIntolerance.AllergyIntoleranceSeverity.MILD;
            case "6736007" : return AllergyIntolerance.AllergyIntoleranceSeverity.MODERATE;
            case "24484000" : return AllergyIntolerance.AllergyIntoleranceSeverity.SEVERE;
            case "442452003" : return AllergyIntolerance.AllergyIntoleranceSeverity.SEVERE;  //life threatening severity
            default: return null;
        }
    }

    // map Snomed code to FHIR allergy certainty
    private static AllergyIntolerance.AllergyIntoleranceCertainty convertSnomedToAllergyCertainty(String certaintySnomedCode) {
        switch (certaintySnomedCode) {
            case "255545003" : return AllergyIntolerance.AllergyIntoleranceCertainty.CONFIRMED;  //definite
            case "385433004" : return AllergyIntolerance.AllergyIntoleranceCertainty.LIKELY;     //consitent with
            case "385434005" : return AllergyIntolerance.AllergyIntoleranceCertainty.UNLIKELY;   //unlikely
            default: return null;
        }
    }

    // convert Problem/Condition episodicity letter/code to it's name
    private static String convertEpisodicityCode(String episodicityCode) {
        switch (episodicityCode) {
            case "F" : return "First";
            case "N" : return "New";
            case "O" : return "Other";
            case "D" : return "Cause of Death";
            default : return null;
        }
    }

    public static MedicationAuthorisationType getMedicationAuthType(String prescriptionType) {
        /*  A	Acute(one-off issue)
            I	Issue of repeat
            R	Repeat authorisation
        */
        if (prescriptionType.equalsIgnoreCase("A")) {
            return MedicationAuthorisationType.ACUTE;
        }
        else if (prescriptionType.equalsIgnoreCase("R")) {
            return MedicationAuthorisationType.REPEAT;
        }
        else
            return null;
    }

    //the FHIR resource type is roughly derived from the code subset and ReadCode
    public static ResourceType getTargetResourceType(Journal parser) throws Exception {
        String subset = parser.getSubset().getString();
        /*  A = Acute (Therapy)
            R = Repeat
            S = Repeat Issue
            P = Problems
            I = Immunisation
            C = Clinical
            T = Test
            L = Allergy
        */
        String readCode = parser.getReadCode().getString();
        if (!Strings.isNullOrEmpty(readCode)
                && Read2.isProcedure(readCode)
                && !isBPCode(readCode)
                && Strings.isNullOrEmpty(parser.getValue1AsText().getString())
                && !subset.equalsIgnoreCase("T")
                && !subset.equalsIgnoreCase("I")) {
            return ResourceType.Procedure;
        } else if ((!Strings.isNullOrEmpty(readCode) && Read2.isDisorder(readCode))
                || subset.equalsIgnoreCase("P")) {
            return ResourceType.Condition;
        } else if (subset.equalsIgnoreCase("I")) {
            return ResourceType.Immunization;
        } else if (subset.equalsIgnoreCase("L")) {
            return ResourceType.AllergyIntolerance;
        } else if (subset.equalsIgnoreCase("T")) {
            return ResourceType.Observation;
        } else if ( subset.equalsIgnoreCase("A") || subset.equalsIgnoreCase("R")) {
            return ResourceType.MedicationStatement;
        } else if ( subset.equalsIgnoreCase("S")) {
            return ResourceType.MedicationOrder;
        } else if (!Strings.isNullOrEmpty(readCode) && Read2.isFamilyHistory(readCode)) {
            return ResourceType.FamilyMemberHistory;
        } else {
            return ResourceType.Observation;
        }
    }
}
