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
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.endeavourhealth.core.terminology.Read2.isBPCode;


public class JournalTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(JournalTransformer.class);

    private static ResourceIdTransformDalI idMapRepository = DalProvider.factoryResourceIdTransformDal();

    private static final String SYSTOLIC = "2469.";
    private static final String DIASTOLIC = "246A.";

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Journal.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    //depending whether deleting or saving, we go through a different path to find what
                    //the target resource type should be
                    Journal journalParser = (Journal) parser;

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

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void deleteResource(Journal parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper,
                                       String version) throws Exception {

        Set<ResourceType> resourceTypes = findOriginalTargetResourceType(fhirResourceFiler, parser);

        for (ResourceType resourceType: resourceTypes) {
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
    }



    /**
     * finds out what resource type an observation was previously saved as.
     * Potentially saved with multiple resource type mappings, so collect them up and use for deletions
     */
    private static Set<ResourceType> findOriginalTargetResourceType(HasServiceSystemAndExchangeIdI hasServiceId, Journal parser) throws Exception {

        List<ResourceType> potentialResourceTypes = new ArrayList<>();
        potentialResourceTypes.add(ResourceType.Observation);
        potentialResourceTypes.add(ResourceType.Condition);
        potentialResourceTypes.add(ResourceType.Procedure);
        potentialResourceTypes.add(ResourceType.AllergyIntolerance);
        potentialResourceTypes.add(ResourceType.FamilyMemberHistory);
        potentialResourceTypes.add(ResourceType.Immunization);
        potentialResourceTypes.add(ResourceType.MedicationStatement);
        potentialResourceTypes.add(ResourceType.MedicationOrder);

        Set<ResourceType> ret = new HashSet<>();

        for (ResourceType resourceType: potentialResourceTypes) {
            if (wasSavedAsResourceType(hasServiceId, parser, resourceType)) {
                ret.add(resourceType);
            }
        }
        return ret;
    }

    private static boolean wasSavedAsResourceType(HasServiceSystemAndExchangeIdI hasServiceId, Journal parser, ResourceType resourceType) throws Exception {
        String sourceId = VisionCsvHelper.createUniqueId(parser.getPatientID(), parser.getObservationID());

        //fix for VE-6
        UUID uuid = IdHelper.getEdsResourceId(hasServiceId.getServiceId(), resourceType, sourceId);
        return uuid != null;
    }

    public static void createResource(Journal parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper,
                                       String version) throws Exception {

        //the coded elements should NEVER all be null, adding this to handle those rows gracefully
//        if (Strings.isNullOrEmpty(parser.getReadCode().getString())
//                && (Strings.isNullOrEmpty(parser.getDrugDMDCode().getString()))
//                && (Strings.isNullOrEmpty(parser.getSnomedCode().getString()))) {
//            TransformWarnings.log(LOG, parser, "Journal ID: {} contains no coded items", parser.getObservationID());
//            return;
//        }

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
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            medicationStatementBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationStatementBuilder);
            return;
        }

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanClinicianID = csvHelper.cleanUserId(clinicianIdCell.getString());
            medicationStatementBuilder.setInformationSource(csvHelper.createPractitionerReference(cleanClinicianID), clinicianIdCell);
        }

        CsvCell effectiveDateCell = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        DateTimeType date = EmisDateTimeHelper.createDateTimeType(effectiveDateCell.getDate(), effectiveDatePrecision);
        if (date != null) {
            medicationStatementBuilder.setAssertedDate(date, effectiveDateCell);
        }

        //no longer set the active or completed status on medications, just the cancellation date if present
        CsvCell endDateCell = parser.getEndDate();
        if (!endDateCell.isEmpty()) {
            medicationStatementBuilder.setCancellationDate(endDateCell.getDate(), endDateCell);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(medicationStatementBuilder, CodeableConceptBuilder.Tag.Medication_Statement_Drug_Code);

        CsvCell dmdIdCell = parser.getDrugDMDCode();
        CsvCell readCodeCell = parser.getReadCode();
        CsvCell termCell = parser.getRubric();

        //null check because column isn't present in test pack
        if (dmdIdCell != null && !dmdIdCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(dmdIdCell.getString(), dmdIdCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        //add in original Read2 coding
        if (!readCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableConceptBuilder.setCodingCode(readCodeCell.getString().substring(0,5), readCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        CsvCell quantity = parser.getValue1();
        if (!quantity.isEmpty()) {
            medicationStatementBuilder.setQuantityValue(quantity.getDouble(), quantity);
        }

        CsvCell quantityUnitCell = parser.getDrugPrep();
        //null check because cell is missing from test pack
        if (quantityUnitCell != null && !quantityUnitCell.isEmpty()) {
            medicationStatementBuilder.setQuantityUnit(quantityUnitCell.getString(), quantityUnitCell);
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

        CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            medicationStatementBuilder.setRecordedBy(reference, enteredByIdCell);
        }

        CsvCell enteredDateTime = parser.getEnteredDateTime();
        if (!enteredDateTime.isEmpty()) {
            medicationStatementBuilder.setRecordedDate(enteredDateTime.getDate(), enteredDateTime);
        }

        String consultationID = extractEncounterLinkId(parser.getLinks().getString());
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
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            medicationOrderBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationOrderBuilder);
            return;
        }

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            medicationOrderBuilder.setPrescriber(csvHelper.createPractitionerReference(cleanUserId));
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision);
        medicationOrderBuilder.setDateWritten(dateTime, effectiveDate);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationOrderBuilder, CodeableConceptBuilder.Tag.Medication_Order_Drug_Code);

        CsvCell dmdId = parser.getDrugDMDCode();
        CsvCell readCodeCell = parser.getReadCode();
        CsvCell term = parser.getRubric();

        if (!dmdId.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(dmdId.getString(), dmdId);
            if (!term.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            }
        }

        //add in original Read2 coding
        if (!readCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableConceptBuilder.setCodingCode(readCodeCell.getString().substring(0,5), readCodeCell);
            if (!term.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            }
        }

        if (!term.isEmpty()) {
            codeableConceptBuilder.setText(term.getString(), term);
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

        CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            medicationOrderBuilder.setRecordedBy(reference, enteredByIdCell);
        }

        CsvCell enteredDateTime = parser.getEnteredDateTime();
        if (enteredDateTime != null) {
            medicationOrderBuilder.setRecordedDate(enteredDateTime.getDate(), enteredDateTime);
        }

        String consultationID = extractEncounterLinkId(parser.getLinks().getString());
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
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            allergyIntoleranceBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
            return;
        }

        if (isInvalidData(parser)) {
            TransformWarnings.log(LOG, parser, "Journal ID: {} contains invalid Allergy data", parser.getObservationID());
            return;
        }

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            allergyIntoleranceBuilder.setClinician(csvHelper.createPractitionerReference(cleanUserId));
        }

        CsvCell enteredDate = parser.getEnteredDateTime();
        allergyIntoleranceBuilder.setRecordedDate(enteredDate.getDate(), enteredDate);

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(allergyIntoleranceBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code);

        CsvCell snomedCodeCell = parser.getSnomedCode();
        CsvCell termCell = parser.getRubric();
        CsvCell readCodeCell = parser.getReadCode();

        //null check because column is missing from test pack
        if (snomedCodeCell != null && !snomedCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(snomedCodeCell.getString(), snomedCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        //add in original Read2 coding
        if (!readCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableConceptBuilder.setCodingCode(readCodeCell.getString().substring(0,5), readCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        // the item may not be coded, but has a rubric, so set as text
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
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

        String consultationID = extractEncounterLinkId(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            Reference reference = csvHelper.createEncounterReference(consultationID, patientID.getString());
            allergyIntoleranceBuilder.setEncounter(reference, parser.getLinks());
        }

        CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            allergyIntoleranceBuilder.setRecordedBy(reference, enteredByIdCell);
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
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            procedureBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
            return;
        }

        CsvCell snomedCodeCell = parser.getSnomedCode();
        CsvCell readCodeCell = parser.getReadCode();
        CsvCell termCell = parser.getRubric();

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);

        //null check because column is missing from test pack
        if (snomedCodeCell != null && !snomedCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(snomedCodeCell.getString(), snomedCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        //add in original Read2 coding
        if (!readCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableConceptBuilder.setCodingCode(readCodeCell.getString().substring(0,5), readCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        // the item may not be coded, but has a rubric, so set as text
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        procedureBuilder.setPerformed(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision));

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            procedureBuilder.addPerformer(reference, clinicianIdCell);
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            procedureBuilder.addNotes(associatedText.getString());
        }

        //set linked encounter
        String consultationID = extractEncounterLinkId(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            procedureBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
        }

        CsvCell getEnteredDateTime = parser.getEnteredDateTime();
        procedureBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            procedureBuilder.setRecordedBy(reference, enteredByIdCell);
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

        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            conditionBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            return;
        }

        //set the category on the condition, so we know it's a problem
        conditionBuilder.setCategory("complaint", observationID);
        conditionBuilder.setAsProblem(true);

        CsvCell snomedCodeCell = parser.getSnomedCode();
        CsvCell readCodeCell = parser.getReadCode();
        CsvCell termCell = parser.getRubric();

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

        //null check because column is missing from test pack
        if (snomedCodeCell != null && !snomedCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(snomedCodeCell.getString(), snomedCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        //add in original Read2 coding
        if (!readCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableConceptBuilder.setCodingCode(readCodeCell.getString().substring(0,5), readCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        // the item may not be coded, but has a rubric, so set as text
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        CsvCell comments = parser.getAssociatedText();
        if (!comments.isEmpty()) {
            conditionBuilder.setNotes(comments.getString(),comments);
        }

        // no other confirmation status except confirmed
        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

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

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            conditionBuilder.setClinician(reference, clinicianIdCell);
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

        //carry over linked items from any previous instance of this problem
        ReferenceList previousReferences = csvHelper.findProblemPreviousLinkedResources(conditionBuilder.getResourceId());
        containedListBuilder.addReferences(previousReferences);

        //apply any linked items from this extract
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewProblemChildren(observationID, patientID);
        containedListBuilder.addReferences(newLinkedResources);

        CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            conditionBuilder.setRecordedBy(reference, enteredByIdCell);
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
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            observationBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), observationBuilder);
            return;
        }

        //status is mandatory, so set the only value we can
        observationBuilder.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.UNKNOWN);

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        observationBuilder.setEffectiveDate(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision),effectiveDate);

        CsvCell snomedCodeCell = parser.getSnomedCode();
        CsvCell readCodeCell = parser.getReadCode();
        CsvCell termCell = parser.getRubric();

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

        //null check because column is missing from test pack
        if (snomedCodeCell != null && !snomedCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(snomedCodeCell.getString(), snomedCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        //add in original Read2 coding
        if (!readCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableConceptBuilder.setCodingCode(readCodeCell.getString().substring(0,5), readCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        // the item may not be coded, but has a rubric, so set as text
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            observationBuilder.setClinician(reference, clinicianIdCell);
        }

        CsvCell getEnteredDateTime = parser.getEnteredDateTime();
        observationBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            observationBuilder.setRecordedBy(reference, enteredByIdCell);
        }

        Double value1 = null;
        String units1 = null;
        Double value2 = null;
        String units2 = null;
        CsvCell value1NameCell = parser.getValue1Name();
        CsvCell value1AsTextCell = parser.getValue1();
        CsvCell associatedTextCell = parser.getAssociatedText();
        String associatedTextAsStr = associatedTextCell.getString();

        // medication review has text in the value field, so append to associated text
        //null check because column is missing from test pack
        if (value1NameCell != null && value1NameCell.getString().equalsIgnoreCase("REVIEW_DAT")) {
            associatedTextAsStr = "Review date: "+value1AsTextCell.getString() + ". " + associatedTextAsStr;

        } else {
            //get the numeric values and units
            if (!parser.getValue1().isEmpty()) {

                try {
                    value1 = parser.getValue1().getDouble();
                } catch (NumberFormatException ex) {
                    value1 = null;  //set to null to force the use of text value later
                }
            }

            CsvCell unitsCell = parser.getValue1NumericUnit();
            //null check because column is missing from test pack
            if (unitsCell != null) {
                units1 = unitsCell.getString();
            }


            if (!parser.getValue2().isEmpty()) {
                value2 = parser.getValue2().getDouble();
            }

            CsvCell value2UnitCell = parser.getValue2NumericUnit();
            //null check because column is missing from test pack
            if (value2UnitCell != null) {
                units2 = value2UnitCell.getString();
            }
        }

        CsvCell obsEntityCell = parser.getObservationEntity();

        boolean isBP = false;
        ObservationBuilder diastolicObservationBuilder = null;
        ObservationBuilder systolicObservationBuilder = null;

        //BP is a special case - create systolic and diastolic coded components.
        if (!readCodeCell.isEmpty()
                && isBPParentCode(readCodeCell.getString())
                && (value1 != null && value1 > 0) && (value2 != null && value2 > 0)) {

            isBP = true;

            // add the Systolic component to the main observation
            observationBuilder.addComponent();
            observationBuilder.setComponentValue(value1, parser.getValue1());
            observationBuilder.setComponentUnit(units1, parser.getValue1NumericUnit());
            CodeableConceptBuilder comOneCodeableConceptBuilder
                    = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Component_Code);
            comOneCodeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            comOneCodeableConceptBuilder.setCodingCode("163030003");
            comOneCodeableConceptBuilder.setCodingDisplay("Systolic blood pressure reading");
            comOneCodeableConceptBuilder.setText("Systolic blood pressure reading");

            // create a linked Systolic BP observation to the main parent observation
            systolicObservationBuilder = new ObservationBuilder();
            systolicObservationBuilder.setId(observationBuilder.getResource().getId().concat(":SYS"));
            systolicObservationBuilder.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.UNKNOWN);
            systolicObservationBuilder.setEffectiveDate(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision),effectiveDate);
            systolicObservationBuilder.setPatient(csvHelper.createPatientReference(patientID));

            //null check because this cell is missing from the test pack
            if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
                Reference clinicianReferenceSys = csvHelper.createPractitionerReference(csvHelper.cleanUserId(clinicianIdCell.getString()));
                systolicObservationBuilder.setClinician(clinicianReferenceSys, clinicianIdCell);
            }

            //null check because this column is missing from the test pack
            if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
                String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
                Reference reference = csvHelper.createPractitionerReference(cleanUserId);
                systolicObservationBuilder.setRecordedBy(reference, enteredByIdCell);
            }

            CodeableConceptBuilder codeableSystolicConceptBuilder
                    = new CodeableConceptBuilder(systolicObservationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
            codeableSystolicConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableSystolicConceptBuilder.setCodingCode("163030003");
            codeableSystolicConceptBuilder.setCodingDisplay("Systolic blood pressure reading");
            codeableSystolicConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableSystolicConceptBuilder.setCodingCode("2469.");
            codeableSystolicConceptBuilder.setCodingDisplay("O/E - Systolic BP reading");
            codeableSystolicConceptBuilder.setText("Systolic blood pressure reading");

            systolicObservationBuilder.setValueNumber(value1, parser.getValue1());
            systolicObservationBuilder.setValueNumberUnits("mm Hg");
            systolicObservationBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

            Reference parentResource
                    = csvHelper.createObservationReference(observationID.getString(), patientID.getString());
            systolicObservationBuilder.setParentResource(parentResource, observationID);

            // add the Diastolic component to the main observation
            observationBuilder.addComponent();
            observationBuilder.setComponentValue(value2, parser.getValue2());
            observationBuilder.setComponentUnit(units2, parser.getValue2NumericUnit());
            CodeableConceptBuilder comTwoCodeableConceptBuilder
                    = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Component_Code);
            comTwoCodeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            comTwoCodeableConceptBuilder.setCodingCode("163031004");
            comTwoCodeableConceptBuilder.setCodingDisplay("Diastolic blood pressure reading");
            comTwoCodeableConceptBuilder.setText("Diastolic blood pressure reading");

            // create a linked Diastolic BP observation to the main parent observation
            diastolicObservationBuilder = new ObservationBuilder();
            diastolicObservationBuilder.setId(observationBuilder.getResource().getId().concat(":DIA"));
            diastolicObservationBuilder.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.UNKNOWN);
            diastolicObservationBuilder.setEffectiveDate(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision),effectiveDate);
            diastolicObservationBuilder.setPatient(csvHelper.createPatientReference(patientID));

            //null check because this cell is missing from the test pack
            if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
                Reference clinicianReferenceDia = csvHelper.createPractitionerReference(csvHelper.cleanUserId(clinicianIdCell.getString()));
                diastolicObservationBuilder.setClinician(clinicianReferenceDia, clinicianIdCell);
            }

            //null check because this column is missing from the test pack
            if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
                String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
                Reference reference = csvHelper.createPractitionerReference(cleanUserId);
                diastolicObservationBuilder.setRecordedBy(reference, enteredByIdCell);
            }

            CodeableConceptBuilder codeableDistolicConceptBuilder
                    = new CodeableConceptBuilder(diastolicObservationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
            codeableDistolicConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableDistolicConceptBuilder.setCodingCode("163031004");
            codeableDistolicConceptBuilder.setCodingDisplay("Diastolic blood pressure reading");
            codeableDistolicConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableDistolicConceptBuilder.setCodingCode("246A.");
            codeableDistolicConceptBuilder.setCodingDisplay("O/E - Diastolic BP reading");
            codeableDistolicConceptBuilder.setText("Diastolic blood pressure reading");

            diastolicObservationBuilder.setValueNumber(value2, parser.getValue2());
            diastolicObservationBuilder.setValueNumberUnits("mm Hg");
            diastolicObservationBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);
            parentResource
                    = csvHelper.createObservationReference(observationID.getString(), patientID.getString());
            diastolicObservationBuilder.setParentResource(parentResource, observationID);
        }
        else {
            //otherwise, add in the 1st value if it exists
            if (value1 != null) {
                observationBuilder.setValueNumber(value1, parser.getValue1());
            } else if (!value1AsTextCell.isEmpty()){
                //the value becomes a text value if it fails the double numeric conversion earlier
                observationBuilder.setValueString(value1AsTextCell.getString(), parser.getValue1());
            }

            if (!Strings.isNullOrEmpty(units1)) {
                observationBuilder.setValueNumberUnits(units1, parser.getValue1NumericUnit());
            } else if (value1 != null) {
                //if value is set but no units, infer from entity type if possible
                //null check because column is missing from test data
                if (obsEntityCell != null && !obsEntityCell.isEmpty()) {
                    String unitsMapped = getEntityTypeUnits(obsEntityCell.getString());
                    if (unitsMapped != null) {
                        observationBuilder.setValueNumberUnits(unitsMapped);
                    }
                }
            }

            //the 2nd value only exists if another special case, so add appended to associated text
            String value2NarrativeText = convertSpecialCaseValues(parser);
            if (!Strings.isNullOrEmpty(value2NarrativeText)) {
                associatedTextAsStr = value2NarrativeText.concat(associatedTextAsStr);
            }
        }

        //null check because column is missing from test pack
        if (obsEntityCell != null && !obsEntityCell.isEmpty()) {
            String obsEntity = obsEntityCell.getString();
            if (obsEntity.equalsIgnoreCase("LETTERS") || obsEntity.equalsIgnoreCase("ATTACHMENT")) {
                associatedTextAsStr = obsEntity.replace("S", "").concat(". " + associatedTextAsStr);
            }
        }

        observationBuilder.setNotes(associatedTextAsStr,associatedTextCell);

        //set linked encounter
        String consultationID = extractEncounterLinkId(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            observationBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
        }

        String documentId = getDocumentId(parser);
        if (!Strings.isNullOrEmpty(documentId)) {
            Identifier fhirDocIdentifier
                    = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
            observationBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
        }

        if (isBP) {
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder, systolicObservationBuilder, diastolicObservationBuilder);
        } else {
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
        }
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
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            familyMemberHistoryBuilder.setDeletedAudit(actionCell);
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

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(familyMemberHistoryBuilder, CodeableConceptBuilder.Tag.Family_Member_History_Main_Code);

        CsvCell snomedCodeCell = parser.getSnomedCode();
        CsvCell readCodeCell = parser.getReadCode();
        CsvCell termCell = parser.getRubric();

        //null check because code missing from test pack
        if (snomedCodeCell != null && !snomedCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(snomedCodeCell.getString(), snomedCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        //add in original Read2 coding
        if (!readCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableConceptBuilder.setCodingCode(readCodeCell.getString().substring(0,5), readCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        // the item may not be coded, but has a rubric, so set as text
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            familyMemberHistoryBuilder.setNotes(associatedText.getString(), associatedText);
        }

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            familyMemberHistoryBuilder.setClinician(reference, clinicianIdCell);
        }

        //set linked encounter
        String consultationID = extractEncounterLinkId(parser.getLinks().getString());
        if (!consultationID.isEmpty()) {
            familyMemberHistoryBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
        }

        CsvCell getEnteredDateTime = parser.getEnteredDateTime();
        familyMemberHistoryBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            familyMemberHistoryBuilder.setRecordedBy(reference, enteredByIdCell);
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
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            immunizationBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), immunizationBuilder);
            return;
        }

        if (isInvalidData(parser)) {
            TransformWarnings.log(LOG, parser, "Journal ID: {} contains invalid Immunisation data", parser.getObservationID());
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

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Main_Code);

        CsvCell snomedCodeCell = parser.getSnomedCode();
        CsvCell readCodeCell = parser.getReadCode();
        CsvCell termCell = parser.getRubric();

        //null check because column is missing from test pack
        if (snomedCodeCell != null && !snomedCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(snomedCodeCell.getString(), snomedCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            } else {
                // lookup Snomed term for blank immunisation term
                String snomedTerm = TerminologyService.lookupSnomedTerm(snomedCodeCell.getString());
                if (!Strings.isNullOrEmpty(snomedTerm)) {
                    codeableConceptBuilder.setCodingDisplay(snomedTerm);
                    codeableConceptBuilder.setText(snomedTerm);
                }
            }
        }

        //add in original Read2 coding
        if (!readCodeCell.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
            codeableConceptBuilder.setCodingCode(readCodeCell.getString().substring(0,5), readCodeCell);
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);
            }
        }

        // the item may not be coded, but has a rubric, so set as text
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            immunizationBuilder.setPerformer(reference, clinicianIdCell);
        }

        CsvCell immsMethodCell = parser.getImmsMethod();
        //null check because this cell is missing from the test pack
        if (immsMethodCell != null) {
            CodeableConceptBuilder immsMethodCodeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Route);
            immsMethodCodeableConceptBuilder.setText(immsMethodCell.getString(), immsMethodCell);
        }

        CsvCell immsSiteCell = parser.getImmsSite();
        //null check because this cell is missing from the test pack
        if (immsSiteCell != null) {
            CodeableConceptBuilder immsSiteCodeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Site);
            immsSiteCodeableConceptBuilder.setText(immsSiteCell.getString(), immsSiteCell);
        }

        CsvCell immsBatchCell = parser.getImmsBatch();
        //null check because this cell is missing from the test pack
        if (immsBatchCell != null) {
            immunizationBuilder.setLotNumber(immsBatchCell.getString(), immsBatchCell);
        }

        CsvCell immsReasonCell = parser.getImmsReason();
        //null check because this cell is missing from the test pack
        if (immsReasonCell != null && !immsReasonCell.isEmpty()) {
            immunizationBuilder.setReason(immsReasonCell.getString(), immsReasonCell);
        }

        //set linked encounter
        String consultationID = extractEncounterLinkId(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            immunizationBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
        }

        CsvCell associatedText = parser.getAssociatedText();
        String associatedTextStr = associatedText.getString();

        // 'In practice' for example - add to notes
        CsvCell immsSourceCell = parser.getImmsSource();
        //null check because this cell is missing from the test pack
        if (immsSourceCell != null && !immsSourceCell.isEmpty()) {

            associatedTextStr = "Source: "+immsSourceCell.getString()+". "+associatedTextStr;
            immunizationBuilder.setNote(associatedTextStr, associatedText, immsSourceCell);
        } else {

            immunizationBuilder.setNote(associatedTextStr, associatedText);
        }

        // DTIPV for example - add to notes
        CsvCell immsCompoundCell = parser.getImmsCompound();
        //null check because this cell is missing from the test pack
        if (immsCompoundCell != null && !immsCompoundCell.isEmpty()) {

            associatedTextStr = "Compound: "+immsCompoundCell.getString()+". "+associatedTextStr;
            immunizationBuilder.setNote(associatedTextStr, associatedText, immsCompoundCell);
        } else {

            immunizationBuilder.setNote(associatedTextStr, associatedText);
        }

        CsvCell getEnteredDateTime = parser.getEnteredDateTime();
        immunizationBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            immunizationBuilder.setRecordedBy(reference, enteredByIdCell);
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
    public static String extractEncounterLinkId(String links) {
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

    // Advanced have indicated that erroneous data rows could be extracted and sent as part of
    // the extract.  Examples include Immunization records, coded with a 65E..
    // but contain a value code and a class type of WEIGHT or BP.  These records will be
    // logged and the parser row will cease processing that row only
    private static boolean isInvalidData(Journal parser) throws Exception {

        boolean invalid = false;
        ResourceType type = getTargetResourceType(parser);

        //Fixed VEI-4 (invalid Immunisation records).  Also, invalid Allergies detected, so handled those
        if (type == ResourceType.Immunization || type == ResourceType.AllergyIntolerance) {

            // First invalid indicator, the Immunisation or Allergy has a value - this fails the assertValueEmpty test
            String valueText = parser.getValue1().getString();
            if (!Strings.isNullOrEmpty(valueText)) {

                // Second invalid indicator, it contains contains weight or BP data
                CsvCell obsEntityCell = parser.getObservationEntity();
                invalid = obsEntityCell != null //null check required because column missing from test pack
                        && (obsEntityCell.getString().equalsIgnoreCase("WEIGHT")
                        || obsEntityCell.getString().equalsIgnoreCase("BP"));
            }
        }
        return invalid;
    }

    private static void assertValueEmpty(ResourceBuilderBase resourceBuilder, Journal parser) throws Exception {
        if (!Strings.isNullOrEmpty(parser.getValue1().getString())
                && !parser.getValue1Name().getString().equalsIgnoreCase("REVIEW_DAT")) {
            throw new FieldNotEmptyException("Value", resourceBuilder.getResource());
        }
    }

    private static String getDocumentId(Journal parser) {
        CsvCell documentIdCell = parser.getDocumentID();

        //null check because column is missing from test pack
        if (documentIdCell == null || documentIdCell.isEmpty()) {
            return null;
        }

        String documentIDLinks = documentIdCell.getString();
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

    //dervice the implicit value units from the entity type
    private static String getEntityTypeUnits(String entityType) {

        switch (entityType) {
            case "WEIGHT":
                return "kg";
            case "HEIGHT":
                return "m";
            case "PULSE":
                return "bpm";
            case "SMOKING":
                return "per day";
            case "ALCOHOL":
                return "per week";
            case "HEADS_CENTI":
                return "cm";
            case "WAIST":
                return "cm";
            default:
                return null;
        }
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
                && Strings.isNullOrEmpty(parser.getValue1().getString())
                && !subset.equalsIgnoreCase("T")
                && !subset.equalsIgnoreCase("I")) {
            return ResourceType.Procedure;
        } else if (subset.equalsIgnoreCase("P")) {
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

    // is the code a parent BP code and not systolic or diastolic
    public static boolean isBPParentCode(String readCode) {

        readCode = readCode.substring(0,5);  //use 5 byte read
        return isBPCode(readCode) && !readCode.equalsIgnoreCase(DIASTOLIC) && !readCode.equalsIgnoreCase(SYSTOLIC);
    }
}
