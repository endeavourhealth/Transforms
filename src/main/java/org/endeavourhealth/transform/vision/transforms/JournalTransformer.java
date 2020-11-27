package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.common.fhir.schema.ImmunizationStatus;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.core.terminology.Read2;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.exceptions.FieldNotEmptyException;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.helpers.VisionCodeHelper;
import org.endeavourhealth.transform.vision.helpers.VisionDateTimeHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JournalTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(JournalTransformer.class);

    private static ResourceIdTransformDalI idMapRepository = DalProvider.factoryResourceIdTransformDal();

    private static final String SYSTOLIC = "2469.";
    private static final String DIASTOLIC = "246A.";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
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
                        deleteResource(journalParser, fhirResourceFiler, csvHelper);
                    } else {
                        createResource(journalParser, fhirResourceFiler, csvHelper);
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
                                       VisionCsvHelper csvHelper) throws Exception {

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

        String sourceId = VisionCsvHelper.createUniqueId(parser.getPatientID(), parser.getObservationID());

        Set<Reference> sourceReferences = new HashSet<>();
        for (ResourceType resourceType: potentialResourceTypes) {
            Reference ref = ReferenceHelper.createReference(resourceType, sourceId);
            sourceReferences.add(ref);
        }

        Map<Reference, UUID> idMap = IdHelper.getEdsResourceIds(hasServiceId.getServiceId(), sourceReferences);

        Set<ResourceType> ret = new HashSet<>();

        for (Reference ref: sourceReferences) {
            UUID id = idMap.get(ref);
            if (id != null) {
                ResourceType resourceType = ReferenceHelper.getResourceType(ref);
                ret.add(resourceType);
            }
        }

        return ret;
    }

    public static void createResource(Journal parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {

        //common validation
        validateContext(parser);


        ResourceType resourceType = getTargetResourceType(parser, csvHelper);
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

    private static void validateContext(Journal parser) throws Exception {

        //according to Vision the Context cell should always be "A"
        CsvCell contextCell = parser.getContext();
        String context = contextCell.getString();
        if (!context.equals("A")) {
            throw new Exception("CONTEXT cell value not 'A'");
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

        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            medicationStatementBuilder.setAssertedDate(dateTimeType, dateCell, timeCell);
        }

        //no longer set the active or completed status on medications, just the cancellation date if present
        CsvCell endDateCell = parser.getEndDate();
        if (!endDateCell.isEmpty()) {
            medicationStatementBuilder.setCancellationDate(endDateCell.getDate(), endDateCell);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationStatementBuilder, CodeableConceptBuilder.Tag.Medication_Statement_Drug_Code);
        VisionCodeHelper.populateCodeableConcept(true, parser, codeableConceptBuilder, csvHelper);


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

        CsvCell authorisationType = parser.getEpisode();
        MedicationAuthorisationType fhirAuthorisationType = getMedicationAuthType(authorisationType.getString());
        if (fhirAuthorisationType != null) {
            medicationStatementBuilder.setAuthorisationType(fhirAuthorisationType, authorisationType);
        }

        //see SD-105 - this never worked, as there is no link from medication issues to their authorisations
        //once a link is understood, this can be replaced.
        /*DateType firstIssueDate = csvHelper.getDrugRecordFirstIssueDate(drugRecordID, patientID);
        if (firstIssueDate != null) {
            medicationStatementBuilder.setFirstIssueDate(firstIssueDate); //, firstIssueDate.getSourceCells());
        }

        DateType mostRecentDate = csvHelper.getDrugRecordLastIssueDate(drugRecordID, patientID);
        if (mostRecentDate != null) {
            medicationStatementBuilder.setLastIssueDate(mostRecentDate); //, mostRecentDate.getSourceCells());
        }*/

        //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
        /*CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            medicationStatementBuilder.setRecordedBy(reference, enteredByIdCell);
        }*/

        CsvCell enteredDateTime = parser.getEnteredDate();
        if (!enteredDateTime.isEmpty()) {
            medicationStatementBuilder.setRecordedDate(enteredDateTime.getDate(), enteredDateTime);
        }

        CsvCell linksCell = parser.getLinks();
        String consultationId = extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference reference = csvHelper.createEncounterReference(consultationId, patientID.getString());
            medicationStatementBuilder.setEncounter(reference, linksCell);
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

        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            medicationOrderBuilder.setDateWritten(dateTimeType, dateCell, timeCell);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationOrderBuilder, CodeableConceptBuilder.Tag.Medication_Order_Drug_Code);
        VisionCodeHelper.populateCodeableConcept(true, parser, codeableConceptBuilder, csvHelper);


        CsvCell quantity = parser.getValue1();
        if (!quantity.isEmpty()) {
            medicationOrderBuilder.setQuantityValue(quantity.getDouble(), quantity);
        }

        CsvCell quantityUnit = parser.getDrugPrep();
        if (!quantityUnit.isEmpty()) {
            medicationOrderBuilder.setQuantityUnit(quantityUnit.getString(), quantityUnit);
        }

        CsvCell dose = parser.getAssociatedText();
        if (!dose.isEmpty()) {
            medicationOrderBuilder.setDose(dose.getString(), dose);
        }

        //see SD-105 - there is NO link between repeat issues and repeat journal records
        /*CsvCell linkaCell = parser.getLinks();
        String links = linkaCell.getString();
        if (!Strings.isNullOrEmpty(links)) {
            String drugRecordID = extractDrugRecordLinkID (links, patientID.getString(), csvHelper);
            if (!Strings.isNullOrEmpty(drugRecordID)) {
                 Reference medicationStatementReference = csvHelper.createMedicationStatementReference(drugRecordID, patientID.getString());
                 medicationOrderBuilder.setMedicationStatementReference(medicationStatementReference, parser.getLinks());
            }
        }*/

        //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
        /*CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            medicationOrderBuilder.setRecordedBy(reference, enteredByIdCell);
        }*/

        CsvCell enteredDateTime = parser.getEnteredDate();
        if (enteredDateTime != null) {
            medicationOrderBuilder.setRecordedDate(enteredDateTime.getDate(), enteredDateTime);
        }

        CsvCell linksCell = parser.getLinks();
        String consultationId = extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference reference = csvHelper.createEncounterReference(consultationId, patientID.getString());
            medicationOrderBuilder.setEncounter(reference, linksCell);
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

        if (isInvalidData(parser, csvHelper)) {
            TransformWarnings.log(LOG, parser, "Journal ID: {} contains invalid Allergy data", parser.getObservationID());
            return;
        }

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            allergyIntoleranceBuilder.setClinician(csvHelper.createPractitionerReference(cleanUserId));
        }

        CsvCell enteredDate = parser.getEnteredDate();
        allergyIntoleranceBuilder.setRecordedDate(enteredDate.getDate(), enteredDate);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(allergyIntoleranceBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code);
        VisionCodeHelper.populateCodeableConcept(false, parser, codeableConceptBuilder, csvHelper);

        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            allergyIntoleranceBuilder.setOnsetDate(dateTimeType, dateCell, timeCell);
        }

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

        CsvCell linksCell = parser.getLinks();
        String consultationId = extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference reference = csvHelper.createEncounterReference(consultationId, patientID.getString());
            allergyIntoleranceBuilder.setEncounter(reference, linksCell);
        }

        //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
        /*CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            allergyIntoleranceBuilder.setRecordedBy(reference, enteredByIdCell);
        }*/

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

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
        VisionCodeHelper.populateCodeableConcept(false, parser, codeableConceptBuilder, csvHelper);

        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            procedureBuilder.setPerformed(dateTimeType, dateCell, timeCell);
        }

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
        CsvCell linksCell = parser.getLinks();
        String consultationId = extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationId, patientID.getString());
            procedureBuilder.setEncounter(encounterReference, linksCell);
        }

        CsvCell getEnteredDateTime = parser.getEnteredDate();
        procedureBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
        /*CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            procedureBuilder.setRecordedBy(reference, enteredByIdCell);
        }*/

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

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
        VisionCodeHelper.populateCodeableConcept(false, parser, codeableConceptBuilder, csvHelper);

        CsvCell comments = parser.getAssociatedText();
        if (!comments.isEmpty()) {
            conditionBuilder.setNotes(comments.getString(),comments);
        }

        // no other confirmation status except confirmed
        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        CsvCell recordedDate = parser.getEnteredDate();
        conditionBuilder.setRecordedDate(recordedDate.getDate(), recordedDate);

        CsvCell endDateCell = parser.getEndDate();
        if (endDateCell.isEmpty()) {
            DateType dateType = new DateType(endDateCell.getDate(), TemporalPrecisionEnum.DAY);
            conditionBuilder.setEndDateOrBoolean(dateType, endDateCell);
        }

        CsvCell episodicityCode = parser.getEpisode();
        if (!episodicityCode.isEmpty()) {
            String episodicity = convertEpisodicityCode(episodicityCode.getString());
            conditionBuilder.setEpisodicity(episodicity, episodicityCode);
        }

        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            conditionBuilder.setOnset(dateTimeType, dateCell, timeCell);
        }

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

        //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
        /*CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            conditionBuilder.setRecordedBy(reference, enteredByIdCell);
        }*/

        CsvCell linksCell = parser.getLinks();
        String consultationId = extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference reference = csvHelper.createEncounterReference(consultationId, patientID.getString());
            conditionBuilder.setEncounter(reference, linksCell);
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

        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            observationBuilder.setEffectiveDate(dateTimeType, dateCell, timeCell);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
        VisionCodeHelper.populateCodeableConcept(false, parser, codeableConceptBuilder, csvHelper);

        CsvCell clinicianIdCell = parser.getClinicianUserID();
        //null check because this cell is missing from the test pack
        if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            observationBuilder.setClinician(reference, clinicianIdCell);
        }

        CsvCell getEnteredDateTime = parser.getEnteredDate();
        observationBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
        /*CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            observationBuilder.setRecordedBy(reference, enteredByIdCell);
        }*/

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
        CsvCell readCodeCell = parser.getReadCode();
        String readCode = VisionCodeHelper.formatReadCode(readCodeCell, csvHelper);
        if (!Strings.isNullOrEmpty(readCode)
                && isBPParentCode(readCode)
                && (value1 != null && value1.doubleValue() > 0)
                && (value2 != null && value2.doubleValue() > 0)) {

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
            dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
            if (dateTimeType != null) {
                observationBuilder.setEffectiveDate(dateTimeType, dateCell, timeCell);
            }
            systolicObservationBuilder.setPatient(csvHelper.createPatientReference(patientID));

            //null check because this cell is missing from the test pack
            if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
                Reference clinicianReferenceSys = csvHelper.createPractitionerReference(csvHelper.cleanUserId(clinicianIdCell.getString()));
                systolicObservationBuilder.setClinician(clinicianReferenceSys, clinicianIdCell);
            }

            //null check because this column is missing from the test pack
            //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
            /*if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
                String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
                Reference reference = csvHelper.createPractitionerReference(cleanUserId);
                systolicObservationBuilder.setRecordedBy(reference, enteredByIdCell);
            }*/

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
            dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
            if (dateTimeType != null) {
                observationBuilder.setEffectiveDate(dateTimeType, dateCell, timeCell);
            }
            diastolicObservationBuilder.setPatient(csvHelper.createPatientReference(patientID));

            //null check because this cell is missing from the test pack
            if (clinicianIdCell != null && !clinicianIdCell.isEmpty()) {
                Reference clinicianReferenceDia = csvHelper.createPractitionerReference(csvHelper.cleanUserId(clinicianIdCell.getString()));
                diastolicObservationBuilder.setClinician(clinicianReferenceDia, clinicianIdCell);
            }

            //null check because this column is missing from the test pack
            //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
            /*if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
                String cleanUserId = csvHelper.cleanUserId(enteredByIdCell.getString());
                Reference reference = csvHelper.createPractitionerReference(cleanUserId);
                diastolicObservationBuilder.setRecordedBy(reference, enteredByIdCell);
            }*/

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
            String value2NarrativeText = convertSpecialCaseValues(parser, csvHelper);
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
        CsvCell linksCell = parser.getLinks();
        String consultationId = extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationId, patientID.getString());
            observationBuilder.setEncounter(encounterReference, linksCell);
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

        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            familyMemberHistoryBuilder.setDate(dateTimeType, dateCell, timeCell);
        }

        //status is mandatory, so set the only possible status we can
        familyMemberHistoryBuilder.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        //most of the codes are just "FH: xxx" so can't be mapped to a definite family member relationship,
        //so just use the generic family member term
        familyMemberHistoryBuilder.setRelationship(FamilyMember.FAMILY_MEMBER);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(familyMemberHistoryBuilder, CodeableConceptBuilder.Tag.Family_Member_History_Main_Code);
        VisionCodeHelper.populateCodeableConcept(false, parser, codeableConceptBuilder, csvHelper);

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
        CsvCell linksCell = parser.getLinks();
        String consultationId = extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationId, patientID.getString());
            familyMemberHistoryBuilder.setEncounter(encounterReference, linksCell);
        }

        CsvCell getEnteredDateTime = parser.getEnteredDate();
        familyMemberHistoryBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        /*CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            familyMemberHistoryBuilder.setRecordedBy(reference, enteredByIdCell);
        }*/

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

        if (isInvalidData(parser, csvHelper)) {
            TransformWarnings.log(LOG, parser, "Journal ID: {} contains invalid Immunisation data", parser.getObservationID());
            return;
        }

        //SD-217 The IMMS_STATUS column indicates if a vaccination was refused, so DO NOT create a FHIR resource if the code is for a vaccination
        //and the status says it didn't happen
        CsvCell immsStatusCell = parser.getImmsStatus();
        if (immsStatusCell != null && !immsStatusCell.isEmpty()) { //null check because col not in test pack
            String status = immsStatusCell.getString();
            if (status.equalsIgnoreCase("Given")) {
                //if the imm was given, then it's OK

            } else if (status.equalsIgnoreCase("Refusal to start or complete course")
                    || status.equalsIgnoreCase("Advised")) {
                //if the status was one of the other values, then it wasn't given, so if the code is NOT a consent code, then
                //we don't want to keep the FHIR resource. And because there's bad data on the DB, we need to delete any already there
                CsvCell rubricCell = parser.getRubric();
                if (rubricCell.isEmpty() || !rubricCell.getString().toLowerCase().contains("consent")) {
                    immunizationBuilder.setDeletedAudit(immsStatusCell);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), immunizationBuilder);
                    return;
                }

            } else {
                //if a fourth value turns up, we need to know about it immediately
                throw new Exception("Unexpected immunisation status value [" + status + "]");
            }
        }

        //CsvCell status = parser.getImmsStatus();
        //these fields are mandatory so set to what we know
        immunizationBuilder.setStatus(ImmunizationStatus.COMPLETED.getCode()); //we know it was given
        immunizationBuilder.setWasNotGiven(false); //we know it was given
        immunizationBuilder.setReported(false); //assume it was adminsitered by the practice

        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            immunizationBuilder.setPerformedDate(dateTimeType, dateCell, timeCell);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(immunizationBuilder, CodeableConceptBuilder.Tag.Immunization_Main_Code);
        VisionCodeHelper.populateCodeableConcept(false, parser, codeableConceptBuilder, csvHelper);

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
        CsvCell linksCell = parser.getLinks();
        String consultationId = extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationId, patientID.getString());
            immunizationBuilder.setEncounter(encounterReference, linksCell);
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

        CsvCell getEnteredDateTime = parser.getEnteredDate();
        immunizationBuilder.setRecordedDate(getEnteredDateTime.getDate(), getEnteredDateTime);

        //removed (https://endeavourhealth.atlassian.net/browse/SD-220)
        /*CsvCell enteredByIdCell = parser.getClinicianUserID();
        //null check because this column is missing from the test pack
        if (enteredByIdCell != null && !enteredByIdCell.isEmpty()) {
            String cleanUserId = csvHelper.cleanUserId(clinicianIdCell.getString());
            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
            immunizationBuilder.setRecordedBy(reference, enteredByIdCell);
        }*/

        String documentId = getDocumentId(parser);
        if (!Strings.isNullOrEmpty(documentId)) {
            Identifier fhirDocIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
            immunizationBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(immunizationBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), immunizationBuilder);
    }

    /**
     * the LINKS column contains IDs of Journal and Encounter records.
     * Encounter IDs are prefixed with an E and each journal record only has one Encounter link (but we validate this)
     */
    public static String extractEncounterLinkId(CsvCell linksCell) throws Exception {
        if (linksCell.isEmpty()) {
            return null;
        }

        List<String> l = new ArrayList<>();

        String links = linksCell.getString();
        String[] linkIds = links.split("[|]");
        for (String link : linkIds) {
            if (link.startsWith("E")) {
                String sub = link.substring(1);
                if (!Strings.isNullOrEmpty(sub)) {
                    l.add(sub);
                }
            }
        }

        if (l.isEmpty()) {
            return null;

        } else if (l.size() > 1) {
            throw new Exception("More than one encounter ID in journal record");

        } else {
            return l.get(0);
        }
    }

    /**
     * the LINKS column contains IDs of Journal and Encounter records.
     * Encounter IDs are prefixed with an E so we only want the ones that do not have an ID
     */
    public static Set<String> extractJournalLinkIds(CsvCell linksCell) {
        if (linksCell.isEmpty()) {
            return null;
        }

        Set<String> ret = new HashSet<>();

        String links = linksCell.getString();
        String[] linkIds = links.split("[|]");
        for (String link : linkIds) {
            if (!Strings.isNullOrEmpty(link)
                && !link.startsWith("E")) {
                ret.add(link);
            }
        }

        if (ret.isEmpty()) {
            return null;

        } else {
            return ret;
        }
    }

    /*public static String extractDrugRecordLinkID(String links, String patientID, VisionCsvHelper csvHelper) {
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
    }*/

    // Advanced have indicated that erroneous data rows could be extracted and sent as part of
    // the extract.  Examples include Immunization records, coded with a 65E..
    // but contain a value code and a class type of WEIGHT or BP.  These records will be
    // logged and the parser row will cease processing that row only
    private static boolean isInvalidData(Journal parser, VisionCsvHelper csvHelper) throws Exception {

        boolean invalid = false;
        ResourceType type = getTargetResourceType(parser, csvHelper);

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

    // implements Appendix B - Special Cases for observations with two values
    private static String convertSpecialCaseValues(Journal parser, VisionCsvHelper csvHelper) throws Exception {
        CsvCell readCodeCell = parser.getReadCode();
        String readCode = VisionCodeHelper.formatReadCode(readCodeCell, csvHelper);
        if (Strings.isNullOrEmpty(readCode)) {
            return null;
        }

        Double value2 = parser.getValue2().getDouble();

        if (!Strings.isNullOrEmpty(readCode)) {
            //alcohol consumption
            if (readCode.startsWith("136")) {
                if (value2 == null || value2.doubleValue() == 0)
                    return "lifetime teetotaller. ";
                if (value2.doubleValue() == 1)
                    return "current drinker. ";
                if (value2.doubleValue() == 2)
                    return "ex-drinker. ";
            }
            //smoking status
            if (readCode.startsWith("137")) {
                if (value2 == null || value2.doubleValue() == 0)
                    return "never smoked. ";
                if (value2.doubleValue() == 1)
                    return "smoker. ";
                if (value2.doubleValue() == 2)
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
    public static ResourceType getTargetResourceType(Journal parser, VisionCsvHelper csvHelper) throws Exception {
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
        CsvCell readCodeCell = parser.getReadCode();
        String readCode = VisionCodeHelper.formatReadCode(readCodeCell, csvHelper);

        if (!Strings.isNullOrEmpty(readCode)
                && Read2.isProcedure(readCode)
                && !Read2.isBPCode(readCode)
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
    private static boolean isBPParentCode(String readCode) {
        return Read2.isBPCode(readCode) && !readCode.equalsIgnoreCase(DIASTOLIC) && !readCode.equalsIgnoreCase(SYSTOLIC);
    }
}
