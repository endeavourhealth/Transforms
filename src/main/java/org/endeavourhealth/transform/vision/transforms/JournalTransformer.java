package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.FieldNotEmptyException;
import org.endeavourhealth.transform.emis.csv.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.terminology.Read2;
import org.endeavourhealth.transform.terminology.TerminologyService;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.endeavourhealth.transform.terminology.Read2.isBPCode;

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

                if (journalParser.getAction().equalsIgnoreCase("D")) {
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
        if (Strings.isNullOrEmpty(parser.getReadCode())
                && (Strings.isNullOrEmpty(parser.getDrugDMDCode()))
                && (Strings.isNullOrEmpty(parser.getSnomedCode()))) {
            LOG.warn("Journal ID: "+parser.getObservationID()+" contains no coded items");
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

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        //remove any cached links of child observations that link to the row we just processed. If the row used
        //the links, they'll already have been removed. If not, then we can't use them anyway.
        //csvHelper.getAndRemoveObservationParentRelationships(observationID, patientID);
    }


    public static MedicationAuthorisationType getMedicationAuthType(Journal parser) {

        String type = parser.getDrugPrescriptionType();
        /*  A	Acute(one-off issue)
            I	Issue of repeat
            R	Repeat authorisation
        */

        if (type.equalsIgnoreCase("A")) {
            return MedicationAuthorisationType.ACUTE;
        }
        else if (type.equalsIgnoreCase("R")) {
            return MedicationAuthorisationType.REPEAT;
        }
        else
            return null;
    }

    //the FHIR resource type is roughly derived from the code subset and ReadCode
    public static ResourceType getTargetResourceType(Journal parser) throws Exception {
        String subset = parser.getSubset();
        /*  A = Acute (Therapy)
            R = Repeat
            S = Repeat Issue
            P = Problems
            I = Immunisation
            C = Clinical
            T = Test
            L = Allergy
        */
        String readCode = parser.getReadCode();
        if (Read2.isProcedure(readCode) && !Read2.isBPCode(readCode) && !subset.equalsIgnoreCase("T")) {
            return ResourceType.Procedure;
        } else if (Read2.isDisorder(readCode) || subset.equalsIgnoreCase("P")) {
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
        } else if (Read2.isFamilyHistory(readCode)) {
             return ResourceType.FamilyMemberHistory;
        } else {
            return ResourceType.Observation;
        }
    }

    private static void createOrDeleteMedicationStatement(Journal parser,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 VisionCsvHelper csvHelper) throws Exception {

        MedicationStatement fhirMedicationStatement = new MedicationStatement();
        String drugRecordID = parser.getObservationID();
        String patientID = parser.getPatientID();

        fhirMedicationStatement.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_MEDICATION_AUTHORISATION));
        VisionCsvHelper.setUniqueId(fhirMedicationStatement, patientID, drugRecordID);
        fhirMedicationStatement.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirMedicationStatement);
            return;
        }

        String clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            fhirMedicationStatement.setInformationSource(csvHelper.createPractitionerReference(clinicianID));
        }

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirMedicationStatement.setDateAssertedElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        if (parser.getEndDate() == null) {
            fhirMedicationStatement.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE);
        } else {
            fhirMedicationStatement.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED);
        }

        String dmdId = parser.getDrugDMDCode();
        String term = parser.getRubric();
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, term, dmdId);
        fhirMedicationStatement.setMedication(codeableConcept);

        Double quantity = parser.getValue1();
        String quantityUnit = parser.getDrugPrep();
        Quantity fhirQuantity = new Quantity();
        fhirQuantity.setValue(BigDecimal.valueOf(quantity.doubleValue()));
        fhirQuantity.setUnit(quantityUnit);
        fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_QUANTITY, fhirQuantity));

        String dose = parser.getAssociatedText();
        MedicationStatement.MedicationStatementDosageComponent fhirDose = fhirMedicationStatement.addDosage();
        fhirDose.setText(dose);

        MedicationAuthorisationType fhirAuthorisationType = getMedicationAuthType(parser);
        if (fhirAuthorisationType != null) {
            Coding fhirCoding = CodingHelper.createCoding(fhirAuthorisationType);
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_TYPE, fhirCoding));
        }

        //get any linked problems for this medication
        List<String> linkedProblems = csvHelper.getAndRemoveProblemRelationships(drugRecordID, patientID);
        if (linkedProblems != null) {
            List<Reference> references = ReferenceHelper.createReferences(linkedProblems);
            for (Reference ref: references) {
                String problemReferenceId = ref.getId();
                fhirMedicationStatement.setReasonForUse (csvHelper.createConditionReference(problemReferenceId, patientID));
            }
        }

        DateType firstIssueDate = csvHelper.getDrugRecordFirstIssueDate(drugRecordID, patientID);
        if (firstIssueDate != null) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_FIRST_ISSUE_DATE, firstIssueDate));
        }

        DateType mostRecentDate = csvHelper.getDrugRecordLastIssueDate(drugRecordID, patientID);
        if (mostRecentDate != null) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_MOST_RECENT_ISSUE_DATE, mostRecentDate));
        }

        String enteredByID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(enteredByID)) {
            Reference reference = csvHelper.createPractitionerReference(enteredByID);
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_BY, reference));
        }

        Date enteredDateTime = parser.getEnteredDateTime();
        if (enteredDateTime != null) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
        }

        addEncounterExtension(fhirMedicationStatement, parser, csvHelper, patientID);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirMedicationStatement);
    }

    private static void createOrDeleteMedicationIssue  (Journal parser,
                                                       FhirResourceFiler fhirResourceFiler,
                                                       VisionCsvHelper csvHelper) throws Exception {

        MedicationOrder fhirMedicationOrder = new MedicationOrder();
        fhirMedicationOrder.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_MEDICATION_ORDER));

        String issueRecordID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirMedicationOrder, patientID, issueRecordID);

        fhirMedicationOrder.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirMedicationOrder);
            return;
        }

        String clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            fhirMedicationOrder.setPrescriber(csvHelper.createPractitionerReference(clinicianID));
        }

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        fhirMedicationOrder.setDateWrittenElement(dateTime);

        String dmdId = parser.getDrugDMDCode();
        String term = parser.getRubric();
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, term, dmdId);
        fhirMedicationOrder.setMedication(codeableConcept);

        String dose = parser.getAssociatedText();
        MedicationOrder.MedicationOrderDosageInstructionComponent fhirDose = fhirMedicationOrder.addDosageInstruction();
        fhirDose.setText(dose);

        Double quantity = parser.getValue1();
        String quantityUnit = parser.getDrugPrep();
        //Integer courseDuration = parser.getCourseDurationInDays();
        MedicationOrder.MedicationOrderDispenseRequestComponent fhirDispenseRequest = new MedicationOrder.MedicationOrderDispenseRequestComponent();
        fhirDispenseRequest.setQuantity(QuantityHelper.createSimpleQuantity(quantity, quantityUnit));
        //fhirDispenseRequest.setExpectedSupplyDuration(QuantityHelper.createDuration(courseDuration, "days"));
        fhirMedicationOrder.setDispenseRequest(fhirDispenseRequest);

        //get any linked problems for this medication issue
        List<String> linkedProblems = csvHelper.getAndRemoveProblemRelationships(issueRecordID, patientID);
        if (linkedProblems != null) {
            List<Reference> references = ReferenceHelper.createReferences(linkedProblems);
            for (Reference ref: references) {
                String problemReferenceId = ref.getId();
                fhirMedicationOrder.setReason (csvHelper.createConditionReference(problemReferenceId, patientID));
            }
        }

        //TODO: Link issue to drug record - Is the drug record in the links field?
        if (!Strings.isNullOrEmpty(parser.getLinks())) {
            String[] links = parser.getLinks().split("|");
            String drugRecordID = links[0];
            if (!Strings.isNullOrEmpty(drugRecordID)) {
                Reference authorisationReference = csvHelper.createMedicationStatementReference(drugRecordID, patientID);
                fhirMedicationOrder.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_ORDER_AUTHORISATION, authorisationReference));
            }
        }

        String enteredByID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(enteredByID)) {
            Reference reference = csvHelper.createPractitionerReference(enteredByID);
            fhirMedicationOrder.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_BY, reference));
        }

        Date enteredDateTime = parser.getEnteredDateTime();
        if (enteredDateTime != null) {
            fhirMedicationOrder.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirMedicationOrder);
    }

    private static void createOrDeleteAllergy(Journal parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              VisionCsvHelper csvHelper) throws Exception {

        AllergyIntolerance fhirAllergy = new AllergyIntolerance();
        fhirAllergy.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ALLERGY_INTOLERANCE));

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirAllergy, patientID, observationID);

        fhirAllergy.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirAllergy);
            return;
        }

        String clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            fhirAllergy.setRecorder(csvHelper.createPractitionerReference(clinicianID));
        }

        Date enteredDate = parser.getEnteredDateTime();
        fhirAllergy.setRecordedDate(enteredDate);

        //if the Snomed code exists, pass through the translator to create a full coded concept
        CodeableConcept codeableConcept = createCodeableConcept (parser);
        if (codeableConcept != null) {
            fhirAllergy.setSubstance(codeableConcept);
        } else {
            LOG.warn("Unable to create codeableConcept for Allergy ID: "+observationID);
            return;
        }

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirAllergy.setOnsetElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String associatedText = parser.getAssociatedText();
        fhirAllergy.setNote(AnnotationHelper.createAnnotation(associatedText));

        //TODO: add severity if available
        String severity = parser.getAllergySeverity();

        //TODO: add certainty if available
        String certainty = parser.getAllergyCertainty();

        addEncounterExtension(fhirAllergy, parser, csvHelper, patientID);
        addRecordedByExtension(fhirAllergy, parser, csvHelper);
        addDocumentExtension(fhirAllergy, parser);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirAllergy, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirAllergy);
    }

    private static void createOrDeleteProcedure(Journal parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                VisionCsvHelper csvHelper) throws Exception {

        Procedure fhirProcedure = new Procedure();
        fhirProcedure.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PROCEDURE));

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirProcedure, patientID, observationID);

        fhirProcedure.setSubject(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirProcedure);
            return;
        }

        fhirProcedure.setStatus(Procedure.ProcedureStatus.COMPLETED);

        CodeableConcept codeableConcept = createCodeableConcept (parser);
        if (codeableConcept != null) {
            fhirProcedure.setCode(codeableConcept);
        } else {
            LOG.warn("Unable to create codeableConcept for Procedure ID: "+observationID);
            return;
        }

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirProcedure.setPerformed(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            Procedure.ProcedurePerformerComponent fhirPerformer = fhirProcedure.addPerformer();
            fhirPerformer.setActor(csvHelper.createPractitionerReference(clinicianID));
        }

        String associatedText = parser.getAssociatedText();
        fhirProcedure.addNotes(AnnotationHelper.createAnnotation(associatedText));

        //set linked encounter
        String consultationID = extractEncounterLinkID(parser.getLinks());
        if (!Strings.isNullOrEmpty(consultationID)) {
            fhirProcedure.setEncounter(csvHelper.createEncounterReference(consultationID, patientID));
        }

        //the document, entered date and person are stored in extensions
        addRecordedByExtension(fhirProcedure, parser, csvHelper);
        addRecordedDateExtension(fhirProcedure, parser);
        addDocumentExtension(fhirProcedure, parser);

        //addReviewExtension(fhirProcedure, fhirProcedure.getCode(), parser, csvHelper, fhirResourceFiler);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirProcedure, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirProcedure);
    }


    private static void createOrDeleteCondition(Journal parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                VisionCsvHelper csvHelper) throws Exception {

        Condition fhirProblem = new Condition();
        fhirProblem.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PROBLEM));

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirProblem, patientID, observationID);

        fhirProblem.setPatient(csvHelper.createPatientReference(patientID));

        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirProblem);
            return;
        }

        //set the category on the condition, so we know it's a problem
        CodeableConcept cc = new CodeableConcept();
        cc.addCoding().setSystem(FhirValueSetUri.VALUE_SET_CONDITION_CATEGORY).setCode("complaint");
        fhirProblem.setCategory(cc);

        CodeableConcept codeableConcept = createCodeableConcept (parser);
        if (codeableConcept != null) {
            fhirProblem.setCode(codeableConcept);
        } else {
            LOG.warn("Unable to create codeableConcept for Condition ID: "+observationID);
            return;
        }

        String comments = parser.getAssociatedText();
        fhirProblem.setNotes(comments);

        Date endDate = parser.getEndDate();
        if (endDate != null) {
            String endDatePrecision = "YMD";
            fhirProblem.setAbatement(EmisDateTimeHelper.createDateType(endDate, endDatePrecision));
        }

        //TODO: Review Vision files for problem associations
//        Date lastReviewDate = parser.getLastReviewDate();
//        String lastReviewPrecision = parser.getLastReviewDatePrecision();
//        DateType lastReviewDateType = EmisDateTimeHelper.createDateType(lastReviewDate, lastReviewPrecision);
//        String lastReviewedByGuid = parser.getLastReviewUserInRoleGuid();
//        if (lastReviewDateType != null
//                || !Strings.isNullOrEmpty(lastReviewedByGuid)) {
//
//            //the review extension is a compound extension, containing who and when
//            Extension fhirExtension = ExtensionConverter.createCompoundExtension(FhirExtensionUri.PROBLEM_LAST_REVIEWED);
//
//            if (lastReviewDateType != null) {
//                fhirExtension.addExtension(ExtensionConverter.createExtension(FhirExtensionUri._PROBLEM_LAST_REVIEWED__DATE, lastReviewDateType));
//            }
//            if (!Strings.isNullOrEmpty(lastReviewedByGuid)) {
//                fhirExtension.addExtension(ExtensionConverter.createExtension(FhirExtensionUri._PROBLEM_LAST_REVIEWED__PERFORMER, csvHelper.createPractitionerReference(lastReviewedByGuid)));
//            }
//            fhirProblem.addExtension(fhirExtension);
//        }
//
//        ProblemSignificance fhirSignificance = convertSignificance(parser.getSignificanceDescription());
//        CodeableConcept fhirConcept = CodeableConceptHelper.createCodeableConcept(fhirSignificance);
//        fhirProblem.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PROBLEM_SIGNIFICANCE, fhirConcept));
//
//        String parentProblemGuid = parser.getParentProblemObservationGuid();
//        String parentRelationship = parser.getParentProblemRelationship();
//        if (!Strings.isNullOrEmpty(parentProblemGuid)) {
//            ProblemRelationshipType fhirRelationshipType = convertRelationshipType(parentRelationship);
//
//            //this extension is composed of two separate extensions
//            Extension typeExtension = ExtensionConverter.createExtension("type", new StringType(fhirRelationshipType.getCode()));
//            Extension referenceExtension = ExtensionConverter.createExtension("target", csvHelper.createProblemReference(parentProblemGuid, patientGuid));
//            fhirProblem.addExtension(ExtensionConverter.createCompoundExtension(FhirExtensionUri.PROBLEM_RELATED, typeExtension, referenceExtension));
//        }
//

        //carry over linked items from any previous instance of this problem
        List<Reference> previousReferences = VisionCsvHelper.findPreviousLinkedReferences(csvHelper, fhirResourceFiler, fhirProblem.getId(), ResourceType.Condition);
        if (previousReferences != null && !previousReferences.isEmpty()) {
            csvHelper.addLinkedItemsToResource(fhirProblem, previousReferences, FhirExtensionUri.PROBLEM_ASSOCIATED_RESOURCE);
        }

        //apply any linked items from this extract
        List<String> linkedResources = csvHelper.getAndRemoveProblemRelationships(observationID, patientID);
        if (linkedResources != null) {
            List<Reference> references = ReferenceHelper.createReferences(linkedResources);
            csvHelper.addLinkedItemsToResource(fhirProblem, references, FhirExtensionUri.PROBLEM_ASSOCIATED_RESOURCE);
        }

        addDocumentExtension(fhirProblem, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirProblem);
    }

    private static void createOrDeleteObservation(Journal parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  VisionCsvHelper csvHelper) throws Exception {

        org.hl7.fhir.instance.model.Observation fhirObservation = new org.hl7.fhir.instance.model.Observation();
        fhirObservation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_OBSERVATION));

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirObservation, patientID, observationID);

        fhirObservation.setSubject(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirObservation);
            return;
        }

        fhirObservation.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.UNKNOWN);

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        CodeableConcept codeableConcept = createCodeableConcept (parser);
        if (codeableConcept != null) {
            fhirObservation.setCode(codeableConcept);
        } else {
            LOG.warn("Unable to create codeableConcept for Observation ID: "+observationID);
            return;
        }

        String clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            fhirObservation.addPerformer(csvHelper.createPractitionerReference(clinicianID));
        }

        Double value1 = null;
        String units1 = null;
        Double value2 = null;
        String units2 = null;
        String value1Name = parser.getValue1Name();
        String associatedText = parser.getAssociatedText();

        // medication review has text in the value field, so append to associated text instead
        if (value1Name.equalsIgnoreCase("REVIEW_DAT")) {
            String value1AsText = parser.getValue1AsText();
            if (!Strings.isNullOrEmpty(value1AsText)) {
                value1AsText = "Review date: "+value1AsText + ". ";
                associatedText = value1AsText.concat(associatedText);
            }
        }
        else {
            //get the numeric values and units
            value1 = parser.getValue1();
            units1 = parser.getValue1NumericUnit();
            value2 = parser.getValue2();
            units2 = parser.getValue2NumericUnit();
        }

        //BP is a special case - create systolic and diastolic coded components
        if (isBPCode (parser.getReadCode()) && value1 != null && value2 != null) {
            Observation.ObservationComponentComponent componentSystolic = fhirObservation.addComponent();
            componentSystolic.setCode(CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, "", "163030003"));
            componentSystolic.setValue(QuantityHelper.createQuantity(value1, units1));

            Observation.ObservationComponentComponent componentDiastolic = fhirObservation.addComponent();
            componentDiastolic.setCode(CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, "", "163031004"));
            componentDiastolic.setValue(QuantityHelper.createQuantity(value2, units2));
        }
        else {
            //otherwise, add in the 1st value if it exists
            if (value1 != null) {
                fhirObservation.setValue(QuantityHelper.createQuantity(value1, units1));
            }

            //the 2nd value only exists if another special case, so add appended to associated text
            String value2NarrativeText = convertSpecialCaseValues(parser);
            if (!Strings.isNullOrEmpty(value2NarrativeText)) {
                //OPTION 1 - create appended associated text
                associatedText = value2NarrativeText.concat(associatedText);

                //OPTION 2 - create an Observation Narrative
//            if (!Strings.isNullOrEmpty(value2NarrativeText)) {
//                Narrative narrative = new Narrative();
//                narrative.setStatus(Narrative.NarrativeStatus.ADDITIONAL);
//                narrative.setDivAsString("<div>" + value2NarrativeText + "</div>");
//                fhirObservation.setText(narrative);
//            }
            }
        }
        fhirObservation.setComments(associatedText);

        //set linked encounter
        String consultationID = extractEncounterLinkID(parser.getLinks());
        if (!Strings.isNullOrEmpty(consultationID)) {
            fhirObservation.setEncounter(csvHelper.createEncounterReference(consultationID, patientID));
        }

        //the document, entered date and person are stored in extensions
        addDocumentExtension(fhirObservation, parser);
        addRecordedByExtension(fhirObservation, parser, csvHelper);
        addRecordedDateExtension(fhirObservation, parser);

        //addReviewExtension(fhirObservation, fhirObservation.getCode(), parser, csvHelper, fhirResourceFiler);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirObservation);
    }

    private static void createOrDeleteFamilyMemberHistory(Journal parser,
                                                          FhirResourceFiler fhirResourceFiler,
                                                          VisionCsvHelper csvHelper) throws Exception {

        FamilyMemberHistory fhirFamilyHistory = new FamilyMemberHistory();
        fhirFamilyHistory.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_FAMILY_MEMBER_HISTORY));

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirFamilyHistory, patientID, observationID);

        fhirFamilyHistory.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirFamilyHistory);
            return;
        }

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirFamilyHistory.setDateElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        fhirFamilyHistory.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        //most of the codes are just "FH: xxx" so can't be mapped to a definite family member relationship,
        //so just use the generic family member term
        fhirFamilyHistory.setRelationship(CodeableConceptHelper.createCodeableConcept(FamilyMember.FAMILY_MEMBER));

        FamilyMemberHistory.FamilyMemberHistoryConditionComponent fhirCondition = fhirFamilyHistory.addCondition();

        CodeableConcept codeableConcept = createCodeableConcept (parser);
        if (codeableConcept != null) {
            fhirCondition.setCode(codeableConcept);
        } else {
            LOG.warn("Unable to create codeableConcept for Family History ID: "+observationID);
            return;
        }

        String associatedText = parser.getAssociatedText();
        fhirCondition.setNote(AnnotationHelper.createAnnotation(associatedText));

        String clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            Reference reference = csvHelper.createPractitionerReference(clinicianID);
            fhirFamilyHistory.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.FAMILY_MEMBER_HISTORY_REPORTED_BY, reference));
        }

        addEncounterExtension(fhirFamilyHistory, parser, csvHelper, patientID);

        //the entered date and person are stored in extensions
        addRecordedByExtension(fhirFamilyHistory, parser, csvHelper);
        addRecordedDateExtension(fhirFamilyHistory, parser);
        addDocumentExtension(fhirFamilyHistory, parser);

        //addReviewExtension(fhirFamilyHistory, fhirCondition.getCode(), parser, csvHelper, fhirResourceFiler);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirFamilyHistory, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirFamilyHistory);
    }

    private static void createOrDeleteImmunization(Journal parser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   VisionCsvHelper csvHelper) throws Exception {

        Immunization fhirImmunisation = new Immunization();
        fhirImmunisation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_IMMUNIZATION));

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirImmunisation, patientID, observationID);

        fhirImmunisation.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirImmunisation);
            return;
        }

        String status = parser.getImmsStatus();
        fhirImmunisation.setStatus(status);
        fhirImmunisation.setWasNotGiven(false);
        fhirImmunisation.setReported(false);

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirImmunisation.setDateElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        CodeableConcept codeableConcept = createCodeableConcept (parser);
        if (codeableConcept != null) {
            fhirImmunisation.setVaccineCode(codeableConcept);
        } else {
            LOG.warn("Unable to create codeableConcept for Immunisation ID: "+observationID);
            return;
        }

        String clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            Reference reference = csvHelper.createPractitionerReference(clinicianID);
            fhirImmunisation.setPerformer(reference);
        }

        //TODO:// analyse test data to set the following if present:
        String immsSource = parser.getImmsSource();
        String immsCompound = parser.getImmsCompound();

        String immsMethod = parser.getImmsMethod();
        fhirImmunisation.setRoute(new CodeableConcept().setText(immsMethod));

        String immsSite = parser.getImmsSite();
        fhirImmunisation.setSite(new CodeableConcept().setText(immsSite));

        String immsBatch = parser.getImmsBatch();
        fhirImmunisation.setLotNumber(immsBatch);

        Immunization.ImmunizationExplanationComponent immsExplanationComponent = new Immunization.ImmunizationExplanationComponent();
        String immsReason = parser.getImmsReason();
        if (!Strings.isNullOrEmpty(immsReason)) {
            immsExplanationComponent.addReason().setText(immsReason);
        } else {
            immsExplanationComponent.addReasonNotGiven();
        }
        fhirImmunisation.setExplanation(immsExplanationComponent);

        //set linked encounter
        String consultationID = extractEncounterLinkID(parser.getLinks());
        if (!Strings.isNullOrEmpty(consultationID)) {
            fhirImmunisation.setEncounter(csvHelper.createEncounterReference(consultationID, patientID));
        }

        String associatedText = parser.getAssociatedText();
        fhirImmunisation.addNote(AnnotationHelper.createAnnotation(associatedText));

        //the document, entered date and person are stored in extensions
        addRecordedByExtension(fhirImmunisation, parser, csvHelper);
        addRecordedDateExtension(fhirImmunisation, parser);
        addDocumentExtension(fhirImmunisation, parser);

        //addReviewExtension(fhirImmunisation, fhirImmunisation.getVaccineCode(), parser, csvHelper, fhirResourceFiler);

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(fhirImmunisation, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirImmunisation);
    }

    private static void addRecordedByExtension(DomainResource resource, Journal parser, VisionCsvHelper csvHelper) throws Exception {
        String enteredByID = parser.getClinicianUserID();
        if (Strings.isNullOrEmpty(enteredByID)) {
            return;
        }

        Reference reference = csvHelper.createPractitionerReference(enteredByID);
        resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_BY, reference));
    }

    private static void addRecordedDateExtension(DomainResource resource, Journal parser) throws Exception {
        Date enteredDateTime = parser.getEnteredDateTime();
        if (enteredDateTime == null) {
            return;
        }

        resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
    }

    private static void addEncounterExtension(DomainResource resource, Journal parser, VisionCsvHelper csvHelper, String patientID) throws Exception {
        String consultationID = extractEncounterLinkID(parser.getLinks());
        if (!Strings.isNullOrEmpty(consultationID)) {
            Reference reference = csvHelper.createEncounterReference(consultationID, patientID);
            resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ASSOCIATED_ENCOUNTER, reference));
        }
    }

    //The consultation encounter link value is pre-fixed with E
    public static String extractEncounterLinkID(String links) {
        if (!Strings.isNullOrEmpty(links)) {
            String[] linkIDs = links.split("|");
            for (String link : linkIDs) {
                if (link.startsWith("E")) {
                    return link.replace("E", "");
                }
            }
        }
        return null;
    }

    // TODO: if it is a medication issue, how determine linked drug statement? - Asked Vision
    public static String extractDrugRecordLinkID(String links) {
        if (!Strings.isNullOrEmpty(links)) {
            String[] linkIDs = links.split("|");
            for (String link : linkIDs) {
                if (link.startsWith("D")) {
                    return link.replace("D", "");
                }
            }
        }
        return null;
    }

    //problem links are NOT pre-fixed with an E and exist in the problem observation cache
    public static String extractProblemLinkIDs(String links, String patientID, VisionCsvHelper csvHelper) {
        String problemLinkIDs = "";
        if (!Strings.isNullOrEmpty(links)) {
            String[] linkIDs = links.split("|");
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

    private static void assertValueEmpty(Resource destinationResource, Journal parser) throws Exception {
        if (!Strings.isNullOrEmpty(parser.getValue1AsText()) && !parser.getValue1Name().equalsIgnoreCase("REVIEW_DAT")) {
        //if (parser.getValue1() != null) {
            throw new FieldNotEmptyException("Value", destinationResource);
        }
    }

    private static void addReviewExtension(DomainResource resource, CodeableConcept codeableConcept, Observation parser,
																					 VisionCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
//        String problemGuid = parser.getProblemGuid();
//        if (Strings.isNullOrEmpty(problemGuid)) {
//            return;
//        }
//
//        //find the original code our problem was coded with
//        String patientGuid = parser.getPatientGuid();
//        String problemReadCode = csvHelper.findProblemObservationReadCode(patientGuid, problemGuid, fhirResourceFiler);
//        if (Strings.isNullOrEmpty(problemReadCode)) {
//            return;
//        }
//
//        //find the original code our current observation is coded with
//        String observationReadCode = CodeableConceptHelper.findOriginalCode(codeableConcept);
//        if (!problemReadCode.equals(observationReadCode)) {
//            //if the codes differ, then return out
//            return;
//        }

        //if the codes are the same, our current observation is a review of the problem
        Extension extension = ExtensionConverter.createExtension(FhirExtensionUri.IS_REVIEW, new BooleanType(true));
        resource.addExtension(extension);
    }

    private static void addDocumentExtension(DomainResource resource, Journal parser) {
        String documentID = parser.getDocumentID();
        if (!Strings.isNullOrEmpty(documentID)) {
            String[] documentIDs = parser.getDocumentID().split("|");
            String documentGuid = documentIDs[1];
            if (Strings.isNullOrEmpty(documentGuid)) {
                return;
            }
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentGuid);
            resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.EXTERNAL_DOCUMENT, fhirIdentifier));
        }
    }

    // convert coded item from Read2 or Snomed to full Snomed codeable concept
    private static CodeableConcept createCodeableConcept (Journal parser) throws Exception {
        CodeableConcept codeableConcept = null;
        String snomedCode = parser.getSnomedCode();
        String term = parser.getRubric();
        //if the Snomed code exists with no term, pass through the translator to create a full coded concept
        if (!Strings.isNullOrEmpty(snomedCode)) {
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, term, snomedCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        //otherwise, perform a READ to Snomed translation
        else {
            String readCode = parser.getReadCode();
            if (readCode.equalsIgnoreCase("ZZZZZ"))
                return null;
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_READ2, term, readCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        return codeableConcept;
    }

    //implements Appendix B - Special Cases for observations with two values
    private static String convertSpecialCaseValues(Journal parser) {
        String readCode = parser.getReadCode();
        Double value2 = parser.getValue2();

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
                Double value1 = parser.getValue1();
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
}
