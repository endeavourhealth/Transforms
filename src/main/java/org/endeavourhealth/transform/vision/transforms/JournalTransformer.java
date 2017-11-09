package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.common.fhir.schema.ImmunizationStatus;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceIdMap;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.FieldNotEmptyException;
import org.endeavourhealth.transform.emis.csv.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.terminology.Read2;
import org.endeavourhealth.transform.terminology.TerminologyService;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class JournalTransformer {

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
                case Medication:
                    createOrDeleteMedication(parser, fhirResourceFiler, csvHelper);
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
        potentialResourceTypes.add(ResourceType.Medication);

        for (ResourceType resourceType: potentialResourceTypes) {
            if (wasSavedAsResourceType(fhirResourceFiler, parser, resourceType)) {
                return resourceType;
            }
        }
        return null;
    }

    private static boolean wasSavedAsResourceType(FhirResourceFiler fhirResourceFiler, Journal parser, ResourceType resourceType) throws Exception {
        String uniqueId = VisionCsvHelper.createUniqueId(parser.getPatientID(), parser.getObservationID());
        ResourceIdMap mapping = idMapRepository.getResourceIdMap(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), resourceType.toString(), uniqueId);
        return mapping != null;
    }

    public static void createResource(Journal parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper,
                                       String version) throws Exception {

        //the Read code should NEVER be null, adding this to handle those rows gracefully
        if (parser.getReadCode() == null) {
            return;
        }

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
            case Medication:
                createOrDeleteMedication(parser, fhirResourceFiler, csvHelper);
                break;
            default:
                throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        }

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        //remove any cached links of child observations that link to the row we just processed. If the row used
        //the links, they'll already have been removed. If not, then we can't use them anyway.
        csvHelper.getAndRemoveObservationParentRelationships(observationID, patientID);
    }


    //the FHIR resource type is roughly derived from the code subset and ReadCode
    private static ResourceType getTargetResourceType(Journal parser,
                                                     VisionCsvHelper csvHelper) throws Exception {
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
        if (Read2.isProcedure(readCode)) {
            return ResourceType.Procedure;
        } else if (Read2.isDisorder(readCode) || subset.equalsIgnoreCase("P")) {
            return ResourceType.Condition;
        } else if (subset.equalsIgnoreCase("I")) {
            return ResourceType.Immunization;
        } else if (subset.equalsIgnoreCase("L")) {
            return ResourceType.AllergyIntolerance;
        } else if (subset.equalsIgnoreCase("T")) {
            return ResourceType.Observation;
        } else if ( subset.equalsIgnoreCase("A") ||
                    subset.equalsIgnoreCase("R") ||
                    subset.equalsIgnoreCase("S")) {
            return ResourceType.Medication;
        } else if (Read2.isFamilyHistory(readCode)) {
             return ResourceType.FamilyMemberHistory;
        } else {
            return ResourceType.Observation;
        }
    }

    private static void createOrDeleteMedication(Journal parser,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 VisionCsvHelper csvHelper) throws Exception {

        MedicationStatement fhirMedicationStatement = new MedicationStatement();
        fhirMedicationStatement.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_MEDICATION_AUTHORISATION));

        String drugRecordID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirMedicationStatement, patientID, drugRecordID);

        fhirMedicationStatement.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientID, fhirMedicationStatement);
            return;
        }

        String clinicianID = parser.getClinicianUserID();

        fhirMedicationStatement.setInformationSource(csvHelper.createPractitionerReference(clinicianID));

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirMedicationStatement.setDateAssertedElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        if (parser.getEndDate() == null ) {
            fhirMedicationStatement.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE);
        } else {
            fhirMedicationStatement.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED);
        }

        String dmdId = parser.getDrugDMDCode();
        String term = parser.getRubric();
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, term, dmdId);
        fhirMedicationStatement.setMedication(codeableConcept);

        Double quantity = parser.getValue1();
        String quantityUnit = parser.getValue1NumericUnit();
        Quantity fhirQuantity = new Quantity();
        fhirQuantity.setValue(BigDecimal.valueOf(quantity.doubleValue()));
        fhirQuantity.setUnit(quantityUnit);
        fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_QUANTITY, fhirQuantity));

        String dose = parser.getValue2().toString();
        MedicationStatement.MedicationStatementDosageComponent fhirDose = fhirMedicationStatement.addDosage();
        fhirDose.setText(dose);


        //        //if the Medication is linked to a Problem, then use the problem's Observation as the Medication reason
//        String problemObservationIDs = parser.getLinks();
//        if (!Strings.isNullOrEmpty(problemObservationIDs)) {
//
//            //TODO: loop through record to find cached linked problem
//            //fhirMedicationStatement.setReasonForUse(csvHelper.createConditionReference(problemObservationID, patientID));
//        }
//
//        DateType firstIssueDate = csvHelper.getDrugRecordFirstIssueDate(drugRecordGuid, patientGuid);
//        if (firstIssueDate != null) {
//            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_FIRST_ISSUE_DATE, firstIssueDate));
//        }
//
//        DateType mostRecentDate = csvHelper.getDrugRecordLastIssueDate(drugRecordGuid, patientGuid);
//        if (mostRecentDate != null) {
//            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_MOST_RECENT_ISSUE_DATE, mostRecentDate));
//        }
//
//        String enteredByGuid = parser.getEnteredByUserInRoleGuid();
//        if (!Strings.isNullOrEmpty(enteredByGuid)) {
//            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
//            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_BY, reference));
//        }
//
//        //in the earliest version of the extract, we only got the entered date and not time
//        Date enteredDateTime = parser.getEnteredDateTime();
//        if (enteredDateTime != null) {
//            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
//        }
//
//        String authorisationType = parser.getPrescriptionType();
//        MedicationAuthorisationType fhirAuthorisationType = MedicationAuthorisationType.fromDescription(authorisationType);
//        Coding fhirCoding = CodingHelper.createCoding(fhirAuthorisationType);
//        fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_TYPE, fhirCoding));
//

        addEncounterExtension(fhirMedicationStatement, parser, csvHelper, patientID);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirMedicationStatement);
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
        fhirAllergy.setRecorder(csvHelper.createPractitionerReference(clinicianID));

        Date enteredDate = parser.getEnteredDateTime();
        fhirAllergy.setRecordedDate(enteredDate);

        String readCode = parser.getReadCode();
        String term = parser.getRubric();
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_READ2, term, readCode);
        TerminologyService.translateToSnomed(codeableConcept);
        fhirAllergy.setSubstance(codeableConcept);

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirAllergy.setOnsetElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String associatedText = parser.getAssociatedText();
        fhirAllergy.setNote(AnnotationHelper.createAnnotation(associatedText));

        //TODO: add severity if avail
        String severity = parser.getAllergySeverity();

        //TODO: add certainty if avail
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

        //if the Snomed code exists, pass through the translator to create a full coded concept
        CodeableConcept codeableConcept;
        String snomedCode = parser.getSnomedCode();
        if (!Strings.isNullOrEmpty(snomedCode)) {
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, "", snomedCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        //otherwise, perform a READ to Snomed translation
        else {
            String readCode = parser.getReadCode();
            String term = parser.getRubric();
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_READ2, term, readCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        fhirProcedure.setCode(codeableConcept);

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirProcedure.setPerformed(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String clinicianID = parser.getClinicianUserID();
        Procedure.ProcedurePerformerComponent fhirPerformer = fhirProcedure.addPerformer();
        fhirPerformer.setActor(csvHelper.createPractitionerReference(clinicianID));

        String associatedText = parser.getAssociatedText();
        fhirProcedure.addNotes(AnnotationHelper.createAnnotation(associatedText));

        //TODO:// Encounter link  - The link value is pre-fixed with E  (need example) for an Encounter link
        String [] links = parser.getLinks().split("|");
//        String consultationID = links [X]    //map to an encounterId if prefixed with an E
//        if (!Strings.isNullOrEmpty(consultationID)) {
//            fhirProcedure.setEncounter(csvHelper.createEncounterReference(consultationID, patientID));
//        }

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

        //if the Snomed code exists, pass through the translator to create a full coded concept
        CodeableConcept codeableConcept;
        String snomedCode = parser.getSnomedCode();
        if (!Strings.isNullOrEmpty(snomedCode)) {
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, "", snomedCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        //otherwise, perform a READ to Snomed translation
        else {
            String readCode = parser.getReadCode();
            String term = parser.getRubric();
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_READ2, term, readCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        fhirProblem.setCode(codeableConcept);

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
//        //carry over linked items from any previous instance of this problem
//        List<Reference> previousReferences = EmisCsvHelper.findPreviousLinkedReferences(csvHelper, fhirResourceFiler, fhirProblem.getId(), ResourceType.Condition);
//        if (previousReferences != null && !previousReferences.isEmpty()) {
//            csvHelper.addLinkedItemsToResource(fhirProblem, previousReferences, FhirExtensionUri.PROBLEM_ASSOCIATED_RESOURCE);
//        }
//
//        //apply any linked items from this extract
//        List<String> linkedResources = csvHelper.getAndRemoveProblemRelationships(observationGuid, patientGuid);
//        if (linkedResources != null) {
//            List<Reference> references = ReferenceHelper.createReferences(linkedResources);
//            csvHelper.addLinkedItemsToResource(fhirProblem, references, FhirExtensionUri.PROBLEM_ASSOCIATED_RESOURCE);
//        }

        addDocumentExtension(fhirProblem, parser);

        csvHelper.cacheProblem(observationID, patientID, fhirProblem);

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

        //if the Snomed code exists, pass through the translator to create a full coded concept
        CodeableConcept codeableConcept;
        String snomedCode = parser.getSnomedCode();
        if (!Strings.isNullOrEmpty(snomedCode)) {
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, "", snomedCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        //otherwise, perform a READ to Snomed translation
        else {
            String readCode = parser.getReadCode();
            String term = parser.getRubric();
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_READ2, term, readCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        fhirObservation.setCode(codeableConcept);

        String clinicianID = parser.getClinicianUserID();
        fhirObservation.addPerformer(csvHelper.createPractitionerReference(clinicianID));

        Double value1 = parser.getValue1();
        String units1 = parser.getValue1NumericUnit();
        if (!Strings.isNullOrEmpty(value1.toString())) {
            fhirObservation.setValue(QuantityHelper.createQuantity(value1, units1));
        }

        Double value2 = parser.getValue2();  //only if a special case
        String units2 = parser.getValue2NumericUnit();
        if (!Strings.isNullOrEmpty(value2.toString())) {
            fhirObservation.setValue(QuantityHelper.createQuantity(value2, units2));
        }

        String associatedText = parser.getAssociatedText();
        fhirObservation.setComments(associatedText);

        //TODO:// Encounter link  - The link value is pre-fixed with E  (need example) for an Encounter link
        String [] links = parser.getLinks().split("|");
//        String consultationID = EncounterLinks|    //map to an encounterId
//        if (!Strings.isNullOrEmpty(consultationID)) {
//            fhirObservation.setEncounter(csvHelper.createEncounterReference(consultationID, patientID));
//        }

        //TODO:// Event links
        List<String> childObservations = csvHelper.getAndRemoveObservationParentRelationships(observationID, patientID);
        if (childObservations != null) {
            List<Reference> references = ReferenceHelper.createReferences(childObservations);
            for (Reference reference : references) {
                org.hl7.fhir.instance.model.Observation.ObservationRelatedComponent fhirRelation = fhirObservation.addRelated();
                fhirRelation.setType(org.hl7.fhir.instance.model.Observation.ObservationRelationshipType.HASMEMBER);
                fhirRelation.setTarget(reference);
            }
        }

        //if we have BP readings from child observations, include them in the components for this observation too
        List<org.hl7.fhir.instance.model.Observation.ObservationComponentComponent> observationComponents = csvHelper.findBpComponents(observationID, patientID);
        if (observationComponents != null) {
            for (org.hl7.fhir.instance.model.Observation.ObservationComponentComponent component: observationComponents) {
                fhirObservation.getComponent().add(component);
            }
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

        //if the Snomed code exists, pass through the translator to create a full coded concept
        CodeableConcept codeableConcept;
        String snomedCode = parser.getSnomedCode();
        if (!Strings.isNullOrEmpty(snomedCode)) {
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, "", snomedCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        //otherwise, perform a READ to Snomed translation
        else {
            String readCode = parser.getReadCode();
            String term = parser.getRubric();
            codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_READ2, term, readCode);
            TerminologyService.translateToSnomed(codeableConcept);
        }
        fhirCondition.setCode(codeableConcept);

        String associatedText = parser.getAssociatedText();
        fhirCondition.setNote(AnnotationHelper.createAnnotation(associatedText));

        String clinicianGuid = parser.getClinicianUserID();
        Reference reference = csvHelper.createPractitionerReference(clinicianGuid);
        fhirFamilyHistory.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.FAMILY_MEMBER_HISTORY_REPORTED_BY, reference));

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
        //TODO: lookup code to set status
        fhirImmunisation.setStatus(ImmunizationStatus.COMPLETED.getCode());
        fhirImmunisation.setWasNotGiven(false);
        fhirImmunisation.setReported(false);

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirImmunisation.setDateElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        String readCode = parser.getReadCode();
        String term = parser.getRubric();
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_READ2, term, readCode);
        TerminologyService.translateToSnomed(codeableConcept);
        fhirImmunisation.setVaccineCode(codeableConcept);

        String clinicianID = parser.getClinicianUserID();
        Reference reference = csvHelper.createPractitionerReference(clinicianID);
        fhirImmunisation.setPerformer(reference);

        //TODO:// analyse test data to set the following if present:
        //Source, Compound, Batch, Method, Site, Reason

        //TODO:// Encounter link  - The link value is pre-fixed with E  (need example) for an Encounter link
        String [] links = parser.getLinks().split("|");
//        String consultationID = links [X]    //map to an encounterId if prefixed with an E
//        if (!Strings.isNullOrEmpty(consultationID)) {
//            fhirImmunisation.setEncounter(csvHelper.createEncounterReference(consultationID, patientID));
//        }

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
        //TODO:// Encounter link  - The link value is pre-fixed with E  (need example) for an Encounter link
        String [] links = parser.getLinks().split("|");
//        String consultationID = links [X]    //map to an encounterId if prefixed with an E
//        if (!Strings.isNullOrEmpty(consultationID)) {
//            Reference reference = csvHelper.createEncounterReference(consultationID, patientID);
//            resource.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ASSOCIATED_ENCOUNTER, reference));
//        }
    }

    private static void assertValueEmpty(Resource destinationResource, Journal parser) throws Exception {
        if (parser.getValue1() != null) {
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
}
