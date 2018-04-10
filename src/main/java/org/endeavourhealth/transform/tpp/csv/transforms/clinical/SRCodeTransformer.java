package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.core.terminology.Read2;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.exceptions.FieldNotEmptyException;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.ConditionResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRCode;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.endeavourhealth.core.terminology.Read2.isBPCode;


public class SRCodeTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRCodeTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRCode.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRCode)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRCode parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        ResourceType resourceType = getTargetResourceType(parser, csvHelper);
        switch (resourceType) {
            case Observation:
                createObservation(parser, fhirResourceFiler, csvHelper);
                break;
            case Condition:
                createCondition(parser, fhirResourceFiler, csvHelper);
                break;
            case Procedure:
                createProcedure(parser, fhirResourceFiler, csvHelper);
                break;
//            case AllergyIntolerance:
//                createOrDeleteAllergy(parser, fhirResourceFiler, csvHelper);
//                break;
            case FamilyMemberHistory:
                createFamilyMemberHistory(parser, fhirResourceFiler, csvHelper);
                break;
            default:
                throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        }
    }


//    private static void createAllergy(SRCode parser,
//                                              FhirResourceFiler fhirResourceFiler,
//                                              TppCsvHelper csvHelper) throws Exception {
//
//        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();
//        CsvCell observationID = parser.getObservationID();
//        CsvCell patientID = parser.getPatientID();
//
//        VisionCsvHelper.setUniqueId(allergyIntoleranceBuilder, patientID, observationID);
//
//        allergyIntoleranceBuilder.setPatient(csvHelper.createPatientReference(patientID));
//
//        //if the Resource is to be deleted from the data store, then stop processing the CSV row
//        if (parser.getAction().getString().equalsIgnoreCase("D")) {
//            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
//            return;
//        }
//
//        CsvCell clinicianID = parser.getClinicianUserID();
//        if (!clinicianID.isEmpty()) {
//            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
//            allergyIntoleranceBuilder.setClinician(csvHelper.createPractitionerReference(cleanUserId));
//        }
//
//        CsvCell enteredDate = parser.getEnteredDateTime();
//        allergyIntoleranceBuilder.setRecordedDate(enteredDate.getDate(), enteredDate);
//
//        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(allergyIntoleranceBuilder, null);
//        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
//
//        CsvCell snomedCode = parser.getSnomedCode();
//        if (!snomedCode.isEmpty()) {
//            codeableConceptBuilder.setCodingCode(snomedCode.getString(), snomedCode);
//        }
//        CsvCell term = parser.getRubric();
//        if (!term.isEmpty()) {
//            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
//            codeableConceptBuilder.setText(term.getString(), term);
//        }
//
//        CsvCell effectiveDate = parser.getEffectiveDateTime();
//        String effectiveDatePrecision = "YMD";
//        allergyIntoleranceBuilder.setOnsetDate(EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), effectiveDatePrecision),effectiveDate);
//
//        CsvCell associatedText = parser.getAssociatedText();
//        if (!associatedText.isEmpty()) {
//            allergyIntoleranceBuilder.setNote(associatedText.getString(), associatedText);
//        }
//
//        CsvCell severity = parser.getAllergySeverity();
//        if (!severity.isEmpty()) {
//            AllergyIntolerance.AllergyIntoleranceSeverity allergyIntoleranceSeverity = convertSnomedToAllergySeverity(severity.getString());
//            if (allergyIntoleranceSeverity != null) {
//                allergyIntoleranceBuilder.setSeverity(allergyIntoleranceSeverity, severity);
//            }
//        }
//
//        CsvCell certainty = parser.getAllergyCertainty();
//        if (!certainty.isEmpty()) {
//            AllergyIntolerance.AllergyIntoleranceCertainty allergyIntoleranceCertainty = convertSnomedToAllergyCertainty(certainty.getString());
//            if (allergyIntoleranceCertainty != null) {
//                allergyIntoleranceBuilder.setCertainty(allergyIntoleranceCertainty, certainty);
//            }
//        }
//
//        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
//        if (!Strings.isNullOrEmpty(consultationID)) {
//            Reference reference = csvHelper.createEncounterReference(consultationID, patientID.getString());
//            allergyIntoleranceBuilder.setEncounter(reference, parser.getLinks());
//        }
//
//        CsvCell enteredByID = parser.getClinicianUserID();
//        if (!enteredByID.isEmpty()) {
//            String cleanUserId = csvHelper.cleanUserId(clinicianID.getString());
//            Reference reference = csvHelper.createPractitionerReference(cleanUserId);
//            allergyIntoleranceBuilder.setRecordedBy(reference, enteredByID);
//        }
//
//        String documentId = getDocumentId(parser);
//        if (!Strings.isNullOrEmpty(documentId)) {
//            Identifier fhirDocIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_DOCUMENT_GUID, documentId);
//            allergyIntoleranceBuilder.addDocumentIdentifier(fhirDocIdentifier, parser.getDocumentID());
//        }
//
//        //assert that these cells are empty, as we don't stored them in this resource type
//        assertValueEmpty(allergyIntoleranceBuilder, parser);
//
//        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
//    }

    private static void createProcedure(SRCode parser,
                                        FhirResourceFiler fhirResourceFiler,
                                        TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        TppCsvHelper.setUniqueId(procedureBuilder, patientId, rowId);

        procedureBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            procedureBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell procedureDoneBy = parser.getIDDoneBy();
        if (!procedureDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(procedureDoneBy);
            procedureBuilder.addPerformer(staffReference, procedureDoneBy);
        }

        //status is mandatory, so set the only value we can
        procedureBuilder.setStatus(org.hl7.fhir.instance.model.Procedure.ProcedureStatus.COMPLETED);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            procedureBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            procedureBuilder.setPerformed(dateTimeType, effectiveDate);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(procedureBuilder,  ProcedureBuilder.TAG_CODEABLE_CONCEPT_CODE);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
            codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);

            // translate to Snomed
            SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
            if (snomedCode != null) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                codeableConceptBuilder.setText(snomedCode.getTerm());
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            procedureBuilder.setEncounter (eventReference, eventId);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(procedureBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }


    private static void createCondition(SRCode parser,
                                        FhirResourceFiler fhirResourceFiler,
                                        TppCsvHelper csvHelper) throws Exception {

        CsvCell problemId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        ConditionBuilder conditionBuilder
                = ConditionResourceCache.getConditionBuilder(problemId, patientId, csvHelper, fhirResourceFiler);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        conditionBuilder.setPatient(patientReference, patientId);

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            conditionBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell clinicianDoneBy = parser.getIDDoneBy();
        if (!clinicianDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(clinicianDoneBy);
            conditionBuilder.setClinician(staffReference, clinicianDoneBy);
        }

        //status is mandatory, so set the only value we can
        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            conditionBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            conditionBuilder.setOnset(dateTimeType, effectiveDate);
        }

        //set the category on the condition, so we know it's a problem
        conditionBuilder.setCategory("complaint", problemId);

        boolean isProblem = csvHelper.isProblemObservationGuid(parser.getIDPatient(), parser.getRowIdentifier());
        conditionBuilder.setAsProblem(isProblem);

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(conditionBuilder, ConditionBuilder.TAG_CODEABLE_CONCEPT_CODE);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
            codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);

            // translate to Snomed
            SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
            if (snomedCode != null) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                codeableConceptBuilder.setText(snomedCode.getTerm());
            }
        }

        CsvCell episodeType = parser.getEpisodeType();
        if (!episodeType.isEmpty()) {
            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(episodeType.getLong());
            String mappedTerm = tppMappingRef.getMappedTerm();
            if (!mappedTerm.isEmpty()) {
                conditionBuilder.setEpisodicity(mappedTerm, episodeType);
            }
        }

    }

    private static void createObservation(SRCode parser,
                                          FhirResourceFiler fhirResourceFiler,
                                          TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        ObservationBuilder observationBuilder = new ObservationBuilder();
        TppCsvHelper.setUniqueId(observationBuilder, patientId, rowId);

        observationBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            observationBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell observationDoneBy = parser.getIDDoneBy();
        if (!observationDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(observationDoneBy);
            observationBuilder.setClinician(staffReference, observationDoneBy);
        }

        //status is mandatory, so set the only value we can
        observationBuilder.setStatus(Observation.ObservationStatus.UNKNOWN);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            observationBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            observationBuilder.setEffectiveDate(dateTimeType, effectiveDate);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(observationBuilder, ObservationBuilder.TAG_MAIN_CODEABLE_CONCEPT);

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
            codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);

            // translate to Snomed
            SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
            if (snomedCode != null) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                codeableConceptBuilder.setText(snomedCode.getTerm());
            }
        }

        CsvCell numericValue = parser.getNumericValue();
        if (!numericValue.isEmpty()) {

            observationBuilder.setValueNumber(numericValue.getDouble(), numericValue);
        }

        CsvCell numericUnits = parser.getNumericUnit();
        if (!numericUnits.isEmpty()) {

            observationBuilder.setValueNumberUnits(numericUnits.getString(), numericUnits);
        }

        CsvCell numericComparator = parser.getNumericComparator();
        if (!numericComparator.isEmpty()) {

            Quantity.QuantityComparator comparator = convertComparator(numericComparator.getString());
            if (comparator != null) {
                observationBuilder.setValueNumberComparator(comparator, numericComparator);
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            observationBuilder.setEncounter (eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
    }

    private static void createFamilyMemberHistory(SRCode parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        FamilyMemberHistoryBuilder familyMemberHistoryBuilder = new FamilyMemberHistoryBuilder();
        TppCsvHelper.setUniqueId(familyMemberHistoryBuilder, patientId, rowId);

        familyMemberHistoryBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            familyMemberHistoryBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell observationDoneBy = parser.getIDDoneBy();
        if (!observationDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(observationDoneBy);
            familyMemberHistoryBuilder.setClinician(staffReference, observationDoneBy);
        }

        //status is mandatory, so set the only value we can
        familyMemberHistoryBuilder.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            familyMemberHistoryBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            familyMemberHistoryBuilder.setDate(dateTimeType, effectiveDate);
        }

        //most of the codes are just "FH: xxx" so can't be mapped to a definite family member relationship,
        //so just use the generic family member term
        familyMemberHistoryBuilder.setRelationship(FamilyMember.FAMILY_MEMBER);

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(familyMemberHistoryBuilder, null);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
            codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);

            // translate to Snomed
            SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
            if (snomedCode != null) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                codeableConceptBuilder.setText(snomedCode.getTerm());
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            familyMemberHistoryBuilder.setEncounter (eventReference, eventId);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(familyMemberHistoryBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
    }

    private static void assertValueEmpty(ResourceBuilderBase resourceBuilder, SRCode parser) throws Exception {
        if (!Strings.isNullOrEmpty(parser.getNumericValue().getString())) {
            throw new FieldNotEmptyException("Value", resourceBuilder.getResource());
        }
    }

    //the FHIR resource type is roughly derived from the code subset and ReadCode
    public static ResourceType getTargetResourceType(SRCode parser, TppCsvHelper csvHelper) throws Exception {

        String readV3Code = parser.getCTV3Code().getString();
        if (!Strings.isNullOrEmpty(readV3Code)
                && Read2.isProcedure(readV3Code)
                && !isBPCode(readV3Code)
                && (parser.getNumericValue().isEmpty())) {
            return ResourceType.Procedure;
        } else if ((!Strings.isNullOrEmpty(readV3Code) && Read2.isDisorder(readV3Code)
                || csvHelper.isProblemObservationGuid(parser.getIDPatient(), parser.getRowIdentifier()))) {
            return ResourceType.Condition;
//        } else if (subset.equalsIgnoreCase("L")) {
//            return ResourceType.AllergyIntolerance;
        } else if (!Strings.isNullOrEmpty(readV3Code)
                && Read2.isFamilyHistory(readV3Code)) {
            return ResourceType.FamilyMemberHistory;
        } else {
            return ResourceType.Observation;
        }
    }

    private static Quantity.QuantityComparator convertComparator(String str) {
        if (str.equalsIgnoreCase("<=")) {
            return Quantity.QuantityComparator.LESS_OR_EQUAL;

        } else if (str.equalsIgnoreCase("<")) {
            return Quantity.QuantityComparator.LESS_THAN;

        } else if (str.equalsIgnoreCase(">=")) {
            return Quantity.QuantityComparator.GREATER_OR_EQUAL;

        } else if (str.equalsIgnoreCase(">")) {
            return Quantity.QuantityComparator.GREATER_THAN;

        } else {
            return null;
        }
    }
}
