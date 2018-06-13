package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
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
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRCode) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(SRCode parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        //TODO - if removedData = true, then we need to work out the target resource type differently, since we don't have the CTV3 code in the current CSV record

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
            case AllergyIntolerance:
                createAllergy(parser, fhirResourceFiler, csvHelper);
                break;
            case FamilyMemberHistory:
                createFamilyMemberHistory(parser, fhirResourceFiler, csvHelper);
                break;
            default:
                throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        }
    }

    public static void createAllergy(SRCode parser,
                                     FhirResourceFiler fhirResourceFiler,
                                     TppCsvHelper csvHelper) throws Exception {


        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if (deleteData != null && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else if (!deleteData.isEmpty() && deleteData.getIntAsBoolean()) {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.AllergyIntolerance allergyIntolerance
                        = (org.hl7.fhir.instance.model.AllergyIntolerance) csvHelper.retrieveResource(rowId.getString(),
                        ResourceType.AllergyIntolerance,
                        fhirResourceFiler);

                if (allergyIntolerance != null) {
                    AllergyIntoleranceBuilder allergyIntoleranceBuilder
                            = new AllergyIntoleranceBuilder(allergyIntolerance);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
                    return;
                }
            }
        }

        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();
        allergyIntoleranceBuilder.setId(rowId.getString(), rowId);

        allergyIntoleranceBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                allergyIntoleranceBuilder.setRecordedBy(staffReference, recordedBy);
            }
        }

        CsvCell procedureDoneBy = parser.getIDDoneBy();
        if (!procedureDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(procedureDoneBy);
            allergyIntoleranceBuilder.setClinician(staffReference, procedureDoneBy);
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            allergyIntoleranceBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            allergyIntoleranceBuilder.setOnsetDate(dateTimeType, effectiveDate);
        }

        allergyIntoleranceBuilder.setStatus(AllergyIntolerance.AllergyIntoleranceStatus.ACTIVE);

        // these are non drug allergies
        allergyIntoleranceBuilder.setCategory(AllergyIntolerance.AllergyIntoleranceCategory.OTHER);

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(allergyIntoleranceBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code);

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
            codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);

            // translate to Snomed if code does not start with "Y" as they are local TPP codes
            if (!readV3Code.getString().startsWith("Y")) {
                SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                    codeableConceptBuilder.setText(snomedCode.getTerm());
                }
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            allergyIntoleranceBuilder.setEncounter(eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
    }

    private static void createProcedure(SRCode parser,
                                        FhirResourceFiler fhirResourceFiler,
                                        TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if (deleteData != null && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else if (!deleteData.isEmpty() && deleteData.getIntAsBoolean()) {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.Procedure procedure
                        = (org.hl7.fhir.instance.model.Procedure) csvHelper.retrieveResource(rowId.getString(),
                        ResourceType.Procedure,
                        fhirResourceFiler);

                if (procedure != null) {
                    ProcedureBuilder procedureBuilder = new ProcedureBuilder(procedure);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
                    return;
                }
            }
        }

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        procedureBuilder.setId(rowId.getString(), rowId);

        procedureBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                procedureBuilder.setRecordedBy(staffReference, recordedBy);
            }
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

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
            codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);

            // translate to Snomed if code does not start with "Y" as they are local TPP codes
            if (!readV3Code.getString().startsWith("Y")) {
                SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                    codeableConceptBuilder.setText(snomedCode.getTerm());
                }
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            procedureBuilder.setEncounter(eventReference, eventId);
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
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if (deleteData != null && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else if (!deleteData.isEmpty() && deleteData.getIntAsBoolean()) {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.Condition condition
                        = (org.hl7.fhir.instance.model.Condition) csvHelper.retrieveResource(problemId.getString(),
                        ResourceType.Condition,
                        fhirResourceFiler);

                if (condition != null) {
                    ConditionBuilder conditionBuilder = new ConditionBuilder(condition);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
                    return;
                }
            }
        }

        //The condition resource will already exist as part of the Problem Transformer, set using the ID value of the code
        ConditionBuilder conditionBuilder
                = ConditionResourceCache.getConditionBuilder(problemId, patientId, csvHelper, fhirResourceFiler);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        conditionBuilder.setPatient(patientReference, patientId);

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                conditionBuilder.setRecordedBy(staffReference, recordedBy);
            }
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
        conditionBuilder.setAsProblem(true);

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {
            // In case we have cached data remove any potentially existing code.
            conditionBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Condition_Main_Code, null);
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
            codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);

            // translate to Snomed if code does not start with "Y" as they are local TPP codes
            if (!readV3Code.getString().startsWith("Y")) {
                SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                    codeableConceptBuilder.setText(snomedCode.getTerm());
                }
            }
        }

        CsvCell episodeType = parser.getEpisodeType();
        if (!episodeType.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(episodeType, parser);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                if (!mappedTerm.isEmpty()) {
                    conditionBuilder.setEpisodicity(mappedTerm, episodeType);
                }
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            conditionBuilder.setEncounter(eventReference, eventId);
        }
    }

    private static void createObservation(SRCode parser,
                                          FhirResourceFiler fhirResourceFiler,
                                          TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if (deleteData != null && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else if ((deleteData != null) && !deleteData.isEmpty() && deleteData.getIntAsBoolean()) {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.Observation observation
                        = (org.hl7.fhir.instance.model.Observation) csvHelper.retrieveResource(rowId.getString(),
                        ResourceType.Observation,
                        fhirResourceFiler);

                if (observation != null) {
                    ObservationBuilder observationBuilder = new ObservationBuilder(observation);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), observationBuilder);
                }
                return;
            }
        }

        ObservationBuilder observationBuilder = new ObservationBuilder();
        observationBuilder.setId(rowId.getString(), rowId);

        observationBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                observationBuilder.setRecordedBy(staffReference, recordedBy);
            }
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

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
            codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);

            // translate to Snomed if code does not start with "Y" as they are local TPP codes
            if (!readV3Code.isEmpty() && !readV3Code.getString().startsWith("Y")) {
                SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                    codeableConceptBuilder.setText(snomedCode.getTerm());
                }
            }
        }

        CsvCell numericValue = parser.getNumericValue();
        if (!numericValue.isEmpty() && parser.getIsNumeric().getBoolean()) {

            observationBuilder.setValueNumber(numericValue.getDouble(), numericValue);
        }

        CsvCell numericUnits = parser.getNumericUnit();
        if (!numericUnits.isEmpty() && parser.getIsNumeric().getBoolean()) {

            observationBuilder.setValueNumberUnits(numericUnits.getString(), numericUnits);
        }

        CsvCell numericComparator = parser.getNumericComparator();
        if (!numericComparator.isEmpty() && parser.getIsNumeric().getBoolean()) {

            Quantity.QuantityComparator comparator = convertComparator(numericComparator.getString());
            if (comparator != null) {
                observationBuilder.setValueNumberComparator(comparator, numericComparator);
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            observationBuilder.setEncounter(eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
    }

    private static void createFamilyMemberHistory(SRCode parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if (deleteData != null && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else if (!deleteData.isEmpty() && deleteData.getIntAsBoolean()) {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.FamilyMemberHistory familyMemberHistory
                        = (org.hl7.fhir.instance.model.FamilyMemberHistory) csvHelper.retrieveResource(rowId.getString(),
                        ResourceType.FamilyMemberHistory,
                        fhirResourceFiler);

                if (familyMemberHistory != null) {
                    FamilyMemberHistoryBuilder familyMemberHistoryBuilder
                            = new FamilyMemberHistoryBuilder(familyMemberHistory);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
                    return;
                }
            }
        }

        FamilyMemberHistoryBuilder familyMemberHistoryBuilder = new FamilyMemberHistoryBuilder();
        familyMemberHistoryBuilder.setId(rowId.getString(), rowId);

        familyMemberHistoryBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                familyMemberHistoryBuilder.setRecordedBy(staffReference, recordedBy);
            }
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

        CsvCell readV3Code = parser.getCTV3Code();
        if (!readV3Code.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(familyMemberHistoryBuilder, CodeableConceptBuilder.Tag.Family_Member_History_Main_Code);

            // add Ctv3 coding
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            codeableConceptBuilder.setCodingCode(readV3Code.getString(), readV3Code);
            CsvCell readV3Term = parser.getCTV3Text();
            if (!readV3Term.isEmpty() && !Strings.isNullOrEmpty(readV3Term.getString())) {
                codeableConceptBuilder.setCodingDisplay(readV3Term.getString(), readV3Term);
                codeableConceptBuilder.setText(readV3Term.getString(), readV3Term);
            }
            // translate to Snomed if code does not start with "Y" as they are local TPP codes
            if (!readV3Code.isEmpty() && !Strings.isNullOrEmpty(readV3Code.getString()) && !readV3Code.getString().startsWith("Y")) {
                SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(readV3Code.getString());
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                    codeableConceptBuilder.setText(snomedCode.getTerm());
                }
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            familyMemberHistoryBuilder.setEncounter(eventReference, eventId);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(familyMemberHistoryBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
    }

    private static void assertValueEmpty(ResourceBuilderBase resourceBuilder, SRCode parser) throws Exception {
        if (!Strings.isNullOrEmpty(parser.getNumericValue().getString())
                && !parser.getIsNumeric().getBoolean()
                && !parser.getNumericValue().getString().equalsIgnoreCase("0.0")) {
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
        } else if (csvHelper.isProblemObservationGuid(parser.getIDPatient(), parser.getRowIdentifier())) {
            return ResourceType.Condition;
        } else if ((!Strings.isNullOrEmpty(readV3Code)
                && csvHelper.isAllergyCode(readV3Code, parser.getCTV3Text().getString()))) {
            return ResourceType.AllergyIntolerance;
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
