package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.terminology.Read2;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.FieldNotEmptyException;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRCode;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.endeavourhealth.core.terminology.Read2.isBPCode;


public class SRCodeTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRCodeTransformer.class);

    private static final String SYSTOLIC = "2469.";
    public static final String DIASTOLIC = "246A.";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRCode.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    CsvCell removedDataCell = ((SRCode) parser).getRemovedData();
                    if (removedDataCell != null && removedDataCell.getIntAsBoolean()) {
                        deleteResource((SRCode) parser, fhirResourceFiler, csvHelper);

                    } else {
                        createResource((SRCode) parser, fhirResourceFiler, csvHelper);
                    }

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void deleteResource(SRCode parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell codeIdCell = parser.getRowIdentifier();
        ResourceType resourceType = wasOriginallySavedAsOtherThanCondition(fhirResourceFiler, codeIdCell);

        if (resourceType != null) {
            switch (resourceType) {
                case Observation:
                    createOrDeleteObservation(parser, fhirResourceFiler, csvHelper);
                    break;
                //conditions are checked at the bottom of this fn
                /*case Condition:
                    createOrDeleteCondition(parser, fhirResourceFiler, csvHelper);
                    break;*/
                case Procedure:
                    createOrDeleteProcedure(parser, fhirResourceFiler, csvHelper);
                    break;
                case AllergyIntolerance:
                    createOrDeleteAllergy(parser, fhirResourceFiler, csvHelper);
                    break;
                case FamilyMemberHistory:
                    createOrDeleteFamilyMemberHistory(parser, fhirResourceFiler, csvHelper);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
            }
        }

        //if we originally saved our record as a Condition, either because the record code indicated it should be
        //condition or because we had a SRProblem record linked to us (in which case it will have been saved as
        //a condition AS WELL as the other resource type), then we'll need to delete that condition now
        if (wasOriginallySavedAsCondition(fhirResourceFiler, codeIdCell)) {
            createOrDeleteCondition(parser, fhirResourceFiler, csvHelper);
        }
    }

    private static void createResource(SRCode parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

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
            default:
                throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        }

        //if we've saved our record as something other than a condition, but we have a SRProblem record linked
        //to our record, then we ALSO want to save this record as a condition
        CsvCell codeIdCell = parser.getRowIdentifier();
        if (resourceType != ResourceType.Condition
                && csvHelper.getConditionResourceCache().containsCondition(codeIdCell)) {

            createOrDeleteCondition(parser, fhirResourceFiler, csvHelper);
        }
    }

    private static void createOrDeleteAllergy(SRCode parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              TppCsvHelper csvHelper) throws Exception {


        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            AllergyIntolerance allergyIntolerance = (AllergyIntolerance) csvHelper.retrieveResource(rowId.getString(), ResourceType.AllergyIntolerance);
            if (allergyIntolerance != null) {
                AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder(allergyIntolerance);
                allergyIntoleranceBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, allergyIntoleranceBuilder);
            }
            return;
        }

        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();
        allergyIntoleranceBuilder.setId(rowId.getString(), rowId);

        allergyIntoleranceBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            allergyIntoleranceBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            allergyIntoleranceBuilder.setClinician(staffReference, staffMemberIdDoneBy);
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            allergyIntoleranceBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            allergyIntoleranceBuilder.setOnsetDate(dateTimeType, effectiveDate);
        }

        allergyIntoleranceBuilder.setStatus(AllergyIntolerance.AllergyIntoleranceStatus.ACTIVE);

        // these are non drug allergies
        allergyIntoleranceBuilder.setCategory(AllergyIntolerance.AllergyIntoleranceCategory.OTHER);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(allergyIntoleranceBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code);
        CsvCell snomedCodeCell = parser.getSNOMEDCode();
        CsvCell snomedDescCell = parser.getSNOMEDText();
        CsvCell ctv3CodeCell = parser.getCTV3Code();
        CsvCell ctv3DescCell = parser.getCTV3Text();
        addCodeableConcept(codeableConceptBuilder, snomedCodeCell, snomedDescCell, ctv3CodeCell, ctv3DescCell);

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {
            Reference eventReference = csvHelper.createEncounterReference(eventId);
            allergyIntoleranceBuilder.setEncounter(eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
    }

    private static void addCodeableConcept(CodeableConceptBuilder codeableConceptBuilder, CsvCell snomedCodeCell, CsvCell snomedDescCell, CsvCell ctv3CodeCell, CsvCell ctv3DescCell) throws Exception {

        boolean addedSnomed = false;

        if (snomedCodeCell != null //might be null in older versions
                && !snomedCodeCell.isEmpty()
                && snomedCodeCell.getString().equals("-1")) {

            SnomedCode snomedCode = TerminologyService.lookupSnomedFromConceptId(snomedCodeCell.getString());
            if (snomedCode != null) {
                addSnomedToCodeableConcept(codeableConceptBuilder, snomedCode);
                addedSnomed = true;
            }
        }

        if (!ctv3CodeCell.isEmpty()) {
            String code = ctv3CodeCell.getString();
            if (!code.startsWith("Y")) {

                //only add the snomed translation if not already added snomed
                if (!addedSnomed) {
                    SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(code);
                    if (snomedCode != null) {
                        addSnomedToCodeableConcept(codeableConceptBuilder, snomedCode);
                    }
                }

                // add Ctv3 coding
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);

            } else {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_TPP_CTV3);
            }

            codeableConceptBuilder.setCodingCode(code, ctv3CodeCell);
            codeableConceptBuilder.setCodingDisplay(ctv3DescCell.getString(), ctv3DescCell);
        }

        //set the text from one of the text cells, perferring Snomed is present
        if (snomedDescCell != null
                && !snomedDescCell.isEmpty()) {

            codeableConceptBuilder.setText(snomedDescCell.getString(), snomedDescCell);

        } else {
            codeableConceptBuilder.setText(ctv3DescCell.getString(), ctv3DescCell);
        }
    }


    private static void addSnomedToCodeableConcept(CodeableConceptBuilder codeableConceptBuilder, SnomedCode snomedCode) {
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
        codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
        codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
    }

    private static void createOrDeleteProcedure(SRCode parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            Procedure procedure = (Procedure) csvHelper.retrieveResource(rowId.getString(), ResourceType.Procedure);
            if (procedure != null) {
                ProcedureBuilder procedureBuilder = new ProcedureBuilder(procedure);
                procedureBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
            }
            return;
        }

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        procedureBuilder.setId(rowId.getString(), rowId);

        procedureBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            procedureBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            procedureBuilder.addPerformer(staffReference, staffMemberIdDoneBy);
        }

        //status is mandatory, so set the only value we can
        procedureBuilder.setStatus(org.hl7.fhir.instance.model.Procedure.ProcedureStatus.COMPLETED);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            procedureBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            procedureBuilder.setPerformed(dateTimeType, effectiveDate);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
        CsvCell snomedCodeCell = parser.getSNOMEDCode();
        CsvCell snomedDescCell = parser.getSNOMEDText();
        CsvCell ctv3CodeCell = parser.getCTV3Code();
        CsvCell ctv3DescCell = parser.getCTV3Text();
        addCodeableConcept(codeableConceptBuilder, snomedCodeCell, snomedDescCell, ctv3CodeCell, ctv3DescCell);

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {
            Reference eventReference = csvHelper.createEncounterReference(eventId);
            procedureBuilder.setEncounter(eventReference, eventId);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(procedureBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }


    private static void createOrDeleteCondition(SRCode parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                TppCsvHelper csvHelper) throws Exception {

        CsvCell conditionId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        //The condition resource may already exist as part of the Problem Transformer or will create one, set using the ID value of the code
        ConditionBuilder conditionBuilder = csvHelper.getConditionResourceCache().getConditionBuilderAndRemoveFromCache(conditionId, csvHelper);

        if (deleteData != null && deleteData.getIntAsBoolean()) {

            boolean mapIds = !conditionBuilder.isIdMapped();
            conditionBuilder.setDeletedAudit(deleteData);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), mapIds, conditionBuilder);

            return;
        }

        Reference patientReference = csvHelper.createPatientReference(patientId);
        if (conditionBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        conditionBuilder.setPatient(patientReference, patientId);

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            if (conditionBuilder.isIdMapped()) {
                staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference, fhirResourceFiler);
            }
            conditionBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            if (conditionBuilder.isIdMapped()) {
                staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference, fhirResourceFiler);
            }
            conditionBuilder.setClinician(staffReference, staffMemberIdDoneBy);
        }

        //status is mandatory, so set the only value we can
        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            conditionBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            conditionBuilder.setOnset(dateTimeType, effectiveDate);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
        CsvCell snomedCodeCell = parser.getSNOMEDCode();
        CsvCell snomedDescCell = parser.getSNOMEDText();
        CsvCell ctv3CodeCell = parser.getCTV3Code();
        CsvCell ctv3DescCell = parser.getCTV3Text();
        addCodeableConcept(codeableConceptBuilder, snomedCodeCell, snomedDescCell, ctv3CodeCell, ctv3DescCell);

        CsvCell episodeType = parser.getEpisodeType();
        if (!episodeType.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(episodeType);
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

            Reference eventReference = csvHelper.createEncounterReference(eventId);
            if (conditionBuilder.isIdMapped()) {
                eventReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(eventReference, fhirResourceFiler);
            }
            conditionBuilder.setEncounter(eventReference, eventId);
        }

        boolean mapIds = !conditionBuilder.isIdMapped();
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapIds, conditionBuilder);
    }

    private static void createOrDeleteObservation(SRCode parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            Observation observation = (Observation) csvHelper.retrieveResource(rowId.getString(), ResourceType.Observation);
            if (observation != null) {
                ObservationBuilder observationBuilder = new ObservationBuilder(observation);
                observationBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, observationBuilder);
            }
            return;
        }

        ObservationBuilder observationBuilder = new ObservationBuilder();
        observationBuilder.setId(rowId.getString(), rowId);

        observationBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            observationBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            observationBuilder.setClinician(staffReference, staffMemberIdDoneBy);
        }

        //status is mandatory, so set the only value we can
        observationBuilder.setStatus(Observation.ObservationStatus.UNKNOWN);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            observationBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            observationBuilder.setEffectiveDate(dateTimeType, effectiveDate);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
        CsvCell snomedCodeCell = parser.getSNOMEDCode();
        CsvCell snomedDescCell = parser.getSNOMEDText();
        CsvCell ctv3CodeCell = parser.getCTV3Code();
        CsvCell ctv3DescCell = parser.getCTV3Text();
        addCodeableConcept(codeableConceptBuilder, snomedCodeCell, snomedDescCell, ctv3CodeCell, ctv3DescCell);

        //ObservationBuilder systolicObservationBuilder = null;
        //ObservationBuilder diastolicObservationBuilder = null;

        //TODO - rewrite the below to work - the aim is to have an Observation for the systolic and diastolic
        // Not sure this is relevant to TPP.  This is only valid when a parent BP code is received with two
        // values, i.e. the systolic and diastolic readings. See Vision Journal implementation

        //with a third Observation containing both values that the first two link to. The below does not do this.
        /*CsvCell readSNOMEDCode = parser.getSNOMEDCode();
        if (readSNOMEDCode != null && !readSNOMEDCode.isEmpty() && !readSNOMEDCode.getString().equals("-1")) {
            SnomedCode snomedCode = TerminologyService.translateRead2ToSnomed(readSNOMEDCode.getString());
            if (snomedCode != null) {
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
                addSnomedToBuilder(codeableConceptBuilder, snomedCode, parser.getSNOMEDText());
            }
        } else {

            CsvCell readV3Code = parser.getCTV3Code();
            if (!readV3Code.isEmpty()) {
                if (readV3Code.getString().equals(SYSTOLIC)) {

                    // add the Systolic component to the main observation
                    observationBuilder.addComponent();
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
                    systolicObservationBuilder.setEffectiveDate(new DateTimeType(effectiveDate.getDate(), TemporalPrecisionEnum.DAY));
                    systolicObservationBuilder.setPatient(csvHelper.createPatientReference(patientId));
                    Reference clinicianReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
                    systolicObservationBuilder.setClinician(clinicianReference, profileIdRecordedBy);
                    CodeableConceptBuilder codeableSystolicConceptBuilder
                            = new CodeableConceptBuilder(systolicObservationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
                    codeableSystolicConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableSystolicConceptBuilder.setCodingCode("163030003");
                    codeableSystolicConceptBuilder.setCodingDisplay("Systolic blood pressure reading");
                    codeableSystolicConceptBuilder.setText("Systolic blood pressure reading");
                    systolicObservationBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
                    Reference parentResource
                            = csvHelper.createObservationReference(observationBuilder.getResource().getId(), patientId.getString());
                    systolicObservationBuilder.setParentResource(parentResource);

                } else if (readV3Code.getString().equals(DIASTOLIC)) {

                    // add the Diastolic component to the main observation
                    observationBuilder.addComponent();
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
                    diastolicObservationBuilder.setEffectiveDate(new DateTimeType(effectiveDate.getDate(), TemporalPrecisionEnum.DAY));
                    diastolicObservationBuilder.setPatient(csvHelper.createPatientReference(patientId));
                    Reference clinicianReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
                    diastolicObservationBuilder.setClinician(clinicianReference, profileIdRecordedBy);
                    CodeableConceptBuilder codeableDistolicConceptBuilder
                            = new CodeableConceptBuilder(diastolicObservationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
                    codeableDistolicConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableDistolicConceptBuilder.setCodingCode("163031004");
                    codeableDistolicConceptBuilder.setCodingDisplay("Diastolic blood pressure reading");
                    codeableDistolicConceptBuilder.setText("Diastolic blood pressure reading");
                    diastolicObservationBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
                    Reference parentResource
                            = csvHelper.createObservationReference(observationBuilder.getResource().getId(), patientId.getString());
                    diastolicObservationBuilder.setParentResource(parentResource);

                } else {
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
                            addSnomedToBuilder(codeableConceptBuilder, snomedCode, null);
                        }
                    }
                }
            }
        }*/

        CsvCell numericValue = parser.getNumericValue();
        CsvCell isNumericCell = parser.getIsNumeric();
        if (!numericValue.isEmpty()
                && (isNumericCell == null || isNumericCell.getBoolean())) { //null check required because the column wasn't always present

            observationBuilder.setValueNumber(numericValue.getDouble(), numericValue);
//            if (systolicObservationBuilder != null) {
//                systolicObservationBuilder.setValueNumber(numericValue.getDouble(), numericValue);
//            }
//            if (diastolicObservationBuilder != null) {
//                diastolicObservationBuilder.setValueNumber(numericValue.getDouble(), numericValue);
//            }
        }

        CsvCell numericUnits = parser.getNumericUnit();
        if (!numericUnits.isEmpty()
                && (isNumericCell == null || isNumericCell.getBoolean())) { //null check required because the column wasn't always present

            observationBuilder.setValueNumberUnits(numericUnits.getString(), numericUnits);
//            if (systolicObservationBuilder != null) {
//                systolicObservationBuilder.setValueNumberUnits(numericUnits.getString(), numericUnits);
//            }
//            if (diastolicObservationBuilder != null) {
//                diastolicObservationBuilder.setValueNumberUnits(numericUnits.getString(), numericUnits);
//            }
        }

        CsvCell numericComparator = parser.getNumericComparator();
        if (!numericComparator.isEmpty()
                && (isNumericCell == null || isNumericCell.getBoolean())) { //null check required because the column wasn't always present

            Quantity.QuantityComparator comparator = convertComparator(numericComparator.getString());
            if (comparator != null) {
                observationBuilder.setValueNumberComparator(comparator, numericComparator);
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId);
            observationBuilder.setEncounter(eventReference, eventId);
        }

//        if (systolicObservationBuilder != null) {
//            fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder, systolicObservationBuilder);
//        } else if (diastolicObservationBuilder != null) {
//            fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder, diastolicObservationBuilder);
//        } else {
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
        //}
    }

    private static void createOrDeleteFamilyMemberHistory(SRCode parser,
                                                          FhirResourceFiler fhirResourceFiler,
                                                          TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            FamilyMemberHistory familyMemberHistory = (FamilyMemberHistory) csvHelper.retrieveResource(rowId.getString(), ResourceType.FamilyMemberHistory);
            if (familyMemberHistory != null) {
                FamilyMemberHistoryBuilder familyMemberHistoryBuilder = new FamilyMemberHistoryBuilder(familyMemberHistory);
                familyMemberHistoryBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, familyMemberHistoryBuilder);
                return;
            }
        }

        FamilyMemberHistoryBuilder familyMemberHistoryBuilder = new FamilyMemberHistoryBuilder();
        familyMemberHistoryBuilder.setId(rowId.getString(), rowId);

        familyMemberHistoryBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            familyMemberHistoryBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            familyMemberHistoryBuilder.setClinician(staffReference, staffMemberIdDoneBy);
        }

        //status is mandatory, so set the only value we can
        familyMemberHistoryBuilder.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            familyMemberHistoryBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            familyMemberHistoryBuilder.setDate(dateTimeType, effectiveDate);
        }

        //most of the codes are just "FH: xxx" so can't be mapped to a definite family member relationship,
        //so just use the generic family member term
        familyMemberHistoryBuilder.setRelationship(FamilyMember.FAMILY_MEMBER);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(familyMemberHistoryBuilder, CodeableConceptBuilder.Tag.Family_Member_History_Main_Code);
        CsvCell snomedCodeCell = parser.getSNOMEDCode();
        CsvCell snomedDescCell = parser.getSNOMEDText();
        CsvCell ctv3CodeCell = parser.getCTV3Code();
        CsvCell ctv3DescCell = parser.getCTV3Text();
        addCodeableConcept(codeableConceptBuilder, snomedCodeCell, snomedDescCell, ctv3CodeCell, ctv3DescCell);

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId);
            familyMemberHistoryBuilder.setEncounter(eventReference, eventId);
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(familyMemberHistoryBuilder, parser);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
    }

    private static void assertValueEmpty(ResourceBuilderBase resourceBuilder, SRCode parser) throws Exception {
        CsvCell valueCell = parser.getNumericValue();
        if (!valueCell.isEmpty()
                && !valueCell.getString().equalsIgnoreCase("0.0")) {
            throw new FieldNotEmptyException("Value", resourceBuilder.getResource());
        }
    }

    //the FHIR resource type is roughly derived from the code subset and ReadCode
    public static ResourceType getTargetResourceType(SRCode parser, TppCsvHelper csvHelper) throws Exception {

        String readV3Code = parser.getCTV3Code().getString();
        if (!Strings.isNullOrEmpty(readV3Code)
                && Read2.isProcedure(readV3Code) //TODO - change to a CTV3 version (using hierarchy not code letters)
                && !isBPCode(readV3Code)
                && (parser.getNumericValue().isEmpty())) {
            return ResourceType.Procedure;
        } else if (csvHelper.isProblemObservationGuid(parser.getRowIdentifier())) { //TODO - change to use CTV3 hierarchy AND check for PAST SRProblem records
            return ResourceType.Condition;
        } else if ((!Strings.isNullOrEmpty(readV3Code)
                && csvHelper.isAllergyCode(readV3Code, parser.getCTV3Text().getString()))) {
            return ResourceType.AllergyIntolerance;
        } else if (!Strings.isNullOrEmpty(readV3Code)
                && Read2.isFamilyHistory(readV3Code)) { //TODO - change to a CTV3 version (using hierarchy not code letters)
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

    /**
     * finds out what resource type an EMIS observation was previously saved as
     */
    public static boolean wasOriginallySavedAsCondition(FhirResourceFiler fhirResourceFiler, CsvCell codeId) throws Exception {
        return checkIfWasSavedAsResourceType(fhirResourceFiler, codeId, ResourceType.Condition);
    }

    public static ResourceType wasOriginallySavedAsOtherThanCondition(FhirResourceFiler fhirResourceFiler, CsvCell codeId) throws Exception {

        List<ResourceType> potentialResourceTypes = new ArrayList<>();
        potentialResourceTypes.add(ResourceType.Observation);
        //potentialResourceTypes.add(ResourceType.Condition); //don't check this here - as conditions are handled differently
        potentialResourceTypes.add(ResourceType.Procedure);
        potentialResourceTypes.add(ResourceType.AllergyIntolerance);
        potentialResourceTypes.add(ResourceType.FamilyMemberHistory);
        potentialResourceTypes.add(ResourceType.Immunization);
        potentialResourceTypes.add(ResourceType.DiagnosticOrder);
        potentialResourceTypes.add(ResourceType.Specimen);
        potentialResourceTypes.add(ResourceType.DiagnosticReport);
        potentialResourceTypes.add(ResourceType.ReferralRequest);

        for (ResourceType resourceType : potentialResourceTypes) {
            if (checkIfWasSavedAsResourceType(fhirResourceFiler, codeId, resourceType)) {
                return resourceType;
            }
        }
        return null;
    }

    private static boolean checkIfWasSavedAsResourceType(FhirResourceFiler fhirResourceFiler, CsvCell codeId, ResourceType resourceType) throws Exception {
        String sourceId = codeId.getString();
        UUID uuid = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), resourceType, sourceId);
        return uuid != null;
    }

}
