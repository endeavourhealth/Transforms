package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.Condition;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ConditionTransformer  {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createCondition((Condition) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createCondition(Condition parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonHiCsvHelper csvHelper) throws Exception {

        CsvCell conditionIdCell = parser.getConditionId();

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setId(conditionIdCell.getString(), conditionIdCell);

        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        Reference patientReference
                = ReferenceHelper.createReference(ResourceType.Patient, personEmpiIdCell.getString());
        conditionBuilder.setPatient(patientReference, personEmpiIdCell);

        //NOTE:deletions are checked by comparing the deletion hash values set up in the deletion pre-transform
        CsvCell hashValueCell = parser.getHashValue();
        boolean deleted = false;  //TODO: requires pre-transform per file to establish deletions
        if (deleted) {
            conditionBuilder.setDeletedAudit(hashValueCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            return;
        }

        CsvCell confirmationCell = parser.getConditionConfirmationStatusDisplay();
        if (!confirmationCell.isEmpty()) {
                String confirmation = confirmationCell.getString();
            if (confirmation.equalsIgnoreCase("confirmed")) {
                conditionBuilder.setVerificationStatus(org.hl7.fhir.instance.model.Condition.ConditionVerificationStatus.CONFIRMED, confirmationCell);

            } else {

                //only interested in Confirmed conditions
                return;
            }
        } else {

            //only interested in Confirmed conditions
            return;
        }

        //is it a problem or a diagnosis?
        CsvCell conditionTypeCodeCell = parser.getConditionTypeCode();
        if (conditionTypeCodeCell.getString().equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_CONDITION_PROBLEM)) {

            conditionBuilder.setAsProblem(true);
            conditionBuilder.setCategory("complaint", conditionTypeCodeCell);

            CsvCell problemStatusDisplayCell = parser.getProblemStatusDisplay();
            String problemStatus = problemStatusDisplayCell.getString();
            if (problemStatus.equalsIgnoreCase("active")) {

                conditionBuilder.setEndDateOrBoolean(null);

            } else if (problemStatus.equalsIgnoreCase("resolved")
                    || problemStatus.equalsIgnoreCase("inactive")
                    || problemStatus.equalsIgnoreCase("cancelled")) {

                //Status date confirmed as problem changed to Resolved/Inactive date for example
                CsvCell statusDateTimeCell = parser.getProblemStatusDtm();
                if (!statusDateTimeCell.isEmpty()) {

                    DateType dt = new DateType(statusDateTimeCell.getDateTime());
                    conditionBuilder.setEndDateOrBoolean(dt);

                } else {

                    //if we don't have a status date, use a boolean to indicate the end
                    conditionBuilder.setEndDateOrBoolean(new BooleanType(true));
                }
            }
        } else if (conditionTypeCodeCell.getString().equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_CONDITION_DIAGNOSIS)) {

            conditionBuilder.setAsProblem(false);
            conditionBuilder.setCategory("diagnosis", conditionTypeCodeCell);

            //an active Diagnosis
            conditionBuilder.setEndDateOrBoolean(null);

        } else {

            throw new TransformException("Unknown Condition type [" + conditionTypeCodeCell.getString() + "]");
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        if (!encounterIdCell.isEmpty()) {
            Reference encounterReference
                    = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
            conditionBuilder.setEncounter(encounterReference, encounterIdCell);
        }

        CsvCell effectiveDateTimeCell = parser.getEffectiveDtm();
        if (!effectiveDateTimeCell.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDateTimeCell.getDateTime());
            conditionBuilder.setOnset(dateTimeType, effectiveDateTimeCell);
        }

        // Conditions are coded either as Snomed or ICD10
        CsvCell conditionCodeCell = parser.getConditionRawCode();
        if (!conditionCodeCell.isEmpty()) {

            String conditionCode = conditionCodeCell.getString();
            CsvCell conditionCodeSystemCell = parser.getConditionCodingSystemId();

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

            if (!conditionCodeSystemCell.isEmpty()) {

                String conceptCodeSystem = conditionCodeSystemCell.getString();
                if (conceptCodeSystem.equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_SNOMED_URN)) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conditionCodeSystemCell);

                } else if (conceptCodeSystem.equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_ICD10_URN)) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conditionCodeSystemCell);

                } else {

                    throw new TransformException("Unknown Condition code system [" + conceptCodeSystem + "]");
                }

                codeableConceptBuilder.setCodingCode(conditionCode, conditionCodeCell);
                CsvCell conditionCodeDisplayCell = parser.getConditionDisplay();
                codeableConceptBuilder.setCodingDisplay(conditionCodeDisplayCell.getString(), conditionCodeDisplayCell);
                codeableConceptBuilder.setText(conditionCodeDisplayCell.getString(), conditionCodeDisplayCell);
            }
        } else {
            //if there's no code, create a non coded code so we retain the text from the non code element
            CsvCell term = parser.getConditionDescription();

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.setText(term.getString());
        }

//
//        CsvCell notes = parser.getDiagnosisNotes();
//        if (!notes.isEmpty()) {
//            conditionBuilder.setNotes(notes.getString(), notes);
//        }


        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }
}
