package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.Condition;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
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

        //TODO: get confirmation status and ignore those that are not confirmed at this point


        //is it a problem or a diagnosis?
        CsvCell conditionTypeCodeCell = parser.getConditionTypeCode();
        if (conditionTypeCodeCell.getString().equalsIgnoreCase("55607006")) {

            conditionBuilder.setAsProblem(true);
            conditionBuilder.setCategory("complaint", conditionTypeCodeCell);
        } else {
            conditionBuilder.setAsProblem(false);
            conditionBuilder.setCategory("diagnosis", conditionTypeCodeCell);
        }


//        CsvCell encounterIdCell = parser.getEncounterID();
//        if (!encounterIdCell.isEmpty()) {
//            Reference encounterReference
//                    = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
//            conditionBuilder.setEncounter(encounterReference, encounterIdCell);
//        }
//
//        CsvCell diagnosisDateTimeCell = parser.getDiagnosisDateTime();
//        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(diagnosisDateTimeCell)) {
//
//            Date d = diagnosisDateTimeCell.getDateTime();
//            DateTimeType dateTimeType = new DateTimeType(d);
//            conditionBuilder.setOnset(dateTimeType, diagnosisDateTimeCell);
//        }
//
//        CsvCell confirmation = parser.getConfirmation();
//        if (!confirmation.isEmpty()) {
//            String confirmationDesc = confirmation.getString();
//            if (confirmationDesc.equalsIgnoreCase("Confirmed")) {
//                conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED, confirmation);
//
//            } else {
//                conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.PROVISIONAL, confirmation);
//            }
//        } else {
//            conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.UNKNOWN, confirmation);
//        }
//
//        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
//        if (!HomertonCsvHelper.isEmptyOrIsZero(encounterSliceIdCell)) {
//
//            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
//            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
//            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
//            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
//        }
//
//        // Condition(Diagnosis) is coded either as Snomed or ICD10
//        CsvCell conceptCodeCell = parser.getConceptCode();
//        if (!conceptCodeCell.isEmpty()) {
//
//            String conceptCode = conceptCodeCell.getString();
//            CsvCell conceptCodeTypeCell = parser.getConceptCodeType();
//
//            CodeableConceptBuilder codeableConceptBuilder
//                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
//
//            if (!conceptCodeTypeCell.isEmpty()) {
//
//                String conceptCodeType = conceptCodeTypeCell.getString();
//                if (conceptCodeType.equalsIgnoreCase(HomertonCsvHelper.CODE_TYPE_SNOMED)) {
//
//                    // Homerton use Snomed descriptionId instead of conceptId
//                    SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(conceptCode);
//                    if (snomedCode == null) {
//                        TransformWarnings.log(LOG, parser, "Failed to find Snomed term for DescriptionId {}", conceptCodeCell.getString());
//
//                        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID, conceptCodeTypeCell);
//                        codeableConceptBuilder.setCodingCode(conceptCode, conceptCodeCell);
//                    } else {
//                        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptCodeTypeCell);
//                        codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode(), conceptCodeCell);
//                        codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm()); //don't pass in the cell as this is derived
//
//                        CsvCell term = parser.getDiagnosisDisplay();
//                        if (!term.isEmpty()) {
//                            codeableConceptBuilder.setText(term.getString(), term);
//                        }
//                    }
//                } else if (conceptCodeType.equalsIgnoreCase(HomertonCsvHelper.CODE_TYPE_ICD_10)) {
//                    String term = TerminologyService.lookupIcd10CodeDescription(conceptCode);
//                    if (Strings.isNullOrEmpty(term)) {
//                        TransformWarnings.log(LOG, parser, "Failed to find ICD-10 term for {}", conceptCodeCell.getString());
//                    }
//
//                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conceptCodeTypeCell);
//                    codeableConceptBuilder.setCodingCode(conceptCode, conceptCodeCell);
//                    codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
//
//                    CsvCell origTerm = parser.getDiagnosisDisplay();
//                    if (!origTerm.isEmpty()) {
//                        codeableConceptBuilder.setText(origTerm.getString(), origTerm);
//                    }
//
//                } else {
//                    throw new TransformException("Unknown Diagnosis code type [" + conceptCodeType + "]");
//                }
//            }
//        } else {
//            //if there's no code, create a non coded code so we retain the text from the non code element
//            CsvCell term = parser.getDiagnosisDisplayNonCoded();
//
//            CodeableConceptBuilder codeableConceptBuilder
//                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
//            codeableConceptBuilder.setText(term.getString());
//        }
//
//        CsvCell notes = parser.getDiagnosisNotes();
//        if (!notes.isEmpty()) {
//            conditionBuilder.setNotes(notes.getString(), notes);
//        }


        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }
}
