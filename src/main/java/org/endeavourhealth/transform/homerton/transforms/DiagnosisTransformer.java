package org.endeavourhealth.transform.homerton.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.DiagnosisTable;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class DiagnosisTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createDiagnosis((DiagnosisTable) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createDiagnosis(DiagnosisTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper) throws Exception {

        CsvCell diagnosisIdCell = parser.getDiagnosisID();

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setAsProblem(false);
        conditionBuilder.setId(diagnosisIdCell.getString(), diagnosisIdCell);

        CsvCell personIdCell = parser.getPersonId();
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
        conditionBuilder.setPatient(patientReference, personIdCell);

        // delete the diagnosis if no longer active.  We have the patientId so this is straight forward
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            return;
        }

        CsvCell encounterIdCell = parser.getEncounterID();
        if (!encounterIdCell.isEmpty()) {
            Reference encounterReference
                    = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
            conditionBuilder.setEncounter(encounterReference, encounterIdCell);
        }

        CsvCell diagnosisDateTimeCell = parser.getDiagnosisDateTime();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(diagnosisDateTimeCell)) {

            Date d = diagnosisDateTimeCell.getDateTime();
            DateTimeType dateTimeType = new DateTimeType(d);
            conditionBuilder.setOnset(dateTimeType, diagnosisDateTimeCell);
        }

        CsvCell confirmation = parser.getConfirmation();
        if (!confirmation.isEmpty()) {
            String confirmationDesc = confirmation.getString();
            if (confirmationDesc.equalsIgnoreCase("Confirmed")) {
                conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED, confirmation);

            } else {
                conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.PROVISIONAL, confirmation);
            }
        }

        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
        if (!HomertonCsvHelper.isEmptyOrIsZero(encounterSliceIdCell)) {

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
        }

        //TODO - need the Personnel data extract
//        CsvCell personnelIdCell = parser.getPersonnelId();
//        if (!HomertonCsvHelper.isEmptyOrIsZero(personnelIdCell)) {
//            Reference practitionerReference = csvHelper.createPractitionerReference(personnelIdCell.getString());
//            conditionBuilder.setClinician(practitionerReference, personnelIdCell);
//        }

        // Condition(Diagnosis) is coded either as Snomed or ICD10
        CsvCell conceptCodeCell = parser.getConceptCode();
        if (!conceptCodeCell.isEmpty()) {

            String conceptCode = conceptCodeCell.getString();
            CsvCell conceptCodeTypeCell = parser.getConceptCodeType();

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

            if (!conceptCodeTypeCell.isEmpty()) {

                String conceptCodeType = conceptCodeTypeCell.getString();
                if (conceptCodeType.equalsIgnoreCase(HomertonCsvHelper.CODE_TYPE_SNOMED)) {

                    // Homerton use Snomed descriptionId instead of conceptId
                    SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(conceptCode);
                    if (snomedCode == null) {
                        TransformWarnings.log(LOG, parser, "Failed to find Snomed term for DescriptionId {}", conceptCodeCell.getString());

                        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID, conceptCodeTypeCell);
                        codeableConceptBuilder.setCodingCode(conceptCode, conceptCodeCell);
                    } else {
                        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptCodeTypeCell);
                        codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode(), conceptCodeCell);
                        codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm()); //don't pass in the cell as this is derived
                        codeableConceptBuilder.setText(snomedCode.getTerm()); //don't pass in the cell as this is derived
                    }
                } else if (conceptCodeType.equalsIgnoreCase(HomertonCsvHelper.CODE_TYPE_ICD_10)) {
                    String term = TerminologyService.lookupIcd10CodeDescription(conceptCode);
                    if (Strings.isNullOrEmpty(term)) {
                        TransformWarnings.log(LOG, parser, "Failed to find ICD-10 term for {}", conceptCodeCell.getString());
                    }

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conceptCodeTypeCell);
                    codeableConceptBuilder.setCodingCode(conceptCode, conceptCodeCell);
                    codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                    codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

                } else {
                    throw new TransformException("Unknown Diagnosis code type [" + conceptCodeType + "]");
                }
            }
        } else {
            //if there's no code, create a non coded code so we retain the text
            CsvCell term = parser.getDiagnosisDisplay();

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.setText(term.getString());
        }

        CsvCell diagnosisType = parser.getDiagnosisType();
        if (!diagnosisType.isEmpty()) {

            conditionBuilder.setCategory(diagnosisType.getString(), diagnosisType);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }
}
