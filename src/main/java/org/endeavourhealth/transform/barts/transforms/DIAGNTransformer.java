package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.DIAGN;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class DIAGNTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGNTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createCondition((DIAGN)parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createCondition(DIAGN parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // this Condition resource id
        CsvCell diagnosisIdCell = parser.getDiagnosisID();

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            //if the record is non-active (i.e. deleted) we ONLY get the ID, date and active indicator, NOT the person ID
            //so we need to re-retrieve the previous instance of the resource to find the patient Reference which we need to delete
            Condition existingCondition = (Condition)csvHelper.retrieveResourceForLocalId(ResourceType.Condition, diagnosisIdCell);
            if (existingCondition != null) {
                ConditionBuilder conditionBuilder = new ConditionBuilder(existingCondition);
                //remember to pass in false as this resource is already ID mapped
                conditionBuilder.setDeletedAudit(activeCell);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, conditionBuilder);
            }
            return;
        }

        //the DIAGN file DOES NOT have a person ID, so we need to go through the ENCNT file to find it
        CsvCell encounterIdCell = parser.getEncounterId();
        String personId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);

        if (personId == null) {
            //TransformWarnings.log(LOG, parser, "Skipping Diagnosis {} due to missing encounter", diagnosisIdCell.getString());
            return;
        }

        if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
            return;
        }

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setAsProblem(false);

        conditionBuilder.setId(diagnosisIdCell.getString(), diagnosisIdCell);

        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId);
        conditionBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
        conditionBuilder.setEncounter(encounterReference, encounterIdCell);

        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        CsvCell diagnosisDateTimeCell = parser.getDiagnosisDateTime();
        if (!diagnosisDateTimeCell.isEmpty()) {
            Date d = BartsCsvHelper.parseDate(diagnosisDateTimeCell);
            DateTimeType dateTimeType = new DateTimeType(d);
            conditionBuilder.setOnset(dateTimeType, diagnosisDateTimeCell);
        }

        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
        if (!BartsCsvHelper.isEmptyOrIsZero(encounterSliceIdCell)) {

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
        }

        CsvCell personnelIdCell = parser.getPersonnelId();
        if (!BartsCsvHelper.isEmptyOrIsZero(personnelIdCell)) {
            Reference practitionerReference = csvHelper.createPractitionerReference(personnelIdCell);
            conditionBuilder.setClinician(practitionerReference, personnelIdCell);
        }

        // Condition(Diagnosis) is coded either as Snomed or ICD10
        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
        if (!conceptIdentifierCell.isEmpty()) {
            String conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
            String conceptCodeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(conceptIdentifierCell);

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

            if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                //NOTE: this code IS a SNOMED concept ID, unlike the Problem file which has a description ID
                String term = TerminologyService.lookupSnomedTerm(conceptCode);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, csvHelper, "Failed to find Snomed term for {}", conceptIdentifierCell);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                String term = TerminologyService.lookupIcd10CodeDescription(conceptCode);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, csvHelper, "Failed to find ICD-10 term for {}", conceptIdentifierCell);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, csvHelper, "Failed to find OPCS-4 term for {}", conceptIdentifierCell);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else {
                throw new TransformException("Unknown DIAGN code type [" + conceptCodeType + "]");
            }

        } else {
            //if there's no code, there's nothing to save
            return;
        }

        // Diagnosis type (category) is a Cerner Millenium code so lookup
        //removing this, as it's always "Diagnosis", which we already know because it's the diagnosis transform
        /*CsvCell diagnosisTypeCode = parser.getDiagnosisTypeCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(diagnosisTypeCode)) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.PRINCIPAL_TYPE, diagnosisTypeCode);
            if (cernerCodeValueRef== null) {
                TransformWarnings.log(LOG, parser, "SEVERE: cerner code for DiagnosisTypeCode {} not found", diagnosisTypeCode);

            } else {
                String category = cernerCodeValueRef.getCodeDispTxt();
                conditionBuilder.setCategory(category, diagnosisTypeCode);
            }
        }*/

        //the sequence number tells us whether it's a principal diagnoses or not
        CsvCell sequenceNumberCell = parser.getSequenceNumber();
        Integer sequenceNumber = sequenceNumberCell.getInt();
        String category = null;
        if (sequenceNumber.intValue() == 1) {
            category = "Principal Diagnosis";
        } else if (sequenceNumber.intValue() > 1) {
            category = "Secondary Diagnosis";
        }
        conditionBuilder.setCategory(category, sequenceNumberCell);

        CsvCell diagnosisFreeText = parser.getDiagnosisFreeText();
        if (!diagnosisFreeText.isEmpty()) {
            conditionBuilder.setNotes(diagnosisFreeText.getString(), diagnosisFreeText);
        }

        // save the resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }

}
