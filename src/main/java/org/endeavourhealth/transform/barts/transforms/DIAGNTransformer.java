package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.DIAGN;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
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
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, conditionBuilder);
            }
            return;
        }

        //the DIAGN file DOES NOT have a person ID, so we need to go through the ENCNT file to find it
        CsvCell encounterIdCell = parser.getEncounterId();
        String personId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);

        if (personId == null) {
            TransformWarnings.log(LOG, parser, "Skipping Diagnosis {} due to missing encounter", diagnosisIdCell.getString());
            return;
        }

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setAsProblem(false);

        conditionBuilder.setId(diagnosisIdCell.toString(), diagnosisIdCell);

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

        CsvCell nomenclatureId = parser.getNomenclatureID();
        if (!BartsCsvHelper.isEmptyOrIsZero(nomenclatureId)) {

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NOMENCLATURE_ID);
            identifierBuilder.setValue(nomenclatureId.getString(), nomenclatureId);
        }

        CsvCell personnelIdCell = parser.getPersonnelId();
        if (!personnelIdCell.isEmpty()) {
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
                String term = TerminologyService.lookupSnomedFromConceptId(conceptCode).getTerm();

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                String term = TerminologyService.lookupIcd10CodeDescription(conceptCode);

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else {
                throw new TransformException("Unknown DIAGN code type [" + conceptCodeType + "]");
            }

        } else {
            TransformWarnings.log(LOG, parser, "Unable to create CodeableConcept for Condition ID {}", diagnosisIdCell);
            return;
        }

        // Diagnosis type (category) is a Cerner Millenium code so lookup
        CsvCell diagnosisTypeCode = parser.getDiagnosisTypeCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(diagnosisTypeCode)) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.DIAGNOSIS_TYPE, diagnosisTypeCode);
            if (cernerCodeValueRef== null) {
                TransformWarnings.log(LOG, parser, "SEVERE: cerner code {} for DiagnosisTypeCode {} not found. Row {} Column {} ",
                        diagnosisTypeCode.getLong(), parser.getDiagnosisTypeCode().getString(),
                        diagnosisTypeCode.getRowAuditId(), diagnosisTypeCode.getColIndex());

            } else {
                String category = cernerCodeValueRef.getCodeDispTxt();
                conditionBuilder.setCategory(category, diagnosisTypeCode);
            }
        }

        CsvCell diagnosisFreeText = parser.getDiagnosicFreeText();
        if (!diagnosisFreeText.isEmpty()) {
            conditionBuilder.setNotes(diagnosisFreeText.getString(), diagnosisFreeText);
        }

        // save the resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }

    /*public static void createCondition(DIAGN parser, FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper) throws Exception {

        // this Condition resource id
        CsvCell diagnosisId = parser.getDiagnosisID();
        ResourceId conditionResourceId = getOrCreateConditionResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, diagnosisId);

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            //if the record is non-active (i.e. deleted) we ONLY get the ID, date and active indicator, NOT the encounter ID
            //so we need to re-retrieve the previous instance of the resource to find the patient Reference which we need to delete
            Condition existingCondition = (Condition)csvHelper.retrieveResource(ResourceType.Condition, conditionResourceId.getResourceId());
            if (existingCondition != null) {
                ConditionBuilder conditionBuilder = new ConditionBuilder(existingCondition);
                //LOG.debug("Delete Condition (" + conditionBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), conditionBuilder);
            }
            return;
        }

        // get encounter details (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterId();
        UUID encounterUuid = csvHelper.findEncounterResourceIdFromEncounterId(encounterIdCell);
        UUID patientUuid = csvHelper.findPatientIdFromEncounterId(encounterIdCell);
        if (patientUuid == null) {
            TransformWarnings.log(LOG, parser, "Skipping Diagnosis {} due to missing encounter", parser.getDiagnosisID().getString());
            return;
        }

        // create the FHIR Condition
        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setAsProblem(false);

        conditionBuilder.setId(conditionResourceId.getResourceId().toString(), diagnosisId);

        // set the patient reference
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString());
        conditionBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter

        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        if (!diagnosisId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DIAGNOSIS_ID);
            identifierBuilder.setValue(diagnosisId.getString(), diagnosisId);
        }

        CsvCell diagnosisDateTimeCell = parser.getDiagnosisDateTime();
        if (!diagnosisDateTimeCell.isEmpty()) {
            Date d = BartsCsvHelper.parseDate(diagnosisDateTimeCell);
            DateTimeType dateTimeType = new DateTimeType(d);
            conditionBuilder.setOnset(dateTimeType, diagnosisDateTimeCell);
        }

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterUuid.toString());
        conditionBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
        if (!encounterSliceIdCell.isEmpty() && encounterSliceIdCell.getLong() > 0) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
        }

        CsvCell nomenclatureId = parser.getNomenclatureID();
        if (!nomenclatureId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NOMENCLATURE_ID);
            identifierBuilder.setValue(nomenclatureId.getString(), nomenclatureId);
        }

        CsvCell personnelIdCell = parser.getPersonnelId();
        if (!personnelIdCell.isEmpty()) {
            ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelIdCell);
            Reference practitionerReference = csvHelper.createPractitionerReference(practitionerResourceId.getResourceId().toString());
            conditionBuilder.setClinician(practitionerReference, personnelIdCell);
        }

        // Condition(Diagnosis) is coded either as Snomed or ICD10
        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
        if (!conceptIdentifierCell.isEmpty()) {
            String conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
            String conceptCodeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(conceptIdentifierCell);

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

            if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                String term = TerminologyService.lookupSnomedFromConceptId(conceptCode).getTerm();

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                String term = TerminologyService.lookupIcd10CodeDescription(conceptCode);

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else {
                throw new TransformException("Unknown DIAGN code type [" + conceptCodeType + "]");
            }

        } else {
            TransformWarnings.log(LOG, parser, "Unable to create CodeableConcept for Condition ID {}", diagnosisId);
            return;
        }

        // Diagnosis type (category) is a Cerner Millenium code so lookup
        CsvCell diagnosisTypeCode = parser.getDiagnosisTypeCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(diagnosisTypeCode)) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.DIAGNOSIS_TYPE, diagnosisTypeCode);
            if (cernerCodeValueRef== null) {
                TransformWarnings.log(LOG, parser, "SEVERE: cerner code {} for DiagnosisTypeCode {} not found. Row {} Column {} ",
                        diagnosisTypeCode.getLong(), parser.getDiagnosisTypeCode().getString(),
                        diagnosisTypeCode.getRowAuditId(), diagnosisTypeCode.getColIndex());

            } else {
                String category = cernerCodeValueRef.getCodeDispTxt();
                conditionBuilder.setCategory(category, diagnosisTypeCode);
            }
        }

        CsvCell diagnosisFreeText = parser.getDiagnosicFreeText();
        if (!diagnosisFreeText.isEmpty()) {
            conditionBuilder.setNotes(diagnosisFreeText.getString(), diagnosisFreeText);
        }

        // save the resource
        //LOG.debug("Save Condition (" + conditionBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), conditionBuilder);
    }*/

}
