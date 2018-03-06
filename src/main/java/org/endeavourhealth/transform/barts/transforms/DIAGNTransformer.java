package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
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

import java.util.UUID;

public class DIAGNTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGNTransformer.class);


    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                createCondition((DIAGN)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createCondition(DIAGN parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // this Condition resource id
        CsvCell diagnosisId = parser.getDiagnosisID();
        ResourceId conditionResourceId = getOrCreateConditionResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, diagnosisId);

        //if the record is non-active (i.e. deleted) we ONLY get the ID, date and active indicator, NOT the encounter ID
        //so we need to re-retrieve the previous instance of the resource to find the patient Reference which we need to delete
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
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
            DateTimeType dateTimeType = new DateTimeType(diagnosisDateTimeCell.getDate());
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

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, ConditionBuilder.TAG_CODEABLE_CONCEPT_CODE);

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
        if (!diagnosisTypeCode.isEmpty() && diagnosisTypeCode.getLong() > 0) {
            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                                                                    CernerCodeValueRef.DIAGNOSIS_TYPE,
                                                                    diagnosisTypeCode.getLong());
            String category = cernerCodeValueRef.getCodeDispTxt();
            conditionBuilder.setCategory(category, diagnosisTypeCode);
        }

        CsvCell diagnosisFreeText = parser.getDiagnosicFreeText();
        if (!diagnosisFreeText.isEmpty()) {
            conditionBuilder.setNotes(diagnosisFreeText.getString(), diagnosisFreeText);
        }

        // save the resource
        LOG.debug("Save Condition (" + conditionBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), conditionBuilder);
    }

}
