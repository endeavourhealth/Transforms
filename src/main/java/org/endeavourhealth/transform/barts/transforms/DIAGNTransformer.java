package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.DIAGN;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
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
                                 DIAGN parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                createCondition(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createCondition(DIAGN parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // get encounter details (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterId();
        UUID encounterUuid = csvHelper.findEncounterResourceIdFromEncounterId(encounterIdCell);
        UUID patientUuid = csvHelper.findPatientIdFromEncounterId(encounterIdCell);
        if (patientUuid == null) {
            LOG.warn("Skipping Diagnosis " + parser.getDiagnosisID().getString() + " due to missing encounter");
            return;
        }

        // this Condition resource id
        CsvCell diagnosisId = parser.getDiagnosisID();
        ResourceId conditionResourceId = getOrCreateConditionResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, diagnosisId);

        // create the FHIR Condition
        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setAsProblem(false);

        conditionBuilder.setId(conditionResourceId.getResourceId().toString(), diagnosisId);

        // set the patient reference
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString());
        conditionBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            LOG.debug("Delete Condition (" + conditionBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), conditionBuilder);
            return;
        }

        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        if (!diagnosisId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirCodeUri.CODE_SYSTEM_CERNER_DIAGNOSIS_ID);
            identifierBuilder.setValue(diagnosisId.getString(), diagnosisId);
        }

        CsvCell diagnosisDateTimeCell = parser.getDiagnosisDateTime();
        if (!diagnosisDateTimeCell.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(diagnosisDateTimeCell.getDate());
            conditionBuilder.setOnset(dateTimeType, diagnosisDateTimeCell);
        }

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterUuid.toString());
        conditionBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell encounterSliceID = parser.getEncounterSliceID();
        if (!encounterSliceID.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirCodeUri.CODE_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceID.getString(), encounterSliceID);
        }

        CsvCell nomenclatureId = parser.getNomenclatureID();
        if (!nomenclatureId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(conditionBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirCodeUri.CODE_SYSTEM_CERNER_NOMENCLATURE_ID);
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
            }

        } else {
            LOG.warn("Unable to create codeableConcept for Condition ID: "+diagnosisId);
            return;
        }

        // Diagnosis type (category) is a Cerner Millenium code so lookup
        CsvCell diagnosisTypeCode = parser.getDiagnosisTypeCode();
        BartsCodeableConceptHelper.applyCodeDisplayTxt(diagnosisTypeCode, CernerCodeValueRef.DIAGNOSIS_TYPE, conditionBuilder, ConditionBuilder.TAG_CODEABLE_CONCEPT_CATEGORY, csvHelper);

        CsvCell diagnosisFreeText = parser.getDiagnosicFreeText();
        if (!diagnosisFreeText.isEmpty()) {
            conditionBuilder.setNotes(diagnosisFreeText.getString(), diagnosisFreeText);
        }

        // save the resource
        LOG.debug("Save Condition (" + conditionBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), conditionBuilder);
    }

}
