package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.DIAGN;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DIAGNTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGNTransformer.class);
    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = null;

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

        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        // get encounter details (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterID();
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        if (encounterResourceId == null) {
            throw new TransformRuntimeException("Encounter ResourceId not found for EncounterId " + parser.getEncounterID() + " in file " + parser.getFilePath());
        }

        // get patient from encounter
        Encounter fhirEncounter = (Encounter) csvHelper.retrieveResource(encounterResourceId.getUniqueId(), ResourceType.Encounter, fhirResourceFiler);
        String patientId = fhirEncounter.getPatient().getId();
        ResourceId patientResourceId =  getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, patientId);
        if (patientResourceId == null) {
            throw new TransformRuntimeException("Patient ResourceId not found for PatientId " + patientId + " from Encounter ResourceId "+encounterResourceId.getUniqueId()+" in file " + parser.getFilePath());
        }

        // this Condition resource id
        CsvCell diagnosisDateTimeCell = parser.getDiagnosisDateTime();
        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
        String conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
        ResourceId conditionResourceId = getDiagnosisResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, patientId, diagnosisDateTimeCell.getString(), conceptCode);

        // create the FHIR Condition
        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setAsProblem(false);

        conditionBuilder.setId(conditionResourceId.getResourceId().toString(), diagnosisDateTimeCell, conceptIdentifierCell);

        // set the patient reference
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString());
        conditionBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            LOG.debug("Delete Condition (PatId=" + patientId + "):" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), conditionBuilder);
            return;
        }

        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        CsvCell diagnosisId = parser.getDiagnosisID();
        if (!diagnosisId.isEmpty()) {
            Identifier identifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID, diagnosisId.getString());
            conditionBuilder.addIdentifier(identifier, diagnosisId);
        }

        if (!diagnosisDateTimeCell.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(diagnosisDateTimeCell.getDate());
            conditionBuilder.setOnset(dateTimeType, diagnosisDateTimeCell);
        }

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString());
        conditionBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell encounterSliceID = parser.getEncounterSliceID();
        if (!encounterSliceID.isEmpty()) {
            Identifier identifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_ENCOUNTER_SLICE_ID, encounterSliceID.getString());
            conditionBuilder.addIdentifier(identifier, encounterSliceID);
        }

        CsvCell nomenclatureId = parser.getNomenclatureID();
        if (!nomenclatureId.isEmpty()) {
            Identifier identifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_NOMENCLATURE_ID, nomenclatureId.getString());
            conditionBuilder.addIdentifier(identifier, nomenclatureId);
        }


        CsvCell personnelId = parser.getPersonnel();
        if (!personnelId.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(personnelId);
            conditionBuilder.setClinician(practitionerReference, personnelId);
        }

        // Condition(Diagnosis) is coded either as Snomed or ICD10
        if (!conceptIdentifierCell.isEmpty()) {
            String conceptCodeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(conceptIdentifierCell);

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, ConditionBuilder.TAG_CODEABLE_CONCEPT_CODE);

            if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                String term = TerminologyService.lookupSnomedFromConceptId(conceptCode).getTerm();

                codeableConceptBuilder.addCoding(FhirUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                String term = TerminologyService.lookupIcd10CodeDescription(conceptCode);

                codeableConceptBuilder.addCoding(FhirUri.CODE_SYSTEM_ICD10, conceptIdentifierCell);
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
        if (!diagnosisTypeCode.isEmpty()) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(RdbmsCernerCodeValueRefDal.DIAGNOSIS_TYPE, diagnosisTypeCode.getLong(), fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                String diagnosisTypeTerm = cernerCodeValueRef.getCodeDispTxt();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, ConditionBuilder.TAG_CODEABLE_CONCEPT_CATEGORY);

                codeableConceptBuilder.addCoding(BartsCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_TYPE);
                codeableConceptBuilder.setCodingCode(diagnosisTypeCode.getString(), diagnosisTypeCode);
                codeableConceptBuilder.setCodingDisplay(diagnosisTypeTerm); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(diagnosisTypeTerm); //don't pass in the cell as this is derived

            } else {
                //LOG.warn("Diagnosis type code: "+diagnosisTypeCode+" not found in Code Value lookup");
            }
        }

        CsvCell diagnosisFreeText = parser.getDiagnosicFreeText();
        if (!diagnosisFreeText.isEmpty()) {
            conditionBuilder.setNotes(diagnosisFreeText.getString(), diagnosisFreeText);
        }

        // save the resource
        LOG.debug("Save Condition (PatId=" + patientId + "):" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), conditionBuilder);
    }

    /*public static void createCondition(DIAGN parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        // get encounter details (should already have been created previously)
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterID());
        if (encounterResourceId == null) {
            throw new TransformRuntimeException("Encounter ResourceId not found for EncounterId " + parser.getEncounterID() + " in file " + parser.getFilePath());
        }

        // get patient from encounter
        Encounter fhirEncounter = (Encounter) csvHelper.retrieveResource(encounterResourceId.getUniqueId(), ResourceType.Encounter, fhirResourceFiler);
        String patientId = fhirEncounter.getPatient().getId();
        ResourceId patientResourceId =  getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, patientId);
        if (patientResourceId == null) {
            throw new TransformRuntimeException("Patient ResourceId not found for PatientId " + patientId + " from Encounter ResourceId "+encounterResourceId.getUniqueId()+" in file " + parser.getFilePath());
        }

        // this Condition resource id
        ResourceId conditionResourceId = getDiagnosisResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, patientId, parser.getDiagnosisDateTimeAsString(), parser.getConceptCode());

        // create the FHIR Condition
        Condition fhirCondition = new Condition();
        fhirCondition.setId(conditionResourceId.getResourceId().toString());
        fhirCondition.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_CONDITION));
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        String diagnosisID = parser.getDiagnosisID();
        fhirCondition.addIdentifier (IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID, diagnosisID));

        // set the patient reference
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
        if (parser.isActive()) {
            fhirCondition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);
        } else {
            LOG.debug("Delete Condition (PatId=" + patientId + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            return;
        }

        Date diagnosisDateTime = parser.getDiagnosisDateTime();
        if (diagnosisDateTime != null) {
            DateTimeType dateDt = new DateTimeType(diagnosisDateTime);
            fhirCondition.setOnset(dateDt);
        }

        fhirCondition.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));

        String encounterSliceID = parser.getEncounterSliceID();
        fhirCondition.addIdentifier (IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_ENCOUNTER_SLICE_ID, encounterSliceID));

        String nomenClatureID = parser.getNomenclatureID();
        fhirCondition.addIdentifier (IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_NOMENCLATURE_ID, nomenClatureID));

        String personnelID = parser.getPersonnel();
        if (!Strings.isNullOrEmpty(personnelID)) {
            fhirCondition.setAsserter(csvHelper.createPractitionerReference(personnelID));
        }

        // Condition(Diagnosis) is coded either as Snomed or ICD10
        String conceptCodeType = parser.getConceptCodeType();
        String conceptCode = parser.getConceptCode();
        if (!Strings.isNullOrEmpty(conceptCodeType) && !Strings.isNullOrEmpty(conceptCode)) {
            if (conceptCodeType.equalsIgnoreCase("SNOMED")) {
                String term = TerminologyService.lookupSnomedFromConceptId(conceptCode).getTerm();
                CodeableConcept procCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, term, conceptCode);
                fhirCondition.setCode(procCode);
            } else if (conceptCodeType.equalsIgnoreCase("ICD10WHO")) {
                String term = TerminologyService.lookupIcd10CodeDescription(conceptCode);
                CodeableConcept procCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_ICD10, term, conceptCode);
                fhirCondition.setCode(procCode);
            }
        } else {
            LOG.warn("Unable to create codeableConcept for Condition ID: "+diagnosisID);
            return;
        }

        // Diagnosis type (category) is a Cerner Millenium code so lookup
        Long diagnosisTypeCode = parser.getDiagnosisTypeCode();
        if (diagnosisTypeCode != null) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(RdbmsCernerCodeValueRefDal.DIAGNOSIS_TYPE, diagnosisTypeCode, fhirResourceFiler.getServiceId());
            if (cernerCodeValueRef != null) {
                String diagnosisTypeTerm = cernerCodeValueRef.getCodeDispTxt();
                CodeableConcept diagTypeCode = CodeableConceptHelper.createCodeableConcept(BartsCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_TYPE, diagnosisTypeTerm, diagnosisTypeCode.toString());
                fhirCondition.setCategory(diagTypeCode);
            } else {
                // LOG.warn("Diagnosis type code: "+diagnosisTypeCode+" not found in Code Value lookup");
            }
        }

        String diagnosisFreeText = parser.getDiagnosicFreeText();
        fhirCondition.setNotes(diagnosisFreeText);

        // save the resource
        LOG.debug("Save Condition (PatId=" + patientId + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
    }*/
}
