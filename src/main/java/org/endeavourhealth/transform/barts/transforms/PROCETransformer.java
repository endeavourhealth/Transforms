package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PROCETransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCETransformer.class);


    public static void transform(String version,
                                 PROCE parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                createProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createProcedure(PROCE parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // Encounter (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterID();
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        if (encounterResourceId == null) {
            LOG.warn("Skipping Procedure " + parser.getProcedureID().getString() + " due to missing encounter");
            return;
        }

        // get patient from encounter
        Encounter fhirEncounter = (Encounter) csvHelper.retrieveResource(encounterResourceId.getUniqueId(), ResourceType.Encounter);
        String patientReferenceValue = fhirEncounter.getPatient().getReference();

        // this Procedure resource id
        CsvCell procedureIdCell = parser.getProcedureID();
        ResourceId procedureResourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, procedureIdCell);

        // create the FHIR Procedure
        ProcedureBuilder procedureBuilder = new ProcedureBuilder();

        procedureBuilder.setId(procedureResourceId.getResourceId().toString(), procedureIdCell);

        // set the patient reference
        Reference patientReference = ReferenceHelper.createReference(patientReferenceValue);
        procedureBuilder.setPatient(patientReference);


        if (!procedureIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_PROCEDURE_ID);
            identifierBuilder.setValue(procedureIdCell.getString(), procedureIdCell);
        }

        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            LOG.debug("Delete Procedure (" + procedureBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
            return;
        }

        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

        CsvCell procedureDateTimeCell = parser.getProcedureDateTime();
        if (!procedureDateTimeCell.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(procedureDateTimeCell.getDate());
            procedureBuilder.setPerformed(dateTimeType, procedureDateTimeCell);
        }

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString());
        procedureBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
        if (!encounterSliceIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
        }

        CsvCell nomenclatureIdCell = parser.getNomenclatureID();
        if (!nomenclatureIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_NOMENCLATURE_ID);
            identifierBuilder.setValue(nomenclatureIdCell.getString(), nomenclatureIdCell);
        }

        CsvCell personnelIdCell = parser.getPersonnelID();
        if (!personnelIdCell.isEmpty()) {
            ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelIdCell.getString());
            Reference practitionerReference = csvHelper.createPractitionerReference(practitionerResourceId.getResourceId().toString());
            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
        }

        // Procedure is coded either Snomed or OPCS4
        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
        if (!conceptIdentifierCell.isEmpty()) {
            String conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
            String conceptCodeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(conceptIdentifierCell);

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, ProcedureBuilder.TAG_CODEABLE_CONCEPT_CODE);

            if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                String term = TerminologyService.lookupSnomedFromConceptId(conceptCode).getTerm();

                codeableConceptBuilder.addCoding(FhirUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);

                codeableConceptBuilder.addCoding(FhirUri.CODE_SYSTEM_OPCS4, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
            }

        } else {
            //LOG.warn("Unable to create codeableConcept for Procedure ID: "+procedureIdCell);
            return;
        }

        // Procedure type (category) is a Cerner Millenium code so lookup
        CsvCell procedureTypeCodeCell = parser.getProcedureTypeCode();
        if (!procedureTypeCodeCell.isEmpty()) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                        CernerCodeValueRef.PROCEDURE_TYPE,
                                                                        procedureTypeCodeCell.getLong(),
                                                                        fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                String procedureTypeTerm = cernerCodeValueRef.getCodeDispTxt();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, ProcedureBuilder.TAG_CODEABLE_CONCEPT_CATEGORY);

                codeableConceptBuilder.addCoding(BartsCsvToFhirTransformer.CODE_SYSTEM_PROCEDURE_TYPE);
                codeableConceptBuilder.setCodingCode(procedureTypeCodeCell.getString(), procedureTypeCodeCell);
                codeableConceptBuilder.setCodingDisplay(procedureTypeTerm); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(procedureTypeTerm); //don't pass in the cell as this is derived

            } else {
                // LOG.warn("Procedure type code: "+procedureTypeCode+" not found in Code Value lookup");
            }
        }

        // save resource
        LOG.debug("Save Procedure (" + procedureBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
    }

    /*public static void createProcedure(PROCE parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        // Encounter (should already have been created previously)
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

        // this Procedure resource id
        ResourceId procedureResourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterID().toString(), parser.getProcedureDateTimeAsString(), parser.getConceptCode(), 0);

        // create the FHIR Procedure
        Procedure fhirProcedure = new Procedure();
        fhirProcedure.setId(procedureResourceId.getResourceId().toString());
        fhirProcedure.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PROCEDURE));
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
        String procedureID = parser.getProcedureID();
        fhirProcedure.addIdentifier (IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_PROCEDURE_ID, procedureID));

        // set the patient reference
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
        if (parser.isActive()) {
            fhirProcedure.setStatus(Procedure.ProcedureStatus.COMPLETED);
        } else {
            LOG.debug("Delete Procedure (PatId=" + patientId + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirProcedure);
            return;
        }

        Date procedureDateTime = parser.getProcedureDateTime();
        if (procedureDateTime != null) {
            DateTimeType dateDt = new DateTimeType(procedureDateTime);
            fhirProcedure.setPerformed(dateDt);
        }

        fhirProcedure.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));

        String encounterSliceID = parser.getEncounterSliceID();
        fhirProcedure.addIdentifier (IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_ENCOUNTER_SLICE_ID, encounterSliceID));

        String nomenClatureID = parser.getNomenclatureID();
        fhirProcedure.addIdentifier (IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_NOMENCLATURE_ID, nomenClatureID));

        String personnelID = parser.getPersonnelID();
        if (!Strings.isNullOrEmpty(personnelID)) {
            Procedure.ProcedurePerformerComponent fhirPerformer = fhirProcedure.addPerformer();
            fhirPerformer.setActor(csvHelper.createPractitionerReference(personnelID));
        }

        // Procedure is coded either Snomed or OPCS4
        String conceptCodeType = parser.getConceptCodeType();
        String conceptCode = parser.getConceptCode();
        if (!Strings.isNullOrEmpty(conceptCodeType) && !Strings.isNullOrEmpty(conceptCode)) {
            if (conceptCodeType.equalsIgnoreCase("SNOMED")) {
                String term = TerminologyService.lookupSnomedFromConceptId(conceptCode).getTerm();
                CodeableConcept procCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, term, conceptCode);
                fhirProcedure.setCode(procCode);
            } else if (conceptCodeType.equalsIgnoreCase("OPCS4")) {
                String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);
                CodeableConcept procCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_OPCS4, term, conceptCode);
                fhirProcedure.setCode(procCode);
            }
        } else {
            LOG.warn("Unable to create codeableConcept for Procedure ID: "+procedureID);
            return;
        }

        // Procedure type (category) is a Cerner Millenium code so lookup
        Long procedureTypeCode = parser.getProcedureTypeCode();
        if (procedureTypeCode != null) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(RdbmsCernerCodeValueRefDal.PROCEDURE_TYPE, procedureTypeCode, fhirResourceFiler.getServiceId());
            if (cernerCodeValueRef != null) {
                String procedureTypeTerm = cernerCodeValueRef.getCodeDispTxt();
                CodeableConcept procTypeCode = CodeableConceptHelper.createCodeableConcept(BartsCsvToFhirTransformer.CODE_SYSTEM_PROCEDURE_TYPE, procedureTypeTerm, procedureTypeCode.toString());
                fhirProcedure.setCategory(procTypeCode);
            } else {
                // LOG.warn("Procedure type code: "+procedureTypeCode+" not found in Code Value lookup");
            }
        }

        // save resource
        LOG.debug("Save Procedure (PatId=" + patientId + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirProcedure);
    }*/
}
