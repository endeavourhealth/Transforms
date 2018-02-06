package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.DIAGN;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class DIAGNTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGNTransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 DIAGN parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createCondition(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(DIAGN parser) {
        return null;
    }


    /*
     *
     */
    public static void createCondition(DIAGN parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Encounter (should already have been created previously)
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterID());
        //if (encounterResourceId == null) {
        //    encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterID());
        //    createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getProcedureDateTime(), parser.getProcedureDateTime(), null, Encounter.EncounterClass.OTHER);
        //}

        // Patient
        //TODO: get patient from encounter
        String patientId = "TODO";
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(patientId)};
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, patientId, null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);

        // this Procedure resource id
        ResourceId procedureResourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterID().toString(), parser.getDiagnosisDateTimeAsString(), parser.getConceptCode(), 0);

        Condition fhirCondition = new Condition();
        fhirCondition.setId(procedureResourceId.getResourceId().toString());
        fhirCondition.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_CONDITION));
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
        String diagnosisID = parser.getDiagnosisID();
        fhirCondition.addIdentifier (IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID, diagnosisID));

        //TODO: get patient from encounter
        //fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
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

        String clinicianID = parser.getClinicianID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            //TODO: need to map to person resource
            fhirCondition.setAsserter(csvHelper.createPractitionerReference(clinicianID));
        }

        // Procedure is coded either Snomed or OPCS4
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

        // Diagnosis type (category) is Cerner Millenium code
        String diagnosisTypeCode = parser.getDiagnosisTypeCode();
        if (!Strings.isNullOrEmpty(diagnosisTypeCode)) {
            //TODO: lookup DB and function for Millenium code types
            String diagnosisTypeTerm = "TODO"; //TerminologyService.lookupTermFromMilleniumCodeType(diagnosisTypeCode);
            CodeableConcept diagTypeCode = CodeableConceptHelper.createCodeableConcept(BartsCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_TYPE, diagnosisTypeTerm, diagnosisTypeCode);
            fhirCondition.setCategory(diagTypeCode);
        }

        String diagnosisFreeText = parser.getDiagnosicFreeText();
        fhirCondition.setNotes(diagnosisFreeText);

        // save resource
        LOG.debug("Save Procedure (PatId=" + patientId + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
    }
}
