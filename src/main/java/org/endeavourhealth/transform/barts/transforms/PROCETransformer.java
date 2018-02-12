package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class PROCETransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCETransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 PROCE parser,
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
                    createProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
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
    public static String validateEntry(PROCE parser) {
        return null;
    }


    /*
     *
     */
    public static void createProcedure(PROCE parser,
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
        ResourceId procedureResourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterID().toString(), parser.getProcedureDateTimeAsString(), parser.getConceptCode(), 0);

        Procedure fhirProcedure = new Procedure();
        fhirProcedure.setId(procedureResourceId.getResourceId().toString());
        fhirProcedure.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PROCEDURE));
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
        String procedureID = parser.getProcedureID();
        fhirProcedure.addIdentifier (IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, BartsCsvToFhirTransformer.CODE_SYSTEM_PROCEDURE_ID, procedureID));

        //TODO: get patient from encounter
        //fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
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

        String clinicianID = parser.getClinicianID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            //TODO: need to map to person resource
            Procedure.ProcedurePerformerComponent fhirPerformer = fhirProcedure.addPerformer();
            fhirPerformer.setActor(csvHelper.createPractitionerReference(clinicianID));
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

        // Procedure type (category) is Cerner Millenium code
        Long procedureTypeCode = parser.getProcedureTypeCode();
        if (procedureTypeCode != null) {
            CernerCodeValueRefDalI DAL = DalProvider.factoryCernerCodeValueRefDal();
            Integer codeSet = 401;
            CernerCodeValueRef cernerCodeValueRef = DAL.getCodeFromCodeSet(Long.valueOf(codeSet.longValue()), procedureTypeCode, fhirResourceFiler.getServiceId());
            String procedureTypeTerm = cernerCodeValueRef.getCodeDispTxt();
            CodeableConcept procTypeCode = CodeableConceptHelper.createCodeableConcept(BartsCsvToFhirTransformer.CODE_SYSTEM_PROCEDURE_TYPE, procedureTypeTerm, procedureTypeCode.toString());
            fhirProcedure.setCategory(procTypeCode);
        }

        // save resource
        LOG.debug("Save Procedure (PatId=" + patientId + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirProcedure);
    }
}
