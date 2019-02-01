package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.ProcedurePojo;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedurePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.Procedure)parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(org.endeavourhealth.transform.barts.schema.Procedure parser, BartsCsvHelper csvHelper) throws Exception {

        ProcedurePojo pojo = new ProcedurePojo();
        pojo.setConsultant(parser.getConsultant());
        pojo.setCreate_dt_tm(parser.getCreateDateTime());
        pojo.setUpdatedBy(parser.getUpdatedBy());
        pojo.setEncounterId(parser.getEncounterId());
        pojo.setNotes(parser.getComment());
        csvHelper.getProcedureCache().cachePojo(pojo);
        //TODO - pre-cache all the interesting fields, as CsvCells, that will be needed by the PROCETransformer


    }


   /*public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    createProcedure((org.endeavourhealth.transform.barts.schema.Procedure)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createProcedure(org.endeavourhealth.transform.barts.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        ProcedurePojo pojo = new ProcedurePojo();
        pojo.setConsultant(parser.getConsultant());
    }

/*
    public static void createProcedure(org.endeavourhealth.transform.barts.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, organisationResourceId, null, null, null, null, null);
        // EpisodeOfCare - Procedure record cannot be linked to an EpisodeOfCare
        // Encounter
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE,  parser.getEncounterId().toString());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId().toString());
        }

        createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getAdmissionDateTime(), parser.getDischargeDateTime(), null, Encounter.EncounterClass.INPATIENT);

        // this Diagnosis resource id
        ResourceId procedureResourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId().toString(), parser.getProcedureDateTimeAsString(), parser.getProcedureCode(), 0);

        // Procedure Code
        //CodeableConcept procedureCode = new CodeableConcept();
        //procedureCode.addCoding().setSystem(getCodeSystemName(FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED)).setDisplay(parser.getProcedureText()).setCode(parser.getProcedureCode());
        CodeableConcept procedureCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, parser.getProcedureText(), parser.getProcedureCode());

        // Create resource

        String code = parser.getProcedureCode();
        String term = parser.getProcedureText();
        Date date = parser.getProcedureDateTime();
        String comment = parser.getComment();
        ProcedureBuilder procedureBuilder = createProcedureResource(procedureResourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, code, term, FhirCodeUri.CODE_SYSTEM_SNOMED_CT, date, comment, null, "clinical coding");

        // save resource
        LOG.debug("Save Procedure(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);

    }*/


}
