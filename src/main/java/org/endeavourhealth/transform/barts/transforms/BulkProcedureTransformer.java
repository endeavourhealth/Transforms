package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.BulkProcedure;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Procedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class BulkProcedureTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProcedureTransformer.class);
    public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createProcedure((BulkProcedure) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    /*
     *
     */
    public static void createProcedure(BulkProcedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        CsvCell localPatientIdCell = parser.getLocalPatientId();
        String localPatientId = localPatientIdCell.getString();
        ResourceId patientResourceId = getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, localPatientId);
        if (patientResourceId == null) {
            patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, localPatientId, null, null, null, null, null, organisationResourceId, null, null, null, null, null);
        }

        // EpisodeOfCare - Procedure record cannot be linked to an EpisodeOfCare

        // Encounter
        CsvCell encounterIdCell = parser.getEncounterId();
        String encounterId = encounterIdCell.getString().split("\\.")[0];
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId);
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId);

            Date admissionDate = null;
            Date dischargeDate = null;

            CsvCell admissionDateCell = parser.getAdmissionDateTime();
            if (!admissionDateCell.isEmpty()) {
                admissionDate = admissionDateCell.getDate();
            }

            CsvCell dischargeDateCell = parser.getDischargeDateTime();
            if (!dischargeDateCell.isEmpty()) {
                dischargeDate = dischargeDateCell.getDate();
            }

            createEncounter(parser.getCurrentState(), fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, admissionDate, dischargeDate, null, Encounter.EncounterClass.INPATIENT);
        }


        // this Diagnosis resource id
        CsvCell procedureDateCell = parser.getProcedureDateTime();
        CsvCell procedureCodeCell = parser.getProcedureCode();
        ResourceId procedureResourceId = readProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId, procedureDateCell.getString(), procedureCodeCell.getString());
        if (procedureResourceId == null) {
            procedureResourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId, procedureDateCell.getString(), procedureCodeCell.getString(), 0);

            // Create resource
            String code = parser.getProcedureCode().getString();
            String term = parser.getProcedureText().getString();
            Date date = parser.getProcedureDateTime().getDate();
            String comment = parser.getComment().getString();
            ProcedureBuilder procedureBuilder = createProcedureResource(procedureResourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, code, term, FhirCodeUri.CODE_SYSTEM_SNOMED_CT, date, comment, null, "clinical coding");

            // save resource
            LOG.debug("Save Procedure(PatId=" + localPatientId + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
        }
    }


}
