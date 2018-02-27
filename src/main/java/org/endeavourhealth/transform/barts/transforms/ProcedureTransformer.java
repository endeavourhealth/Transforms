package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ProcedureTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);
    public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            int lineCount = 0;
            // Skip header line
            parser.nextRecord();
            lineCount++;

            while (parser.nextRecord()) {
                try {
                    lineCount++;
                    String valStr = validateEntry((org.endeavourhealth.transform.barts.schema.Procedure)parser);
                    if (valStr == null) {
                        createProcedure((org.endeavourhealth.transform.barts.schema.Procedure)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                    } else {
                        valStr = "Validation error on line " + Integer.toString(lineCount) + " - " + valStr;
                        LOG.debug("Validation error:" + valStr);
                        SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                    }

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(org.endeavourhealth.transform.barts.schema.Procedure parser) {
        if (parser.getLocalPatientId() == null || parser.getLocalPatientId().length() == 0) {
            return "LocalPatientId not found";
        } else {
            return null;
        }
    }


    /*
     *
     */
    public static void createProcedure(org.endeavourhealth.transform.barts.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
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

        Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding")};

        // Create resource
        Procedure fhirProcedure = new Procedure();
        createProcedureResource(fhirProcedure, procedureResourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, procedureCode, parser.getProcedureDateTime(), parser.getComment(), null, null, ex);

        // save resource
        LOG.debug("Save Procedure(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirProcedure);

    }


}
