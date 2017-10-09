package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class ProcedureTransformer extends BasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);
    public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 org.endeavourhealth.transform.barts.schema.Procedure parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                createProcedureResource(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }


    public static void createProcedureResource(org.endeavourhealth.transform.barts.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler);
        // Patient
        ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare
        ResourceId episodeOfCareResourceId = resolveEpisodeResource(parser.getCurrentState(), primaryOrgHL7OrgOID, null, parser.getLocalPatientId(), parser.getEncounterId().toString(), parser.getFINNo(), fhirResourceFiler, patientResourceId, organisationResourceId, parser.getProcedureDateTime(), EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
        // Encounter
        ResourceId encounterResourceId = resolveEncounterResource(parser.getCurrentState(), primaryOrgHL7OrgOID, null, parser.getLocalPatientId(), parser.getEncounterId().toString(), fhirResourceFiler, patientResourceId, episodeOfCareResourceId, Encounter.EncounterState.FINISHED);
        // this Diagnosis resource id
        ResourceId procedureResourceId = resolveProcedureResourceId(primaryOrgOdsCode, fhirResourceFiler, null, parser.getLocalPatientId(), parser.getEncounterId().toString(), parser.getProcedureDateTimeAsString(), parser.getProcedureCode());

        Procedure fhirProcedure = new Procedure();

        // Turn key into Resource id
        fhirProcedure.setId(procedureResourceId.getResourceId().toString());

        //fhirCondition.addIdentifier().setSystem("http://cerner.com/fhir/cds-unique-id").setValue(parser.getCDSUniqueID());

        fhirProcedure.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));

        // set patient reference
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // status
        fhirProcedure.setStatus(Procedure.ProcedureStatus.COMPLETED);

        // Code
        cc = new CodeableConcept();
        cc.addCoding().setSystem("http://snomed.info/sct").setCode(parser.getProcedureCode());
        fhirProcedure.setCode(cc);

        // Performed date/time
        Timing t = new Timing().addEvent(parser.getProcedureDateTime());
        fhirProcedure.setPerformed(t);

        // set notes
        fhirProcedure.addNotes(new Annotation().setText(parser.getProcedureText()));

        // save resource
        LOG.debug("Save Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirProcedure);

    }

}
