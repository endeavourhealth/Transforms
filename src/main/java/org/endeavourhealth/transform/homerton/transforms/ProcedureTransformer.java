package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.models.ResourceId;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.hl7.fhir.instance.model.Annotation;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ProcedureTransformer extends BasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);
    public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 org.endeavourhealth.transform.homerton.schema.Procedure parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                createProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }


    /*
     *
     */
    public static void createProcedure(org.endeavourhealth.transform.homerton.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode ) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // Patient
        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare - Procedure record cannot be linked to an EpisodeOfCare
        // Encounter
        //ResourceId encounterResourceId = getEncounterResourceId( parser.getEncounterId().toString());
        /*
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(parser.getEncounterId().toString());

            createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getAdmissionDateTime(), parser.getDischargeDateTime(), null, Encounter.EncounterClass.INPATIENT);
        }
        */

        // this Diagnosis resource id
        //ResourceId procedureResourceId = getProcedureResourceId(parser.getEncounterId().toString(), parser.getProcedureDateTimeAsString(), parser.getProcedureCode());

        // Procedure Code
        CodeableConcept procedureCode = new CodeableConcept();
        //procedureCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getProcedureText()).setCode(parser.getProcedureCode());

        // Create resource
        Procedure fhirProcedure = new Procedure();
        //createProcedureResource(fhirProcedure, procedureResourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, procedureCode, parser.getProcedureDateTime(), parser.getComment(), null);

        // save resource
        LOG.debug("Save Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
        //savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirProcedure);

    }

    /*
     *
     */
    public static void createProcedureResource(Procedure fhirProcedure, ResourceId procedureResourceId, ResourceId encounterResourceId, ResourceId patientResourceId, Procedure.ProcedureStatus status, CodeableConcept procedureCode, Date procedureDate, String notes, Identifier identifiers[]) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Turn key into Resource id
        fhirProcedure.setId(procedureResourceId.getResourceId().toString());

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirProcedure.addIdentifier(identifiers[i]);
            }
        }

        // Encounter
        if (encounterResourceId != null) {
            fhirProcedure.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }

        // set patient reference
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // status
        fhirProcedure.setStatus(status);

        // Code
        fhirProcedure.setCode(procedureCode);

        // Performed date/time
        //Timing t = new Timing().addEvent(procedureDate);
        DateTimeType dateDt = new DateTimeType(procedureDate);
        fhirProcedure.setPerformed(dateDt);

        // set notes
        if (notes != null) {
            fhirProcedure.addNotes(new Annotation().setText(notes));
        }

    }
}
