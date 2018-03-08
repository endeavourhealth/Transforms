package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ProcedureTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);
    public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 org.endeavourhealth.transform.homerton.schema.Procedure parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper,
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

    public static void createProcedure(org.endeavourhealth.transform.homerton.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper,
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
        //CodeableConcept procedureCode = new CodeableConcept();
        //procedureCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getProcedureText()).setCode(parser.getProcedureCode());

        // Create resource
        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        //createProcedureResource(fhirProcedure, procedureResourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, procedureCode, parser.getProcedureDateTime(), parser.getComment(), null);

        CsvCell procedureIdCell = parser.getProcedureId();
        procedureBuilder.setId(procedureIdCell.getString(), procedureIdCell);

        // set patient reference
        CsvCell cnnCell = parser.getCNN();
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, cnnCell.getString());
        procedureBuilder.setPatient(patientReference, cnnCell);

        // save resource
        if (LOG.isTraceEnabled()) {
            LOG.trace("Save Procedure:" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
        }
        savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);

    }


    /*
     *
     */
    /*public static void createProcedure(org.endeavourhealth.transform.homerton.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode ) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // Patient
        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare - Procedure record cannot be linked to an EpisodeOfCare
        // Encounter
        //ResourceId encounterResourceId = getEncounterResourceId( parser.getEncounterId().toString());
        *//*
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(parser.getEncounterId().toString());

            createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getAdmissionDateTime(), parser.getDischargeDateTime(), null, Encounter.EncounterClass.INPATIENT);
        }
        *//*

        // this Diagnosis resource id
        //ResourceId procedureResourceId = getProcedureResourceId(parser.getEncounterId().toString(), parser.getProcedureDateTimeAsString(), parser.getProcedureCode());

        // Procedure Code
        //CodeableConcept procedureCode = new CodeableConcept();
        //procedureCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getProcedureText()).setCode(parser.getProcedureCode());

        // Create resource
        Procedure fhirProcedure = new Procedure();
        //createProcedureResource(fhirProcedure, procedureResourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, procedureCode, parser.getProcedureDateTime(), parser.getComment(), null);

        fhirProcedure.setId(parser.getProcedureId());

        // set patient reference
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, parser.getCNN()));

        // save resource
        LOG.debug("Save Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
        savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);

    }*/

    /*
     *
     */
    public static void createProcedureResource(Procedure fhirProcedure, ResourceId procedureResourceId, ResourceId encounterResourceId, ResourceId patientResourceId, Procedure.ProcedureStatus status, CodeableConcept procedureCode, Date procedureDate, String notes, Identifier identifiers[]) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Turn key into Resource id


        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirProcedure.addIdentifier(identifiers[i]);
            }
        }

        // Encounter
        if (encounterResourceId != null) {
            fhirProcedure.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }


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
