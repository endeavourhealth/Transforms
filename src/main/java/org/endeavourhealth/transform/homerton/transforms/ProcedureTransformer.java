package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.endeavourhealth.transform.homerton.schema.ProcedureTable;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ProcedureTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);
    public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    String valStr = validateEntry((ProcedureTable) parser);
                    if (valStr == null) {
                        createProcedure((ProcedureTable) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode);
                    } else {
                        TransformWarnings.log(LOG, parser, "Validation error: {}", valStr);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    /*
     *
     */
    public static String validateEntry(ProcedureTable parser) {
        return null;
    }

    public static void createProcedure(ProcedureTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode ) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        CsvCell procedureIdCell = parser.getProcedureId();
        CsvCell encounterIdCell = parser.getEncounterId();

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // PatientTable
        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare - ProcedureTable record cannot be linked to an EpisodeOfCare

        // EncounterTable
        ResourceId encounterResourceId = getEncounterResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, encounterIdCell.toString());
        if (encounterResourceId == null) {
            TransformWarnings.log(LOG, parser, "Skipping Procedure {} because Encounter not found {} could be found in file {}", procedureIdCell.getString(), encounterIdCell.getString(), parser.getFilePath());
            return;
        }

        Encounter encounter = (Encounter)csvHelper.retrieveResource(ResourceType.Encounter, encounterResourceId.getResourceId());

        Reference patientReference = encounter.getPatient();

        // this DiagnosisTable resource id
        //ResourceId procedureResourceId = getProcedureResourceId(parser.getEncounterId().toString(), parser.getProcedureDateTimeAsString(), parser.getProcedureCode());

        // ProcedureTable Code
        //CodeableConcept procedureCode = new CodeableConcept();
        //procedureCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getProcedureText()).setCode(parser.getProcedureCode());

        // Create resource
        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        //createProcedureResource(fhirProcedure, procedureResourceId, encounterResourceId, patientResourceId, ProcedureTable.ProcedureStatus.COMPLETED, procedureCode, parser.getProcedureDateTime(), parser.getComment(), null);

        procedureBuilder.setId(procedureIdCell.getString(), procedureIdCell);

        // set patient reference
        procedureBuilder.setPatient(patientReference, encounterIdCell);

        // save resource
        if (LOG.isTraceEnabled()) {
            LOG.trace("Save ProcedureTable:" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
        }
        savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);

    }


    /*
     *
     */
    /*public static void createProcedure(org.endeavourhealth.transform.homerton.schema.ProcedureTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode ) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // PatientTable
        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare - ProcedureTable record cannot be linked to an EpisodeOfCare
        // EncounterTable
        //ResourceId encounterResourceId = getEncounterResourceId( parser.getEncounterId().toString());
        *//*
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(parser.getEncounterId().toString());

            createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, EncounterTable.EncounterState.FINISHED, parser.getAdmissionDateTime(), parser.getDischargeDateTime(), null, EncounterTable.EncounterClass.INPATIENT);
        }
        *//*

        // this DiagnosisTable resource id
        //ResourceId procedureResourceId = getProcedureResourceId(parser.getEncounterId().toString(), parser.getProcedureDateTimeAsString(), parser.getProcedureCode());

        // ProcedureTable Code
        //CodeableConcept procedureCode = new CodeableConcept();
        //procedureCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getProcedureText()).setCode(parser.getProcedureCode());

        // Create resource
        ProcedureTable fhirProcedure = new ProcedureTable();
        //createProcedureResource(fhirProcedure, procedureResourceId, encounterResourceId, patientResourceId, ProcedureTable.ProcedureStatus.COMPLETED, procedureCode, parser.getProcedureDateTime(), parser.getComment(), null);

        fhirProcedure.setId(parser.getProcedureId());

        // set patient reference
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.PatientTable, parser.getCNN()));

        // save resource
        LOG.debug("Save ProcedureTable:" + FhirSerializationHelper.serializeResource(fhirProcedure));
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

        // EncounterTable
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
