package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.AandeAttendances;
import org.endeavourhealth.transform.bhrut.schema.Spells;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

//import static org.hl7.fhir.instance.model.ResourceType.Encounter;

public class AndEAttendanceTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(AndEAttendanceTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Spells.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    AandeAttendances andeParser = (AandeAttendances) parser;

                    if (andeParser.getLinestatus().equals("delete")) {
                        deleteResource(andeParser, fhirResourceFiler, csvHelper, version);
                    } else {
                        createResources(andeParser, fhirResourceFiler, csvHelper, version);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void deleteResource(AandeAttendances parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getId().toString());
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);
        CsvCell actionCell = parser.getLinestatus();
        if (actionCell.getString().equalsIgnoreCase("Delete")) {
            encounterBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
            return;
        }
    }

    public static void createResources(AandeAttendances parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getId().getString());
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);

        encounterBuilder.setPeriodStart(parser.getArrivalDttm().getDateTime(), parser.getArrivalDttm());
        encounterBuilder.setPeriodEnd(parser.getDischargedDttm().getDateTime(), parser.getDischargedDttm());
        CsvCell org = parser.getHospitalCode();
        Reference orgReference = csvHelper.createOrganisationReference(org.getString());
        encounterBuilder.setServiceProvider(orgReference);

        if (!parser.getReferralSource().isEmpty()) {
            //TODO Need to know possible values. Sample has
            //Self Referred
            //Other
        }

        if (!parser.getArrivalMode().isEmpty()) {
            //TODO Need to know set of values.  S3 data has
            //Ambulance Other LAS
            //Private Transport
            //Other
        }
        //TODO Discharge method? Sample spells file nothing whose values match those in spells file.
        if (!parser.getAttendanceType().isEmpty()) {
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Attendance_Type);
            cc.setText(parser.getAttendanceType().getString(), parser.getAttendanceType());
        }
        if (!parser.getReferralSource().isEmpty()) {
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Attendance_Source);
            cc.setText(parser.getReferralSource().getString(), parser.getReferralSource());
        }
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }


}
