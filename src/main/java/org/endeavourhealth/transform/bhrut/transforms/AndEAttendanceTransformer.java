package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.AandeAttendances;
import org.endeavourhealth.transform.bhrut.schema.Spells;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
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
        encounterBuilder.setDeletedAudit(actionCell);

        fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
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

        //the class is Emergency
        encounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        encounterBuilder.setPeriodStart(parser.getArrivalDttm().getDateTime(), parser.getArrivalDttm());
        encounterBuilder.setPeriodEnd(parser.getDischargedDttm().getDateTime(), parser.getDischargedDttm());
        CsvCell org = parser.getHospitalCode();
        Reference orgReference = csvHelper.createOrganisationReference(org.getString());
        encounterBuilder.setServiceProvider(orgReference);

        //TODO - the ATTENDANCE_NUMBER,  maybe an identifier?

        //set the encounter extensions
        CsvCell arrivalModeCell = parser.getArrivalMode();
        if (!arrivalModeCell.isEmpty()) {

            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_AE_Arrival_Mode);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);

            //there are only two NHS codes for Arrival mode:
            //1 - Brought in by Emergency Ambulance (including helicopter/'Air Ambulance') , 2 - Other
            String arrivalModeText = arrivalModeCell.getString();
            if (arrivalModeText.toLowerCase().contains("ambulance")) {
                cc.setCodingCode("1");
                cc.setCodingDisplay("Brought in by Emergency Ambulance (including helicopter/'Air Ambulance')");
            } else {
                cc.setCodingCode("2");
                cc.setCodingDisplay("Other");
            }

            //original text from the extract file
            cc.setText(arrivalModeText, arrivalModeCell);
        }

        CsvCell attendanceTypeCell = parser.getAttendanceType();
        if (!attendanceTypeCell.isEmpty()) {

            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_AE_Attendance_Category);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            if (attendanceTypeCell.getString().equalsIgnoreCase("1")) {

                cc.setCodingCode("1", attendanceTypeCell);
                cc.setCodingCode("First Accident and Emergency Attendance - the first in a series, or the only attendance, in a particular Accident and Emergency Episode");
            } else if (attendanceTypeCell.getString().equalsIgnoreCase("2")) {

                cc.setCodingCode("2", attendanceTypeCell);
                cc.setCodingCode("Follow-up Accident and Emergency Attendance - planned: a subsequent planned attendance at the same department, and for the same incident as the first attendance");
            } else if (attendanceTypeCell.getString().equalsIgnoreCase("3")) {

                cc.setCodingCode("3", attendanceTypeCell);
                cc.setCodingCode("Follow-up Accident and Emergency Attendance - unplanned: a subsequent unplanned attendance at the same department, and for the same incident as the first attendance");
            } else {

                throw new TransformException("Unexpected attendance type/category code: [" + attendanceTypeCell.getString() + "]");
            }
            //there is no original text for this code so do not setText
        }

        CsvCell referralSourceCell = parser.getReferralSource();
        if (!referralSourceCell.isEmpty()) {
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_AE_Attendance_Source);

            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            String nhsReferralSourceCode = convertReferralSourceText(referralSourceCell.getString());
            cc.setCodingCode(nhsReferralSourceCode);
            cc.setCodingDisplay(lookupReferralSource(nhsReferralSourceCode));
            cc.setText(referralSourceCell.getString(), parser.getReferralSource());
        }

        //TODO - analysis data to see if there are sub-encounters possible to model, i.e. treatments
        //TODO - the chief complaint needs capturing

        //TODO - Use RECORDED_OUTCOME for a&e to determine this element?
        //encounterBuilder.setDischargeDisposition(?);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }

    private static String convertReferralSourceText(String referralSourceText) {

        if (referralSourceText.toLowerCase().contains("gp") ||
                referralSourceText.toLowerCase().contains("general medical practitioner") )
            return "00";
        else if (referralSourceText.toLowerCase().contains("self referral") ||
                referralSourceText.toLowerCase().contains("self referred"))
            return "01";
        else if (referralSourceText.toLowerCase().contains("social services"))
            return "02";
        else if (referralSourceText.toLowerCase().contains("emergency services"))
            return "03";
        else if (referralSourceText.toLowerCase().contains("work"))
            return "04";
        else if (referralSourceText.toLowerCase().contains("educational"))
            return "05";
        else if (referralSourceText.toLowerCase().contains("police"))
            return "06";
        else if (referralSourceText.toLowerCase().contains("clinic") ||
                referralSourceText.toLowerCase().contains("hospital") ||
                referralSourceText.toLowerCase().contains("nursing home"))
            return "07";
        else if (referralSourceText.toLowerCase().contains("dentist") ||
                referralSourceText.toLowerCase().contains("general dental practitioner"))
            return "92";
        else if (referralSourceText.toLowerCase().contains("community dental"))
            return "93";

        return "08";   //Other
    }

    private static String lookupReferralSource(String referralSourceCode) {

        switch (referralSourceCode) {
            case "00" : return "GENERAL MEDICAL PRACTITIONER";
            case "01" : return "Self referral";
            case "02" : return "Local Authority Social Services";
            case "03" : return "Emergency services";
            case "04" : return "Work";
            case "05" : return "Educational Establishment";
            case "06" : return "Police";
            case "07" : return "Health Care Provider: same or other";
            case "08" : return "Other";
            case "92" : return "GENERAL DENTAL PRACTITIONER ";
            case "93" : return "Community Dental Service";
            default : return null;
        }
    }
}
