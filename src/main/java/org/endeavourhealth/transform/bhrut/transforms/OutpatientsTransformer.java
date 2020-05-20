package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Outpatients;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.Map;

public class OutpatientsTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OutpatientsTransformer.class);
    public static final String VISIT_ID_PREFIX = "Visit:";

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(org.endeavourhealth.transform.bhrut.schema.Outpatients.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((org.endeavourhealth.transform.bhrut.schema.Outpatients) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(org.endeavourhealth.transform.bhrut.schema.Outpatients parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        CsvCell visitId = parser.getId();
        String visitIdUnique = VISIT_ID_PREFIX + visitId.getString();
        appointmentBuilder.setId(visitIdUnique, visitId);
        SlotBuilder slotBuilder = new SlotBuilder();
        slotBuilder.setId(visitIdUnique, visitId);
        CsvCell idCell = parser.getId();
        CsvCell patientIdCell = parser.getPasId();
        CsvCell staffIdCell = parser.getConsultantCode();
        Reference staffReference = csvHelper.createPractitionerReference(staffIdCell.getString());
        encounterBuilder.setId(idCell.toString());


        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell actionCell = parser.getLinestatus();
        if (actionCell.getString().equalsIgnoreCase("Delete")) {
            encounterBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
            return;
        }

        //link the consultation to our episode of care
        Reference episodeReference = csvHelper.createEpisodeReference(patientIdCell);
        encounterBuilder.setEpisodeOfCare(episodeReference);
        //we have no status field in the source data, but will only receive completed encounters, so we can infer this
        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);
        Reference encounterReference = csvHelper.createEncounterReference(parser.getId().getString(), patientIdCell.getString());

        Reference slotRef = csvHelper.createSlotReference(patientIdCell, parser.getId());
        appointmentBuilder.addSlot(slotRef, parser.getId());


        CsvCell org = parser.getHospitalCode();
        Reference orgReference = csvHelper.createOrganisationReference(org.getString());
        encounterBuilder.setServiceProvider(orgReference);

        CsvCell admittingConsultant = parser.getConsultantCode();
        Reference practitioner = csvHelper.createPractitionerReference(admittingConsultant.getString());
        encounterBuilder.addParticipant(practitioner, EncounterParticipantType.CONSULTANT, admittingConsultant);

        Reference thisEncounter = csvHelper.createEncounterReference(parser.getId().getString(), patientReference.getId());
        ConditionBuilder condition = new ConditionBuilder();
        condition.setId(idCell.getString() + "Condition:0");
        condition.setPatient(patientReference, patientIdCell);
        condition.setEncounter(thisEncounter, parser.getId());
        CodeableConceptBuilder code = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
        code.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
        code.setCodingCode(parser.getPrimaryDiagnosisCode().getString(), parser.getPrimaryDiagnosisCode());
        condition.setCode(code.getCodeableConcept(), parser.getPrimaryDiagnosisCode());
        condition.setClinician(staffReference, staffIdCell);

        //PrimaryProcedureCode
        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setIsPrimary(true);
            procedureBuilder.setId(parser.getId().getString(), parser.getId());
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition,
                    CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(parser.getPrimaryProcedureCode().getString(),
                    parser.getPrimaryProcedureCode());
        }

        //PrimaryDiagnosisCode
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setIsPrimary(true);
            procedureBuilder.setId(parser.getId().getString(), parser.getId());
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition,
                    CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(parser.getPrimaryDiagnosisCode().getString(),
                    parser.getPrimaryDiagnosisCode());
        }

        //SecondaryProcedureCode
        for (int i = 1; i <= 11; i++) {
            Method method = Outpatients.class.getDeclaredMethod("getSecondaryProcedureCode" + i);
            CsvCell procCode = (CsvCell) method.invoke(parser);
            if (!procCode.isEmpty()) {
                ConditionBuilder cc = new ConditionBuilder((Condition) condition.getResource());
                cc.setId(idCell.getString() + "Condition:" + i);
                cc.removeCodeableConcept(CodeableConceptBuilder.Tag.Condition_Main_Code, null);
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
                codeableConceptBuilder.setCodingCode(procCode.getString(), procCode);
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), cc);
            } else {
                break;  //No point parsing empty cells.
            }
        }

        //SecondaryDiagnosisCode
        for (int i = 1; i <= 3; i++) {
            Method method = Outpatients.class.getDeclaredMethod("getDiag" + i);
            CsvCell diagCode = (CsvCell) method.invoke(parser);
            if (!diagCode.isEmpty()) {
                ConditionBuilder cc = new ConditionBuilder((Condition) condition.getResource());
                cc.setId(idCell.getString() + "Diag:" + i);
                cc.removeCodeableConcept(CodeableConceptBuilder.Tag.Condition_Main_Code, null);
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
                codeableConceptBuilder.setCodingCode(diagCode.getString(), diagCode);
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), cc);
            } else {
                break;  //No point parsing empty cells. Assume non-empty cells are sequential.
            }
        }


        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientIdCell);

        //CsvCell visitDate = parser.getDateBooked();
        CsvCell visitDate = parser.getAppointmentDttm();
        if (!visitDate.isEmpty()) {
            slotBuilder.setStartDateTime(visitDate.getDate(), visitDate);
            appointmentBuilder.setStartDateTime(visitDate.getDate(), visitDate);
        }

        CsvCell endDateCell = parser.getApptDepartureDttm();
        Date endDateTime = null;
        if (!endDateCell.isEmpty()) {
            endDateTime = endDateCell.getDateTime();
            slotBuilder.setEndDateTime(endDateTime, endDateCell);
            appointmentBuilder.setEndDateTime(endDateTime, endDateCell);
        }

        //Todo check Appointment Type
        CsvCell telephoneApptCell = parser.getApptType();
        if (telephoneApptCell.getBoolean()) {
            appointmentBuilder.setType("Telephone Appointment", telephoneApptCell);
        }

        CsvCell bookingDateCell = parser.getBookedDttm();
        if (!bookingDateCell.isEmpty()) {
            Date d = bookingDateCell.getDateTime();
            appointmentBuilder.setBookedDateTime(d, bookingDateCell);
        }

        CsvCell cancelledDateCell = parser.getCancelDttm();
        if (!cancelledDateCell.isEmpty()) {
            Date d = cancelledDateCell.getDateTime();
            appointmentBuilder.setCancelledDateTime(d, cancelledDateCell);
        }

        CsvCell patientSeenDateCell = parser.getApptSeenDttm();
        if (!patientSeenDateCell.isEmpty()) {
            Date d = patientSeenDateCell.getDateTime();
            appointmentBuilder.setSentInDateTime(d, patientSeenDateCell);
        }

        //calculate the delay as the difference between the scheduled start and actual start
        if (!visitDate.isEmpty()
                && !patientSeenDateCell.isEmpty()) {
            Date dtScheduledStart = visitDate.getDateTime();
            Date dtSeen = patientSeenDateCell.getDateTime();
            long msDiff = dtSeen.getTime() - dtScheduledStart.getTime();
            if (msDiff >= 0) { //if patient was seen early, then there's no delay
                long minDiff = msDiff / (1000L * 60L);
                Duration fhirDuration = QuantityHelper.createDuration(new Integer((int) minDiff), "minutes");
                appointmentBuilder.setPatientDelay(fhirDuration, visitDate, patientSeenDateCell);
            }
        }

        //calculate the total patient wait as the difference between the arrival time and when they were seen
        //CsvCell dtArrivedCell = parser.getDatePatientArrival();
        CsvCell dtArrivedCell = parser.getApptArrivalDttm();
        if (!dtArrivedCell.isEmpty()
                && !patientSeenDateCell.isEmpty()) {

            Date dtArrived = dtArrivedCell.getDateTime();
            Date dtSeen = patientSeenDateCell.getDateTime();
            long msDiff = dtSeen.getTime() - dtArrived.getTime();
            long minDiff = msDiff / (1000L * 60L);
            Duration fhirDuration = QuantityHelper.createDuration(new Integer((int) minDiff), "minutes");
            appointmentBuilder.setPatientWait(fhirDuration, dtArrivedCell, patientSeenDateCell);
        }

        //CsvCell visitStatus = parser.getCurrentStatus();
        CsvCell visitStatus = parser.getAppointmentStatus();
        if (!visitStatus.isEmpty()) {
            if (visitStatus.getString().equalsIgnoreCase("cancelled")) {
                appointmentBuilder.setStatus(Appointment.AppointmentStatus.CANCELLED);
            } else if (visitStatus.getString().equalsIgnoreCase("deferred")) {
                appointmentBuilder.setStatus(Appointment.AppointmentStatus.PENDING);
            } else {
                appointmentBuilder.setStatus(Appointment.AppointmentStatus.FULFILLED);
            }
        }
        //CsvCell followUpDetails = parser.getFollowUpDetails();
        CsvCell followUpDetails = parser.getAppointmentOutcome();
        if (!followUpDetails.isEmpty()) {
            appointmentBuilder.setComments(followUpDetails.getString(), followUpDetails);
        }

        //CsvCell visitDuration = parser.getDuration();
        CsvCell visitDuration = parser.getApptDepartureDttm();
        if (!visitDuration.isEmpty()) {
            appointmentBuilder.setMinutesActualDuration(visitDuration.getInt());
        }

        if (!parser.getAdminCategoryCode().isEmpty()) {
            CsvCell adminCategoryCode = parser.getAdminCategoryCode();
            CsvCell adminCategory = parser.getAdminCategory();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admin_Category);
            cc.setText(adminCategory.getString(), adminCategory);
            cc.addCoding(FhirExtensionUri.ENCOUNTER_ADMIN_CATEGORY);
            cc.setCodingCode(adminCategoryCode.getString(), adminCategoryCode);
            cc.setCodingDisplay(adminCategory.getString(), adminCategory);
        }

        if (!parser.getAppointmentStatusCode().isEmpty()) {
            CsvCell appointmentStatusCode = parser.getAppointmentStatusCode();
            CsvCell appointmentStatus = parser.getAppointmentStatus();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Appointment_Attended);
            cc.setText(appointmentStatus.getString(), appointmentStatus);
            cc.addCoding(FhirExtensionUri.ENCOUNTER_APPOINTMENT_ATTENDED);
            cc.setCodingCode(appointmentStatusCode.getString(), appointmentStatusCode);
            cc.setCodingDisplay(appointmentStatus.getString(), appointmentStatus);
        }
        if (!parser.getAppointmentOutcomeCode().isEmpty()) {
            CsvCell appointmentOutcomeCode = parser.getAppointmentOutcomeCode();
            CsvCell appointmentOutcome = parser.getAppointmentOutcome();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Appointment_Outcome);
            cc.setText(appointmentOutcome.getString(), appointmentOutcome);
            cc.addCoding(FhirExtensionUri.ENCOUNTER_APPOINTMENT_OUTCOME);
            cc.setCodingCode(appointmentOutcomeCode.getString(), appointmentOutcomeCode);
            cc.setCodingDisplay(appointmentOutcome.getString(), appointmentOutcome);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), appointmentBuilder, condition, slotBuilder);
    }

}