package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Duration;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        CsvCell patientIdCell = parser.getPasId();
        encounterBuilder.setId(parser.getId().getString());
        CsvCell visitId = parser.getId();
        String visitIdUnique = VISIT_ID_PREFIX + visitId.getString();


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

        CsvCell org = parser.getHospitalCode();
        Reference orgReference = csvHelper.createOrganisationReference(org.getString());
        encounterBuilder.setServiceProvider(orgReference);

        CsvCell admittingConsultant = parser.getConsultantCode();
        Reference practitioner = csvHelper.createPractitionerReference(admittingConsultant.getString());
        encounterBuilder.addParticipant(practitioner, EncounterParticipantType.CONSULTANT, admittingConsultant);

        //Todo need to verify the Diagnosis code along with 3 Secondary Diagnosis codes.

        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            CsvCell primaryDiagCode = parser.getPrimaryDiagnosisCode();
            ConditionBuilder condition = new ConditionBuilder();
            Reference enc = csvHelper.createEncounterReference(encounterBuilder.getResourceId(), patientReference.getId());
            condition.setEncounter(enc, primaryDiagCode);
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(primaryDiagCode.getString(), primaryDiagCode);
            condition.setCategory("diagnosis");
        }
        //Todo need to verify the PrimaryProcedure code along with 11 SecondaryPrimary codes.

        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            CsvCell primaryProcCode = parser.getPrimaryProcedureCode();
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setEncounter(encounterReference);
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(primaryProcCode.getString(), primaryProcCode);
        }

        // CsvCell secondaryProcCode  = parser.getSecondaryProcedureCode1();


        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        appointmentBuilder.setId(visitIdUnique, visitId);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientIdCell);

        //CsvCell visitDate = parser.getDateBooked();
        CsvCell visitDate = parser.getAppointmentDttm();
        if (!visitDate.isEmpty()) {
            appointmentBuilder.setStartDateTime(visitDate.getDate(), visitDate);
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
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), appointmentBuilder);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }

}