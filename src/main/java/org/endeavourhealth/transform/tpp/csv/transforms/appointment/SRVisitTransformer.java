package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRVisit;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRVisitTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRVisitTransformer.class);

    public static final String VISIT_ID_PREFIX = "Visit:"; //we can't just use the row ID as it will mix up with appointment row IDs

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRVisit.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRVisit) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRVisit parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell visitId = parser.getRowIdentifier();
        String visitIdUnique = VISIT_ID_PREFIX + visitId.getString();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            Appointment appointment = (Appointment)csvHelper.retrieveResource(visitIdUnique, ResourceType.Encounter);
            if (appointment != null) {
                AppointmentBuilder encounterBuilder = new AppointmentBuilder(appointment);
                encounterBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, encounterBuilder);
            }
            return;
        }

        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        appointmentBuilder.setId(visitIdUnique, visitId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientId);

        CsvCell visitDate = parser.getDateBooked();
        if (!visitDate.isEmpty()) {
            appointmentBuilder.setStartDateTime(visitDate.getDate(), visitDate);
        }

        CsvCell profileIdAssigned = parser.getIDProfileAssigned();
        if (!profileIdAssigned.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdAssigned);
            appointmentBuilder.addParticipant(staffReference, Appointment.ParticipationStatus.ACCEPTED, profileIdAssigned);
        }

        CsvCell visitStatus = parser.getCurrentStatus();
        if (!visitStatus.isEmpty()) {
            if (visitStatus.getString().equalsIgnoreCase("cancelled")) {
                appointmentBuilder.setStatus(Appointment.AppointmentStatus.CANCELLED);
            } else if (visitStatus.getString().equalsIgnoreCase("deferred")) {
                appointmentBuilder.setStatus(Appointment.AppointmentStatus.PENDING);
            } else {
                appointmentBuilder.setStatus(Appointment.AppointmentStatus.FULFILLED);
            }
        }

        CsvCell visitDuration = parser.getDuration();
        if (!visitDuration.isEmpty()) {
            appointmentBuilder.setMinutesDuration(visitDuration.getInt());
        }

        CsvCell followUpDetails = parser.getFollowUpDetails();
        if (!followUpDetails.isEmpty()) {
            appointmentBuilder.setComments(followUpDetails.getString(), followUpDetails);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), appointmentBuilder);
    }
}
