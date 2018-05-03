package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRVisit;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRVisitTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRVisitTransformer.class);

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
    }

    private static void createResource(SRVisit parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell visitId = parser.getRowIdentifier();
        String visitIdUnique = "Visit:"+visitId.getString();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if (!deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.Encounter encounter
                        = (org.hl7.fhir.instance.model.Encounter) csvHelper.retrieveResource(visitIdUnique,
                        ResourceType.Encounter,
                        fhirResourceFiler);

                if (encounter != null) {
                    EncounterBuilder encounterBuilder = new EncounterBuilder(encounter);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
                    return;
                }
            }
        }

        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        appointmentBuilder.setId(visitIdUnique, visitId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientId);

        CsvCell visitDate = parser.getDateBooked();
        if (!visitDate.isEmpty()) {
            appointmentBuilder.setStartDateTime(visitDate.getDate(), visitDate);
        }

        CsvCell visitStaffAssigned = parser.getIDProfileAssigned();
        if (!visitStaffAssigned.isEmpty()) {

            String staffMemberId = csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                    visitStaffAssigned.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            appointmentBuilder.addParticipant(staffReference, Appointment.ParticipationStatus.ACCEPTED, visitStaffAssigned);
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
