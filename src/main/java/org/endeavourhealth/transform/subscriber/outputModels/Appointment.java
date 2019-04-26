package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Appointment extends AbstractSubscriberCsvWriter {

    public Appointment(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            long organisationId,
                            long patientId,
                            long personId,
                            Long practitionerId,
                            Long scheduleId,
                            Date startDate,
                            Integer plannedDuration,
                            Integer actualDuration,
                            int appointmentStatusConceptId,
                            Integer patientWait,
                            Integer patientDelay,
                            Date sentIn,
                            Date left,
                            String sourceId,
                            Date cancelledDate) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organisationId,
                "" + patientId,
                "" + personId,
                convertLong(practitionerId),
                convertLong(scheduleId),
                convertDate(startDate),
                convertInt(plannedDuration),
                convertInt(actualDuration),
                "" + appointmentStatusConceptId,
                convertInt(patientWait),
                convertInt(patientDelay),
                convertDate(sentIn),
                convertDate(left),
                sourceId,
                convertDate(cancelledDate));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "practitioner_id",
                "schedule_id",
                "start_date",
                "planned_duration",
                "actual_duration",
                "appointment_status_concept_id",
                "patient_wait",
                "patient_delay",
                "date_time_sent_in",
                "date_time_left",
                "source_id",
                "cancelled_date"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.class,
                Long.class,
                Date.class,
                Integer.class,
                Integer.class,
                Integer.TYPE,
                Integer.class,
                Integer.class,
                Date.class,
                Date.class,
                String.class,
                Date.class,
        };
    }
}
