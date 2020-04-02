package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class Appointment extends AbstractTargetTable {

    public Appointment(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organizationId,
                            long patientId,
                            long personId,
                            Long practitionerId,
                            Long scheduleId,
                            Date startDate,
                            Integer plannedDuration,
                            Integer actualDuration,
                            Integer appointmentStatusConceptId,
                            Integer patientWait,
                            Integer patientDelay,
                            Date sentIn,
                            Date left,
                            String sourceId,
                            Date cancelledDate) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertLong(practitionerId),
                convertLong(scheduleId),
                convertDateTime(startDate),
                convertInt(plannedDuration),
                convertInt(actualDuration),
                convertInt(appointmentStatusConceptId),
                convertInt(patientWait),
                convertInt(patientDelay),
                convertDateTime(sentIn),
                convertDateTime(left),
                sourceId,
                convertDateTime(cancelledDate));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
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
    public SubscriberTableId getTableId() {
        return SubscriberTableId.APPOINTMENT;
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.class,
                Long.class,
                Date.class,
                Integer.class,
                Integer.class,
                Integer.class,
                Integer.class,
                Integer.class,
                Date.class,
                Date.class,
                String.class,
                Date.class,
        };
    }
}
