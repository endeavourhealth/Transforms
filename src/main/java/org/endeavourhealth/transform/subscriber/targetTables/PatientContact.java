package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class PatientContact extends AbstractTargetTable {

    public PatientContact(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }



    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Integer.class,
                Integer.class,
                Date.class,
                Date.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "patient_id",
                "use_concept_id",
                "type_concept_id",
                "start_date",
                "end_date",
                "value",
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PATIENT_CONTACT;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                "" + EventLog.EVENT_LOG_DELETE,
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organisationId,
                            long patientId,
                            long personId,
                            Integer useConceptId,
                            Integer typeConceptId,
                            Date startDate,
                            Date endDate,
                            String value) throws Exception {

        super.printRecord(
                getEventTypeDesc(subscriberId),
                "" + subscriberId.getSubscriberId(),
                "" + organisationId,
                "" + patientId,
                "" + personId,
                convertInt(useConceptId),
                convertInt(typeConceptId),
                convertDate(startDate),
                convertDate(endDate),
                value);
    }

}
