package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class Flag extends AbstractTargetTable {

    public Flag(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organisationId,
                            long patientId,
                            long personId,
                            Date effectiveDate,
                            Integer datePrecisionId,
                            Boolean isActive,
                            String flagText
    ) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organisationId,
                "" + patientId,
                "" + personId,
                convertDate(effectiveDate),
                convertInt(datePrecisionId),
                convertBoolean(isActive),
                flagText);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "effective_date",
                "date_precision_concept_id",
                "is_active",
                "flag_text"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.FLAG;
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Date.class,
                Integer.class,
                Boolean.class,
                String.class,
        };
    }
}