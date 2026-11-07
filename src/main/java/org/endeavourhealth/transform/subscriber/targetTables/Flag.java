package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;

import java.util.Date;
import java.util.List;

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
                            Integer datePrecisionConceptId,
                            boolean isActive,
                            String flagText,
                            SubscriberTransformHelper params) throws Exception {

        //get any configured excluded columns for this table
        List<String> excludedTableColumns
                = params.getSubscriberExcludedColumns(getTableId(), getPermittedColumnExclusions());
        if (excludedTableColumns != null) {
            //check each column set for exclusion and blank
            for (String column: excludedTableColumns) {
                switch (column) {
                    case "flag_text" : flagText = null; break;
                    default : break;
                }
            }
        }

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organisationId,
                "" + patientId,
                "" + personId,
                convertDateTime(effectiveDate), //SD-498
                convertInt(datePrecisionConceptId),
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

    @Override
    public String[] getPermittedColumnExclusions() {
        return new String[] {
          "flag_text"
        };
    }
}