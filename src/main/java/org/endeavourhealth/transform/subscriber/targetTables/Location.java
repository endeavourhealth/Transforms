package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;

import java.util.List;

public class Location extends AbstractTargetTable {

    public Location(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            String name,
                            String typeCode,
                            String typeDesc,
                            String postcode,
                            Long managingOrganisationId,
                            SubscriberTransformHelper params) throws Exception {

        //get any configured excluded columns for this table
        List<String> excludedTableColumns
                = params.getSubscriberExcludedColumns(getTableId(), getPermittedColumnExclusions());
        if (excludedTableColumns != null) {
            //check each column set for exclusion and blank
            for (String column: excludedTableColumns) {
                switch (column) {
                    case "name" : name = null; break;
                    case "type_code" : typeCode = null; break;
                    case "type_desc" : typeDesc = null; break;
                    case "postcode" : postcode = null; break;
                    default : break;
                }
            }
        }

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                name,
                typeCode,
                typeDesc,
                postcode,
                convertLong(managingOrganisationId));
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                String.class,
                Long.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "name",
                "type_code",
                "type_desc",
                "postcode",
                "managing_organization_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.LOCATION;
    }

    @Override
    public String[] getPermittedColumnExclusions() {
        return new String[] {
                "name",
                "type_code",
                "type_desc",
                "postcode"
        };
    }
}
