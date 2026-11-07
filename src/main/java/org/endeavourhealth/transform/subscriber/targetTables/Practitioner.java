package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;

import java.util.List;

public class Practitioner extends AbstractTargetTable {

    public Practitioner(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organizaationId,
                            String name,
                            String roleCode,
                            String roleDesc,
                            String gmcCode,
                            SubscriberTransformHelper params) throws Exception {

        //get any configured excluded columns for this table
        List<String> excludedTableColumns
                = params.getSubscriberExcludedColumns(getTableId(), getPermittedColumnExclusions());
        if (excludedTableColumns != null) {
            //check each column set for exclusion and blank
            for (String column: excludedTableColumns) {
                switch (column) {
                    case "name" : name = null; break;
                    case "role_code" : roleCode = null; break;
                    case "role_desc" : roleDesc = null; break;
                    case "gmc_code" : gmcCode = null; break;
                    default : break;
                }
            }
        }

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizaationId,
                name,
                roleCode,
                roleDesc,
                gmcCode);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "organization_id",
                "name",
                "role_code",
                "role_desc",
                "gmc_code"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PRACTITIONER;
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                String.class
        };
    }

    @Override
    public String[] getPermittedColumnExclusions() {
        return new String[] {
                "name",
                "role_code",
                "role_desc",
                "gmc_code"
        };
    }
}
