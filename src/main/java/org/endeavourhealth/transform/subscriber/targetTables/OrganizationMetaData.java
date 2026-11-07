package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class OrganizationMetaData extends AbstractTargetTable {

    public OrganizationMetaData(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long subscriberOrganizationId) throws Exception {
        super.printRecord(convertBoolean(true),
                "" + subscriberOrganizationId);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                String.class,
		        Date.class,
                Date.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "publishing_software",
                "last_data_to_dds",
		        "last_data_cutoff"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ORGANIZATION_METADATA;
    }

    public void writeUpsert(long subscriberOrganizationId,
                            String publishingSoftware,
			                Date lastDataToDDS,
                            Date lastDataCutoff) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberOrganizationId,
                publishingSoftware,
                convertDateTime(lastDataToDDS),
                convertDateTime(lastDataCutoff));
    }

    @Override
    public String[] getPermittedColumnExclusions() {
        return null;
//        return new String[] {
//          //implement which columns can be excluded from the database write for extra pseudo level blanking
//        };
    }
}