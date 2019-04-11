package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;

public class Location extends AbstractSubscriberCsvWriter {

    public Location(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeUpsert(long id,
                            String name,
                            String typeCode,
                            String typeDesc,
                            String postcode,
                            Long managingOrganisationId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                name,
                typeCode,
                typeDesc,
                postcode,
                convertLong(managingOrganisationId));
    }

    @Override
    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
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
                "save_mode",
                "id",
                "name",
                "type_code",
                "type_desc",
                "postcode",
                "managing_organization_id"
        };
    }

}
