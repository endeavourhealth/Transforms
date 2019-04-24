package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Schedule extends AbstractSubscriberCsvWriter {

    public Schedule(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            long organisationId,
                            Long practitionerId,
                            Date startDate,
                            String type,
                            String location,
                            String comments) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organisationId,
                convertLong(practitionerId),
                convertDate(startDate),
                type,
                location,
                comments);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "practitioner_id",
                "start_date",
                "type",
                "location",
                "comments"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.TYPE,
                Long.class,
                Date.class,
                String.class,
                String.class,
                String.class
        };
    }
}
