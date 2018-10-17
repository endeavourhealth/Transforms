package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Schedule extends AbstractPcrCsvWriter {

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
                            Date scheduleStart,
                            Date scheduleEnd,
                            String type,
                            String location) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organisationId,
                convertLong(practitionerId),
                convertDate(scheduleStart),
                convertDate(scheduleEnd),
                type,
                location);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "practitioner_id",
                "schedule_start",
                "schedule_end",
                "type",
                "location"
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
                Date.class,
                String.class,
                String.class
        };
    }
}
