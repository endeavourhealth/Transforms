package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class EventLog extends AbstractPcrCsvWriter {


    public EventLog(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
        //TODO Should this just be a no-op? Why would we delete log records?
    }

    public void writeUpsert(long id,
                            Long organisation_id,
                            Date entry_date,
                            Long entry_practitioner_id,
                            Long device_id,
                            int entry_mode,
                            String table_name,
                            Long item_id) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                convertLong(organisation_id),
                convertDate(entry_date),
                convertLong(entry_practitioner_id),
                convertLong(device_id),
        convertInt(entry_mode),
         table_name,
        convertLong(item_id));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "organisation_id",
                "entry_date",
                "entry_practitioner_id",
                "device_id",
                "entry_mode",
                "table_name",
                "item_id"
        };
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Long.TYPE,
                Long.TYPE,
                Date.class,
                Long.TYPE,
                Long.TYPE,
                Integer.TYPE,
                String.class,
                Long.TYPE
        };
    }


}
