package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

public class PcrIdMap extends AbstractPcrCsvWriter {


    public PcrIdMap(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(Integer id,
                            String resourceId,
                            String resourceType,
                            Integer sourceDb
    ) throws Exception {
        super.printRecord(OutputContainer.UPSERT,
                convertInt(id),
                resourceId,
                resourceType,
                convertInt(sourceDb)
        );
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "resourceId",
                "resourceType",
                "sourceDb"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Integer.class,
                String.class,
                String.class,
                Integer.class
        };
    }
}
