package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

public class FreeText extends AbstractPcrCsvWriter {


    public FreeText(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(Long id,
                            Integer patient_id,
                            String free_text
    ) throws Exception {
        super.printRecord(OutputContainer.UPSERT,
                convertLong(id),
                convertInt(patient_id),
                free_text
        );
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "patient_id",
                "free_text"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Long.class,
                Integer.class,
                String.class
        };
    }
}
