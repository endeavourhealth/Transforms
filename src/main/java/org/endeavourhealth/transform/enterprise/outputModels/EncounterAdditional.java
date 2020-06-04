package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

public class EncounterAdditional extends AbstractEnterpriseCsvWriter {

    public EncounterAdditional(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            String propertyId,
                            String valueId
                            ) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + propertyId,
                "" + valueId);   }


    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "property_id",
                "value_id",
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                String.class,
                String.class
        };
    }
}