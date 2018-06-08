package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

public class LinkDistributor extends AbstractEnterpriseCsvWriter {

    public LinkDistributor(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeUpsert(String sourceSkid,
                     String targetSaltKeyName,
                     String targetSkid) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + sourceSkid,
                "" + targetSaltKeyName,
                "" + targetSkid);
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
                String.class,
                String.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "source_skid",
                "target_salt_key_name",
                "target_skid"
        };
    }
}
