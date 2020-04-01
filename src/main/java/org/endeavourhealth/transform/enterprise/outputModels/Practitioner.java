package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

public class Practitioner extends AbstractEnterpriseCsvWriter {

    public Practitioner(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }


    public void writeUpsert(long id,
                            long organizationId,
                            String name,
                            String roleCode,
                            String roleDesc,
                            String gmcCode) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                name,
                roleCode,
                roleDesc,
                gmcCode);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "name",
                "role_code",
                "role_desc",
                "gmc_code"
        };
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                String.class
        };
    }
}
