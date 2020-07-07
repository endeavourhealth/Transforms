package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

public class PseudoId extends AbstractEnterpriseCsvWriter {

    public PseudoId(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    @Override
    public void writeDelete(long id) throws Exception {
        super.printRecord(
                OutputContainer.DELETE,
                "" + id);
    }


    public void writeUpsert(long id, long organizationId, long patientId, long personId, String saltKeyName, String pseudoId) throws Exception {

        super.printRecord(
                OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                "" + patientId,
                "" + personId,
                saltKeyName,
                pseudoId
        );
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "salt_key_name",
                "pseudo_id"
        };
    }


}
