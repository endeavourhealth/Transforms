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


    public void writeUpsert(long id, long patientId, String saltKeyName, String pseudoId) throws Exception {

        super.printRecord(
                OutputContainer.UPSERT,
                "" + id,
                "" + patientId,
                saltKeyName,
                pseudoId
        );
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                String.class,
                String.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "patient_id",
                "salt_key_name",
                "pseudo_id"
        };
    }


}
