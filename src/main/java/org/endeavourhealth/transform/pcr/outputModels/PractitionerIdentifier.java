package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

public class PractitionerIdentifier extends AbstractPcrCsvWriter {

    public PractitionerIdentifier(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }


    public void writeUpsert(long id,
                            long practitionerId,
                            Long type_concept_id,
                            String value
                            ) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + practitionerId,
                "" + type_concept_id,
                value);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "practitioner_id",
                "type_concept_id",
                "value"
        };
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                String.class,

        };
    }
}
