package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class PatientIdentifier extends AbstractPcrCsvWriter {



    public PatientIdentifier(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }


    public void writeUpsert(long id,
                            long patientId,
                            int conceptId,
                            String identifier) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientId,
                "" + conceptId,
                identifier);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "patient_id",
                "concept_id",
                "identifier"
        };
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                String.class,
                Long.TYPE,
                Long.TYPE,
                Integer.TYPE,
                String.class
        };
    }


}