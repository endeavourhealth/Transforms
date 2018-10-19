package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class PatientAddress extends AbstractPcrCsvWriter {



    public PatientAddress(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }


    public void writeUpsert(long id,
                            long patientId,
                            int typeTermId,
                            int addressId,
                            Date startDate,
                            Date endDate) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientId,
                "" + typeTermId,
                "" + addressId,
                convertDate(startDate),
                convertDate(endDate));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "patient_id",
                "addressId",
                "start_date",
                "end_date"
        };
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                String.class,
                Long.TYPE,
                Long.TYPE,
                Integer.TYPE,
                Integer.TYPE,
                Date.class,
                Date.class
        };
    }


}
