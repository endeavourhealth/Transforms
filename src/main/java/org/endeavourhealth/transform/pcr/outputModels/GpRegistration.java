package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class GpRegistration extends AbstractPcrCsvWriter {

    //TODO Auto-generated. May need some long/int tweaking etc

    public GpRegistration(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(Long id,
                            Long patientId,
                            Long owningOrganisationId,
                            Date startDate,
                            Date endDate
    ) throws Exception {
        super.printRecord(OutputContainer.UPSERT,
                convertLong(id),
                convertLong(patientId),
                convertLong(owningOrganisationId),
                convertDate(startDate),
                convertDate(endDate)
        );
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "patient_id",
                "owning_organisation_id",
                "start_date",
                "end_date"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                String.class,
                Long.class,
                Long.class,
                Long.class,
                Date.class,
                Date.class
        };
    }
}
