package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class GpRegistrationHistory extends AbstractPcrCsvWriter {

    //TODO Auto-generated. May need some long/int tweaking etc

    public GpRegistrationHistory(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(Long id,
                            Long patientId,
                            Long owningOrganisationId,
                            Date effectiveDate,
                            Long patientType,
                            Long patientStatus
    ) throws Exception {
        super.printRecord(OutputContainer.UPSERT,
                convertLong(id),
                convertLong(patientId),
                convertLong(owningOrganisationId),
                convertDate(effectiveDate),
                convertLong(patientType),
                convertLong(patientStatus)
        );
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "patient_id",
                "owning_organisation_id",
                "effective_date",
                "patient_type",
                "patient_status"
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
                Long.class,
                Long.class
        };
    }
}
