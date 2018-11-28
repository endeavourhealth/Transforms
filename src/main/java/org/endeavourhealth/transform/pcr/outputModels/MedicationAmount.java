package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.math.BigDecimal;

public class MedicationAmount extends AbstractPcrCsvWriter {


    public MedicationAmount(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(Long id,
                            Long patientId,
                            String dose,
                            BigDecimal quantityValue,
                            String quantityUnits,
                            Long enteredByPractitionerId
    ) throws Exception {
        super.printRecord(OutputContainer.UPSERT,
                convertLong(id),
                convertLong(patientId),
                dose,
                convertBigDecimal(quantityValue),
                quantityUnits,
                convertLong(enteredByPractitionerId)
        );
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "patient_id",
                "dose",
                "quantity_value",
                "quantity_units",
                "entered_by_practitioner_id"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                String.class,
                Long.class,
                Long.class,
                String.class,
                BigDecimal.class,
                String.class,
                Long.class
        };
    }
}
