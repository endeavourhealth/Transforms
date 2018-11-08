package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.math.BigDecimal;

public class MedicationAmount extends AbstractPcrCsvWriter {

    public MedicationAmount(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            Integer patientId,
                            String dose,
                            BigDecimal quantityValue,
                            String quantityUnit) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientId,
                dose,
                convertBigDecimal(quantityValue),
                quantityUnit
                );
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "patient_id",
                "dose",
                "quantity_value",
                "quantity_units"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Integer.class,
                String.class,
                BigDecimal.class,
                String.class
        };
    }
}

//            id bigint NOT NULL,
//            patient_id int NOT NULL,
//            dose varchar(255),
//            quantity_value double,
//            quantity_units varchar(255),
