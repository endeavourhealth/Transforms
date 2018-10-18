package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.math.BigDecimal;
import java.util.Date;

public class ObservationValue extends AbstractPcrCsvWriter {

    public ObservationValue(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(Integer patientId,
                            long observationId,
                            Integer operatorConceptId,
                            BigDecimal resultValue,
                            String resultValueUnits,
                            Date resultDate,
                            String resultText,
                            Integer resultConceptId,
                            long referenceRangeId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + patientId,
                "" + observationId,
                convertInt(operatorConceptId),
                convertBigDecimal(resultValue),
                resultValueUnits,
                convertDate(resultDate),
                resultText,
                convertInt(resultConceptId),
                convertLong(referenceRangeId));
    }


    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "patient_id",
                "observation_id",
                "operator_concept_id",
                "result_value",
                "result_value_units",
                "result_date",
                "result_text",
                "result_concept_id",
                "reference_range_id"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Integer.class,
                Long.TYPE,
                Integer.class,
                BigDecimal.class,
                String.class,
                Date.class,
                String.class,
                Integer.class,
                Long.TYPE
        };

    }
}

//                patient_id int NOT NULL,
//                observation_id bigint NOT NULL COMMENT 'refers to the observation this belongs to',
//                operator_concept_id int COMMENT 'refers to information model, giving operator (e.g. =, <, <=, >, >=)',
//                result_value double,
//                result_value_units varchar(255),
//                result_date datetime DEFAULT NULL,
//                result_text text DEFAULT NULL,
//                result_concept_id int(20) DEFAULT NULL,
//                reference_range_id bigint COMMENT 'refers to reference_range table in information model',