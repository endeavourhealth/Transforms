package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

public class PatientAdditional extends AbstractEnterpriseCsvWriter {

    public PatientAdditional(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            String propertyId,
                            String valueId,
                            String jsonValue
                            ) throws Exception {

        //SD-382 - getting nulls in the property_id field for _additional tables
        if (propertyId == null) {
            throw new Exception("Null propertyId value for patient_additional record for patient " + id);
        }

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                propertyId,
                valueId,
                jsonValue);   }


    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "property_id",
                "value_id",
                "json_value"
        };
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
}