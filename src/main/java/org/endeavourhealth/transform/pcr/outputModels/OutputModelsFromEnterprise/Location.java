package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;

public class Location extends AbstractPcrCsvWriter {

    public Location(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeUpsert(long id,
                            long organisation_id,
                            String name,
                            Long type_term_id,
                            long addressId,
                            boolean isActive,
                            Long parentLocationId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                convertLong(organisation_id),
                name,
                convertLong(type_term_id),
                convertLong(addressId),
                convertBoolean(isActive),
                convertLong(parentLocationId));
    }

    @Override
    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                String.class,
                Long.TYPE,
                Long.TYPE,
                Long.class,
                Long.TYPE,
                Long.TYPE,
                boolean.class,
                Long.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "organisation_id",
                "name",
                "type_concept_id",
                "address_id",
                "is_active",
                "parent_location_id"
        };
    }

}
