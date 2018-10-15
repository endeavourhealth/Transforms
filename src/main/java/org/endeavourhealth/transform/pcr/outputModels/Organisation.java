package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

public class Organisation extends AbstractPcrCsvWriter {

    public Organisation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            String serviceId,
                            String systemId,
                            String odsCode,
                            String name,
                            String typeCode,
                            boolean isActive,
                            String mainLocationId,
                            Long parentOrganisationId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                serviceId,
                systemId,
                odsCode,
                name,
                convertBoolean(isActive),
                convertLong(parentOrganisationId),
                typeCode,
                mainLocationId
                );
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "service_id",
                "system_id",
                "ods_code",
                "name",
                "is_active",
                "parent_organization_id",
                "type_term_id",
                "main_location_id"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                String.class,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                Long.class
        };
    }

}
