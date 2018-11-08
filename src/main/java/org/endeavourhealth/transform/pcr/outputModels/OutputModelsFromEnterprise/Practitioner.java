package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;

public class Practitioner extends AbstractPcrCsvWriter {

    public Practitioner(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }


    public void writeUpsert(long id,
                            long organisationId,
                            String title,
                            String firstName,
                            String middleName,
                            String lastName,
                            int genderTermId,
                            Date dateOfBirth,
                            boolean isActive,
                            String roleTermId,
                            String specialtyTermId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + convertLong(organisationId),
                title,
                firstName,
                middleName,
                lastName,
                convertInt(genderTermId),
                convertDate(dateOfBirth),
                convertBoolean(isActive),
                roleTermId,
                specialtyTermId);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "title",
                "first_name",
                "middle_name",
                "last_name",
                "gender_term_id",
                "date_of_birth",
                "is_active",
                "role_term_id",
                "speciality_term_id"
        };
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                Long.TYPE,
                Date.class,
                boolean.class,
                Long.TYPE,
                Long.TYPE

        };
    }
}
