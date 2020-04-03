package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class PatientContact extends AbstractEnterpriseCsvWriter {

    public PatientContact(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    @Override
    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organisationId,
                            long patientId,
                            long personId,
                            Integer useConceptId,
                            Integer typeConceptId,
                            Date startDate,
                            Date endDate,
                            String value) throws Exception {

        super.printRecord(
                OutputContainer.UPSERT,
                "" + subscriberId.getSubscriberId(),
                "" + organisationId,
                "" + patientId,
                "" + personId,
                convertInt(useConceptId),
                convertInt(typeConceptId),
                convertDate(startDate),
                convertDate(endDate),
                value);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Integer.class,
                Integer.class,
                Date.class,
                Date.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "use_concept_id",
                "type_concept_id",
                "start_date",
                "end_date",
                "value",
        };
    }
}
