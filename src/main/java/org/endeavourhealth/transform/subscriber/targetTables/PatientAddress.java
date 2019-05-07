package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class PatientAddress extends AbstractTargetTable {
    public PatientAddress(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                Integer.class,
                Date.class,
                Date.class,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "address_line_1",
                "address_line_2",
                "address_line_3",
                "address_line_4",
                "city",
                "postcode",
                "use_concept_id",
                "start_date",
                "end_date",
                "lsoa_2001_code",
                "lsoa_2011_code",
                "msoa_2001_code",
                "msoa_2011_code",
                "ward_code",
                "local_authority_code"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PATIENT_ADDRESS;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organisationId,
                            long patientId,
                            long personId,
                            String addressLine1,
                            String addressLine2,
                            String addressLine3,
                            String addressLine4,
                            String city,
                            String postcode,
                            Integer useConceptId,
                            Date startDate,
                            Date endDate,
                            String lsoa2001,
                            String lsoa2011,
                            String msoa2001,
                            String msoa2011,
                            String ward,
                            String localAuthority) throws Exception {

        super.printRecord(
                convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organisationId,
                "" + patientId,
                "" + personId,
                addressLine1,
                addressLine2,
                addressLine3,
                addressLine4,
                city,
                postcode,
                convertInt(useConceptId),
                convertDate(startDate),
                convertDate(endDate),
                lsoa2001,
                lsoa2011,
                msoa2001,
                msoa2011,
                ward,
                localAuthority
        );
    }
}
