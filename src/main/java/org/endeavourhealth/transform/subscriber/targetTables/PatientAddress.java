package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

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
                String.class,
                BigDecimal.class
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
                "local_authority_code",
                "townsend_deprivation_index"
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
                            String localAuthority,
                            Double townsendDeprivationIndex,
                            SubscriberTransformHelper params) throws Exception {

        //get any configured excluded columns for this table
        List<String> excludedTableColumns
                = params.getSubscriberExcludedColumns(getTableId(), getPermittedColumnExclusions());
        if (excludedTableColumns != null) {
            //check each column set for exclusion and blank
            for (String column: excludedTableColumns) {
                switch (column) {
                    case "postcode" : postcode = null; break;
                    case "lsoa_2001_code" : lsoa2001 = null; break;
                    case "lsoa_2011_code" : lsoa2011 = null; break;
                    case "msoa_2001_code" : msoa2001 = null; break;
                    case "msoa_2011_code" : msoa2011 = null; break;
                    case "ward_code" : ward = null; break;
                    default : break;
                }
            }
        }

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
                localAuthority,
                convertDouble(townsendDeprivationIndex)
        );
    }

    @Override
    public String[] getPermittedColumnExclusions() {
        return new String[] {
                "postcode",
                "lsoa_2001_code",
                "lsoa_2011_code",
                "msoa_2001_code",
                "msoa_2011_code",
                "ward_code"
        };
    }
}
