package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;

public class PatientAddressMatch extends AbstractTargetTable {
    public PatientAddressMatch(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE, // thingy
                //Long.TYPE, // id (bigint)
                Long.TYPE, // patient_address_id (bigint)
                String.class, // uprn (varchar)
                Integer.class, // status (tinyint)
                String.class, // classification (varchar)
                BigDecimal.class, // latitude (double)
                BigDecimal.class, // longitude (double)
                BigDecimal.class, // x (double)
                BigDecimal.class, // y (double)
                String.class, // qualifier (varchar)
                String.class, // algorithm (varchar)
                Date.class, // match_date (datetime)
                String.class, // abp_address_number (varchar)
                String.class, // abp_address_street (varchar)
                String.class, // abp_address_locality (varchar)
                String.class, // abp_address_town (varchar)
                String.class, // abp_address_postcode (varchar)
                String.class, // abp_address_organization (varchar)
                String.class, // match_pattern_postcode (varchar)
                String.class, // match_pattern_street (varchar)
                String.class, // match_pattern_number (varchar)
                String.class, // match_pattern_building (varchar)
                String.class, // match_pattern_flat (varchar)
                String.class, // algorithm_version (varchar)
                String.class // epoc (varchar)
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "id", // "patient_address_id"
                "uprn",
                "status",
                "classification",
                "latitude",
                "longitude",
                "xcoordinate",
                "ycoordinate",
                "qualifier",
                "algorithm",
                "match_date",
                "abp_address_number",
                "abp_address_street",
                "abp_address_locality",
                "abp_address_town",
                "abp_address_postcode",
                "abp_address_organization",
                "match_pattern_postcode",
                "match_pattern_street",
                "match_pattern_number",
                "match_pattern_building",
                "match_pattern_flat",
                "algorithm_version",
                "epoc"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PATIENT_ADDRESS_MATCH;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(//SubscriberId subscriberId,
                            long patient_address_id,
                            String uprn,
                            Integer status,
                            String classification,
                            BigDecimal latitude,
                            BigDecimal longitude,
                            BigDecimal xcoordinate,
                            BigDecimal ycoordinate,
                            String qualifier,
                            String algorithm,
                            Date match_date,
                            String abp_address_number,
                            String abp_address_street,
                            String abp_address_locality,
                            String abp_address_town,
                            String abp_address_postcode,
                            String abp_address_organization,
                            String match_pattern_postcode,
                            String match_pattern_street,
                            String match_pattern_number,
                            String match_pattern_building,
                            String match_pattern_flat,
                            String algorithm_version,
                            String epoc
    ) throws Exception {

        super.printRecord(
                convertBoolean(false),
                //"" + subscriberId.getSubscriberId(),
                "" + patient_address_id,
                uprn,
                convertInt(status),
                classification,
                convertBigDecimal(latitude),
                convertBigDecimal(longitude),
                convertBigDecimal(xcoordinate),
                convertBigDecimal(ycoordinate),
                qualifier,
                algorithm,
                convertDateTime(match_date),
                abp_address_number,
                abp_address_street,
                abp_address_locality,
                abp_address_town,
                abp_address_postcode,
                abp_address_organization,
                match_pattern_postcode,
                match_pattern_street,
                match_pattern_number,
                match_pattern_building,
                match_pattern_flat,
                algorithm_version,
                epoc
        );
    }
}

                