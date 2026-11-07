package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

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
                Long.TYPE, // patient_address_id (bigint)
                String.class, // uprn (varchar)
                String.class, // uprn_ralf00 (varchar)
                Integer.class, // status (tinyint)
                String.class, // uprn_property_classification (varchar)
                BigDecimal.class, // latitude (double)
                BigDecimal.class, // longitude (double)
                BigDecimal.class, // x (double)
                BigDecimal.class, // y (double)
                String.class, // qualifier (varchar)
                String.class, // match_rule (varchar)
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
                String.class // epoch (varchar)
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "patient_address_id",
                "uprn",
                "uprn_ralf00",
                "status",
                "uprn_property_classification",
                "latitude",
                "longitude",
                "uprn_xcoordinate",
                "uprn_ycoordinate",
                "qualifier",
                "match_rule",
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
                "epoch"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PATIENT_ADDRESS_MATCH;
    }

    public void writeDelete(SubscriberId patient_address_id) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + patient_address_id.getSubscriberId()
        );
    }

    public void writeUpsert(
                            SubscriberId patient_address_id,
                            String uprn,
                            String uprnRalf00,
                            Integer status,
                            String classification,
                            BigDecimal latitude,
                            BigDecimal longitude,
                            BigDecimal uprnxcoordinate,
                            BigDecimal uprnycoordinate,
                            String qualifier,
                            String match_rule,
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
                            String epoch
    ) throws Exception {

        super.printRecord(
                convertBoolean(false),
                "" + patient_address_id.getSubscriberId(),
                uprn,
                uprnRalf00,
                convertInt(status),
                classification,
                convertBigDecimal(latitude),
                convertBigDecimal(longitude),
                convertBigDecimal(uprnxcoordinate),
                convertBigDecimal(uprnycoordinate),
                qualifier,
                match_rule,
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
                epoch
        );
    }

    @Override
    public String[] getPermittedColumnExclusions() {
        return null;
//        return new String[] {
//          //implement which columns can be excluded from the database write for extra pseudo level blanking
//        };
    }
}

