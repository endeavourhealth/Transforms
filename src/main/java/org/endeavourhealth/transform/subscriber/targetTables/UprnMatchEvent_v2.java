package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;

public class UprnMatchEvent_v2 extends AbstractTargetTable {
    public UprnMatchEvent_v2(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE, // thingy
                Long.TYPE, // id
                String.class, // uprn
                String.class, // uprn_ralf00
                Long.TYPE, // location_id
				Long.TYPE, // patient_address_id
                BigDecimal.class, // latitude
                BigDecimal.class, // longitude
                BigDecimal.class, // x
                BigDecimal.class, // y
                String.class, // qualifier
                String.class, // algorithm
                Date.class, // match_date
				Long.TYPE, // abp_address_id
                String.class, // match_pattern_postcode
                String.class, // match_pattern_street
                String.class, // match_pattern_number
                String.class, // match_pattern_building
                String.class, // match_pattern_flat
                String.class, // algorithm_version
                String.class, // epoch
                String.class // previous_address
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "id", // "uprn_match_event_id"
                "uprn",
                "uprn_ralf00",
                "location_id",
				"patient_address_id",
                "latitude",
                "longitude",
                "xcoordinate",
                "ycoordinate",
                "qualifier",
                "algorithm",
                "match_date",
				"abp_address_id",
                "match_pattern_postcode",
                "match_pattern_street",
                "match_pattern_number",
                "match_pattern_building",
                "match_pattern_flat",
                "algorithm_version",
                "epoch",
                "previous_address"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.UPRN_MATCH_EVENT_V2;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(//SubscriberId subscriberId,
                            Long uprn_match_event_id,
                            String uprn,
							String uprn_ralf00,
                            Long location_id,
							Long patient_address_id,
                            BigDecimal latitude,
                            BigDecimal longitude,
                            BigDecimal xcoordinate,
                            BigDecimal ycoordinate,
                            String qualifier,
                            String algorithm,
                            Date match_date,
							Long abp_address_id,
                            String match_pattern_postcode,
                            String match_pattern_street,
                            String match_pattern_number,
                            String match_pattern_building,
                            String match_pattern_flat,
                            String algorithm_version,
                            String epoch,
                            String previous_address
    ) throws Exception {

        super.printRecord(
                convertBoolean(false),
                //"" + subscriberId.getSubscriberId(),
                convertLong(uprn_match_event_id),
                uprn,
				uprn_ralf00,
                convertLong(location_id),
				convertLong(patient_address_id),
                convertBigDecimal(latitude),
                convertBigDecimal(longitude),
                convertBigDecimal(xcoordinate),
                convertBigDecimal(ycoordinate),
                qualifier,
                algorithm,
                convertDateTime(match_date),
				convertLong(abp_address_id),
                match_pattern_postcode,
                match_pattern_street,
                match_pattern_number,
                match_pattern_building,
                match_pattern_flat,
                algorithm_version,
                epoch,
                previous_address
        );
    }
}

                