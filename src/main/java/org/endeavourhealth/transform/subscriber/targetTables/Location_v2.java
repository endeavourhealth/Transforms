package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;


public class Location_v2 extends AbstractTargetTable {
    public Location_v2(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE, // thingy
				Long.TYPE, // id
                String.class, // name
                String.class, // type_code
                String.class, // type_desc
                String.class, // postcode
				Long.class, // managing_organization_id
				String.class, // uprn
				String.class, // uprn_ralf00
				BigDecimal.class, // latitude
				BigDecimal.class, // longitude
				BigDecimal.class, // xcoordinate
				BigDecimal.class, // ycoordinate
				String.class, // lsoa_code
				String.class, // msoa_code
				String.class // imp_code
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "id",
                "name",
                "type_code",
                "type_desc",
				"postcode",
				"managing_organization_id",
				"uprn",
				"uprn_ralf00",
				"latitude",
				"longitude",
				"xcoordinate",
				"ycoordinate",
				"lsoa_code",
				"msoa_code",
				"imp_code"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.LOCATION_V2;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(//SubscriberId subscriberId,
                            Long location_id,
			    			String name,
			    			String type_code,
			    			String type_desc,
			    			String postcode,
                            Long managing_organization_id,
                            String uprn,
                            String uprn_ralf00,
			    			BigDecimal latitude,
			    			BigDecimal longitude,
			    			BigDecimal xcoordinate,
			    			BigDecimal ycoordinate,
			    			String lsoa_code,
			    			String msoa_code,
			    			String imp_code
    ) throws Exception {

        super.printRecord(
                convertBoolean(false),
                //"" + subscriberId.getSubscriberId(),
                "" + convertLong(location_id),
				name,
				type_code,
				type_desc,
				postcode,
				convertLong(managing_organization_id),
				uprn,
				uprn_ralf00,
				convertBigDecimal(latitude),
				convertBigDecimal(longitude),
				convertBigDecimal(xcoordinate),
				convertBigDecimal(ycoordinate),
				lsoa_code,
				msoa_code,
				imp_code
        );
    }
}

