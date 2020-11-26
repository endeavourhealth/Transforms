package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;


public class Address_v2 extends AbstractTargetTable {
    public Address_v2(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE, // thingy
				Long.TYPE, // id
				String.class, // line_1
                String.class, // line_2
				String.class, // line_3
				String.class, // city
				String.class, // county
				String.class, // postcode
				Long.class // location_id
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "id",
                "line_1",
                "line_2",
                "line_3",
				"city",
				"county",
				"postcode",
				"location_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ADDRESS_V2;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(//SubscriberId subscriberId,
                            Long address_id,
							String line_1,
			    			String line_2,
			    			String line_3,
			    			String city,
			    			String county,
							String postcode,
							Long location_id
    ) throws Exception {

        super.printRecord(
                convertBoolean(false),
                //"" + subscriberId.getSubscriberId(),
                "" + convertLong(address_id),
				line_1,
				line_2,
				line_3,
				city,
				county,
				postcode,
				convertLong(location_id)
        );
    }
}