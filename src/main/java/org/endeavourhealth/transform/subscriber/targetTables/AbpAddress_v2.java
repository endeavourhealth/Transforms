package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;


public class AbpAddress_v2 extends AbstractTargetTable {
    public AbpAddress_v2(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE, // thingy
				Long.TYPE, // id
				Long.class, // property_id
                String.class, // flat
				String.class, // building
				String.class, // number
				String.class, // dependent_thoroughfare
				String.class, // street
				String.class, // dependent_locality
				String.class, // locality
				String.class, // town
				String.class, // postcode
				String.class, // abp_organisation
				Long.class // classification_id
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "id",
                "property_id",
                "flat",
                "building",
				"number",
				"dependent_thoroughfare",
				"street",
				"dependent_locality",
				"locality",
				"town",
				"postcode",
				"abp_organisation",
				"classification_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ABP_ADDRESS_V2;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(//SubscriberId subscriberId,
                            Long abp_address_id,
							Long property_id,
			    			String flat,
			    			String building,
			    			String number,
			    			String dependent_thoroughfare,
							String street,
							String dependent_locality,
							String locality,
							String town,
							String postcode,
							String abp_organisation,
                            Long classification_id
    ) throws Exception {

        super.printRecord(
                convertBoolean(false),
                //"" + subscriberId.getSubscriberId(),
                "" + convertLong(abp_address_id),
				convertLong(property_id),
				flat,
				building,
				number,
				dependent_thoroughfare,
				street,
				dependent_locality,
				locality,
				town,
				postcode,
				abp_organisation,
				convertLong(classification_id)
        );
    }
}