package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

public class PatientAddressRalf extends AbstractTargetTable {

    public PatientAddressRalf(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE,
                Long.TYPE, // id
                Long.TYPE, // organization_id
                Long.TYPE, // patient_id
                Long.TYPE, // person_id
                Long.TYPE, // patient_address_id
                String.class, // patient_address_match_uprn_ralf00
                String.class, // salt_name
                String.class, // ralf
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
                "patient_address_id",
                "patient_address_match_uprn_ralf00",
                "salt_name",
                "ralf"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PATIENT_ADDRESS_RALF;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organizationId,
                            long patientId,
                            long personId,
                            long patientAddressId,
                            String patientAddressMatchUprnRalf00,
                            String saltKeyName,
                            String ralf) throws Exception {

        super.printRecord(
                convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + patientId,
                "" + personId,
                "" + patientAddressId,
                "" + patientAddressMatchUprnRalf00,
                saltKeyName,
                ralf
        );
    }
}
