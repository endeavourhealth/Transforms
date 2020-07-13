package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

public class PatientPseudoId extends AbstractTargetTable {

    public PatientPseudoId(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }


    public void writeUpsert(SubscriberId subscriberId, long organizationId, long patientId, long personId, String saltKeyName, String pseudoId,
                            boolean isNhsNumberValid, boolean isNhsNumberVerifiedByPublisher) throws Exception {

        super.printRecord(
                convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + patientId,
                "" + personId,
                saltKeyName,
                pseudoId,
                convertBoolean(isNhsNumberValid),
                convertBoolean(isNhsNumberVerifiedByPublisher)
        );
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                Boolean.TYPE,
                Boolean.TYPE
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
                "salt_name",
                "skid",
                "is_nhs_number_valid",
                "is_nhs_number_verified_by_publisher"
        };

    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PATIENT_PSEUDO_ID;
    }
}
