package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class EpisodeOfCare extends AbstractTargetTable {

    public EpisodeOfCare(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord("" + EventLog.EVENT_LOG_DELETE,
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organizationId,
                            long patientId,
                            long personId,
                            Integer registrationTypeConceptId,
                            Integer registrationStatusConceptId,
                            Date dateRegistered,
                            Date dateRegisteredEnd,
                            Long usualGpPractitionerId) throws Exception {

        super.printRecord(getEventTypeDesc(subscriberId),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertInt(registrationTypeConceptId),
                convertInt(registrationStatusConceptId),
                convertDate(dateRegistered),
                convertDate(dateRegisteredEnd),
                convertLong(usualGpPractitionerId));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "registration_type_concept_id",
                "registration_status_concept_id",
                "date_registered",
                "date_registered_end",
                "usual_gp_practitioner_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.EPISODE_OF_CARE;
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Integer.class,
                Integer.class,
                Date.class,
                Date.class,
                Long.class
        };
    }
}
