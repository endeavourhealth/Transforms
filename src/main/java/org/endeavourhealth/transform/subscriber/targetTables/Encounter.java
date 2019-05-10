package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.math.BigDecimal;
import java.util.Date;

public class Encounter extends AbstractTargetTable {

    public Encounter(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organizationId,
                            long patientId,
                            long personId,
                            Long practitionerId,
                            Long appointmentId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionConceptId,
                            Long episodeOfCareId,
                            Long serviceProviderOrganisationId,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            Double ageAtEvent,
                            String type,
                            String subtype,
                            String admissionMethod,
                            Date endDate,
                            String institutionLocationId) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertLong(practitionerId),
                convertLong(appointmentId),
                convertDate(clinicalEffectiveDate),
                convertInt(datePrecisionConceptId),
                // convertLong(snomedConceptId),
                // originalCode,
                // originalTerm,
                convertLong(episodeOfCareId),
                convertLong(serviceProviderOrganisationId),
                convertInt(coreConceptId),
                convertInt(nonCoreConceptId),
                convertDouble(ageAtEvent),
                type,
                subtype,
                admissionMethod,
                convertDate(endDate),
                institutionLocationId);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "practitioner_id",
                "appointment_id",
                "clinical_effective_date",
                "date_precision_concept_id",
                "episode_of_care_id",
                "service_provider_organization_id",
                "core_concept_id",
                "non_core_concept_id",
                "age_at_event",
                "type",
                "sub_type",
                "admission_method",
                "end_date",
                "institution_location_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ENCOUNTER;
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.class,
                Long.class,
                Date.class,
                Integer.class,
                Long.class,
                Long.class,
                Integer.class,
                Integer.class,
                BigDecimal.class,
                String.class,
                String.class,
                String.class,
                Date.class,
                String.class
        };
    }
}
