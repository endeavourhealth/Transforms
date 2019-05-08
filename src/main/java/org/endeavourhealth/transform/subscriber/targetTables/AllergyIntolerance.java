package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class AllergyIntolerance extends AbstractTargetTable {

    public AllergyIntolerance(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            Long encounterId,
                            Long practitionerId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            // Long snomedConceptId,
                            // String originalCode,
                            // String originalTerm,
                            boolean isReview,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            Double ageAtEvent,
                            Boolean isPrimary) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertLong(encounterId),
                convertLong(practitionerId),
                convertDate(clinicalEffectiveDate),
                convertInt(datePrecisionId),
                // convertLong(snomedConceptId),
                // originalCode,
                // originalTerm,
                convertBoolean(isReview),
                convertInt(coreConceptId),
                convertInt(nonCoreConceptId),
                convertDouble(ageAtEvent),
                convertBoolean(isPrimary));
    }



    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "encounter_id",
                "practitioner_id",
                "clinical_effective_date",
                "date_precision_concept_id",
                "is_review",
                "core_concept_id",
                "non_core_concept_id",
                "age_at_event",
                "is_primary"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ALLERGY_INTOLERANCE;
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
                Boolean.TYPE,
                Integer.class,
                Integer.class,
                String.class,
                Boolean.TYPE,
        };
    }
}
