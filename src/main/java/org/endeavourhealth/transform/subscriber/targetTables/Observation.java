package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.math.BigDecimal;
import java.util.Date;

public class Observation extends AbstractTargetTable {

    public Observation(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            BigDecimal resultValue,
                            String resultValueUnits,
                            Date resultDate,
                            String resultText,
                            Long resultConceptId,
                            // String originalCode,
                            boolean isProblem,
                            // String originalTerm,
                            boolean isReview,
                            Date problemEndDate,
                            Long parentObservationId,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            Double ageAtEvent,
                            Long episodicityConceptId,
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
                convertBigDecimal(resultValue),
                resultValueUnits,
                convertDate(resultDate),
                resultText,
                convertLong(resultConceptId),
                // originalCode,
                convertBoolean(isProblem),
                // originalTerm,
                convertBoolean(isReview),
                convertDate(problemEndDate),
                convertLong(parentObservationId),
                convertInt(coreConceptId),
                convertInt(nonCoreConceptId),
                convertDouble(ageAtEvent),
                convertLong(episodicityConceptId),
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
                "date_precision_id",
                "result_value",
                "result_value_units",
                "result_date",
                "result_text",
                "result_concept_id",
                "is_problem",
                "is_review",
                "problem_end_date",
                "parent_observation_id",
                "core_concept_id",
                "non_core_concept_id",
                "age_at_event",
                "episodicity_concept_id",
                "is_primary"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.OBSERVATION;
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
                BigDecimal.class,
                String.class,
                Date.class,
                String.class,
                Long.class,
                Boolean.TYPE,
                Boolean.TYPE,
                Date.class,
                Long.class,
                Integer.class,
                Integer.class,
                Integer.class,
                String.class,
                Long.class,
                Boolean.TYPE,
        };
    }
}
