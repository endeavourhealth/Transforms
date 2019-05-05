package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationOrder extends AbstractTargetTable {

    public MedicationOrder(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            Long encounterId,
                            Long practitionerId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            // Long dmdId,
                            String dose,
                            BigDecimal quantityValue,
                            String quantityUnit,
                            Integer durationDays,
                            BigDecimal estimatedCost,
                            Long medicationStatementId,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            // String originalTerm,
                            Integer bnfReference,
                            Double ageAtEvent,
                            String issueMethod) throws Exception {

        super.printRecord(getEventTypeDesc(subscriberId),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertLong(encounterId),
                convertLong(practitionerId),
                convertDate(clinicalEffectiveDate),
                convertInt(datePrecisionId),
                // convertLong(dmdId),
                dose,
                convertBigDecimal(quantityValue),
                quantityUnit,
                convertInt(durationDays),
                convertBigDecimal(estimatedCost),
                convertLong(medicationStatementId),
                convertInt(coreConceptId),
                convertInt(nonCoreConceptId),
                // originalTerm,
                convertInt(bnfReference),
                convertDouble(ageAtEvent),
                issueMethod);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "encounter_id",
                "practitioner_id",
                "clinical_effective_date",
                "date_precision_id",
                "dose",
                "quantity_value",
                "quantity_unit",
                "duration_days",
                "estimated_cost",
                "medication_statement_id",
                "core_concept_id",
                "non_core_concept_id",
                "bnf_reference",
                "age_at_event",
                "issue_method"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.MEDICATION_ORDER;
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
                String.class,
                BigDecimal.class,
                String.class,
                Integer.class,
                BigDecimal.class,
                Long.class,
                Integer.class,
                Integer.class,
                Integer.class,
                String.class,
                String.class
        };
    }
}
