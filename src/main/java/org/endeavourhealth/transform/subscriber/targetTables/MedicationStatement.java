package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationStatement extends AbstractTargetTable {

    public MedicationStatement(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            Integer datePrecisionConceptId,
                            // Long dmdId,
//                            boolean isActive,
                            Date cancellationDate,
                            String dose,
                            BigDecimal quantityValue,
                            String quantityUnit,
                            Integer authorisationTypeConceptId,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            // String originalTerm,
                            String bnfReference,
                            Double ageAtEvent,
                            String issueMethod) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertLong(encounterId),
                convertLong(practitionerId),
                convertDate(clinicalEffectiveDate),
                convertInt(datePrecisionConceptId),
                // convertLong(dmdId),
//                convertBoolean(isActive),
                convertDate(cancellationDate),
                dose,
                convertBigDecimal(quantityValue),
                quantityUnit,
                convertInt(authorisationTypeConceptId),
                convertInt(coreConceptId),
                convertInt(nonCoreConceptId),
                // originalTerm,
                bnfReference,
                convertDouble(ageAtEvent),
                issueMethod);
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
//                "is_active",
//                "cancellation_date",
                "dose",
                "quantity_value",
                "quantity_unit",
                "authorisation_type_concept_id",
                "core_concept_id",
                "non_core_concept_id",
                "bnf_reference",
                "age_at_event",
                "issue_method"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.MEDICATION_STATEMENT;
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
//                Boolean.class,
                Date.class,
                String.class,
                BigDecimal.class,
                String.class,
                Integer.class,
                Integer.class,
                Integer.class,
                String.class,
                BigDecimal.class,
                String.class
        };
    }
}
