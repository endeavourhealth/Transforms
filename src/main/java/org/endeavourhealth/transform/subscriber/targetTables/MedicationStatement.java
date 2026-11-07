package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

public class MedicationStatement extends AbstractTargetTable {

    private boolean includeDateRecorded = false;

    public MedicationStatement(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void setIncludeDateRecorded(boolean includeDateRecorded) {
        this.includeDateRecorded = includeDateRecorded;
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
                            Date cancellationDate,
                            String dose,
                            BigDecimal quantityValue,
                            String quantityUnit,
                            Integer authorisationTypeConceptId,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            String bnfReference,
                            Double ageAtEvent,
                            String issueMethod,
                            Date dateRecorded,
                            SubscriberTransformHelper params) throws Exception {

        //get any configured excluded columns for this table
        List<String> excludedTableColumns
                = params.getSubscriberExcludedColumns(getTableId(), getPermittedColumnExclusions());
        if (excludedTableColumns != null) {
            //check each column set for exclusion and blank
            for (String column: excludedTableColumns) {
                switch (column) {
                    case "age_at_event" : ageAtEvent = null; break;
                    default : break;
                }
            }
        }

        if (includeDateRecorded) {
            super.printRecord(convertBoolean(false),
                    "" + subscriberId.getSubscriberId(),
                    "" + organizationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDateTime(clinicalEffectiveDate), //SD-498
                    convertInt(datePrecisionConceptId),
                    convertDate(cancellationDate),
                    dose,
                    convertBigDecimal(quantityValue),
                    quantityUnit,
                    convertInt(authorisationTypeConceptId),
                    convertInt(coreConceptId),
                    convertInt(nonCoreConceptId),
                    bnfReference,
                    convertDouble(ageAtEvent),
                    issueMethod,
                    convertDateTime(dateRecorded));
        } else {
            super.printRecord(convertBoolean(false),
                    "" + subscriberId.getSubscriberId(),
                    "" + organizationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDateTime(clinicalEffectiveDate), //SD-498
                    convertInt(datePrecisionConceptId),
                    convertDate(cancellationDate),
                    dose,
                    convertBigDecimal(quantityValue),
                    quantityUnit,
                    convertInt(authorisationTypeConceptId),
                    convertInt(coreConceptId),
                    convertInt(nonCoreConceptId),
                    bnfReference,
                    convertDouble(ageAtEvent),
                    issueMethod);
        }
    }

    @Override
    public String[] getCsvHeaders() {
        if (includeDateRecorded) {
            return new String[]{
                    "is_delete",
                    "id",
                    "organization_id",
                    "patient_id",
                    "person_id",
                    "encounter_id",
                    "practitioner_id",
                    "clinical_effective_date",
                    "date_precision_concept_id",
                    "cancellation_date",
                    "dose",
                    "quantity_value",
                    "quantity_unit",
                    "authorisation_type_concept_id",
                    "core_concept_id",
                    "non_core_concept_id",
                    "bnf_reference",
                    "age_at_event",
                    "issue_method",
                    "date_recorded"
            };
        } else {

            return new String[]{
                    "is_delete",
                    "id",
                    "organization_id",
                    "patient_id",
                    "person_id",
                    "encounter_id",
                    "practitioner_id",
                    "clinical_effective_date",
                    "date_precision_concept_id",
                    "cancellation_date",
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
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.MEDICATION_STATEMENT;
    }

    @Override
    public Class[] getColumnTypes() {
        if (includeDateRecorded) {
            return new Class[]{
                    Byte.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Date.class,
                    String.class,
                    BigDecimal.class,
                    String.class,
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    String.class,
                    BigDecimal.class,
                    String.class,
                    Date.class
            };
        } else {
            return new Class[]{
                    Byte.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
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

    @Override
    public String[] getPermittedColumnExclusions() {
        return new String[] {
                "age_at_event"
        };
    }
}
