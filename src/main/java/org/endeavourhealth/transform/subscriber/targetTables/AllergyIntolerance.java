package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

public class AllergyIntolerance extends AbstractTargetTable {

    private boolean includeDateRecorded = false;

    public AllergyIntolerance(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            // Long snomedConceptId,
                            // String originalCode,
                            // String originalTerm,
                            boolean isReview,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            Double ageAtEvent,
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
                    // convertLong(snomedConceptId),
                    // originalCode,
                    // originalTerm,
                    convertBoolean(isReview),
                    convertInt(coreConceptId),
                    convertInt(nonCoreConceptId),
                    convertDouble(ageAtEvent),
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
                    // convertLong(snomedConceptId),
                    // originalCode,
                    // originalTerm,
                    convertBoolean(isReview),
                    convertInt(coreConceptId),
                    convertInt(nonCoreConceptId),
                    convertDouble(ageAtEvent));
        }
    }



    @Override
    public String[] getCsvHeaders() {
        if (includeDateRecorded) {
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
                    "date_recorded"
            };
        } else {
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
                    "age_at_event"
            };
        }
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ALLERGY_INTOLERANCE;
    }

    @Override
    public Class[] getColumnTypes() {
        if (includeDateRecorded) {
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
                    BigDecimal.class,
                    Date.class
            };
        } else {
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
                    BigDecimal.class,
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