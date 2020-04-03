package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.math.BigDecimal;
import java.util.Date;

public class ProcedureRequest extends AbstractTargetTable {

    private boolean includeDateRecorded = false;

    public ProcedureRequest(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            Integer procedureRequestStatusConceptId,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            Double ageAtEvent,
                            Date dateRecorded) throws Exception {

        if (includeDateRecorded) {
            super.printRecord(convertBoolean(false),
                    "" + subscriberId.getSubscriberId(),
                    "" + organizationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionConceptId),
                    convertInt(procedureRequestStatusConceptId),
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
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionConceptId),
                    convertInt(procedureRequestStatusConceptId),
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
                    "status_concept_id",
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
                    "status_concept_id",
                    "core_concept_id",
                    "non_core_concept_id",
                    "age_at_event",
            };
        }
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PROCEDURE_REQUEST;
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
                    Integer.class,
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
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    BigDecimal.class,
            };
        }
    }
}
