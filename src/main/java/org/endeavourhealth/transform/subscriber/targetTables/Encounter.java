package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

public class Encounter extends AbstractTargetTable {

    private boolean includeDateRecorded = false;

    public Encounter(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            String institutionLocationId,
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
                    convertLong(practitionerId),
                    convertLong(appointmentId),
                    convertDateTime(clinicalEffectiveDate), //SD-498
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
                    convertDateTime(endDate), //SD-498
                    institutionLocationId,
                    convertDateTime(dateRecorded));
        } else {
            super.printRecord(convertBoolean(false),
                    "" + subscriberId.getSubscriberId(),
                    "" + organizationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(practitionerId),
                    convertLong(appointmentId),
                    convertDateTime(clinicalEffectiveDate), //SD-498
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
                    convertDateTime(endDate), //SD-498
                    institutionLocationId);
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
                    "institution_location_id",
                    "date_recorded"
            };
        } else {
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
                    "institution_location_id",
            };
        }
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ENCOUNTER;
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
                    Long.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    BigDecimal.class,
                    String.class,
                    String.class,
                    String.class,
                    Date.class,
                    String.class,
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
                    Long.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    BigDecimal.class,
                    String.class,
                    String.class,
                    String.class,
                    Date.class,
                    String.class,
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
