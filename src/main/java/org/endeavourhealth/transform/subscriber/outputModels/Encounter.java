package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Encounter extends AbstractSubscriberCsvWriter {

    public Encounter(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            long organizationId,
                            long patientId,
                            long personId,
                            Long practitionerId,
                            Long appointmentId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            // Long snomedConceptId,
                            // String originalCode,
                            // String originalTerm,
                            Long episodeOfCareId,
                            Long serviceProviderOrganisationId,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            Double ageAtEvent,
                            String type,
                            String subtype,
                            Boolean isPrimary,
                            String admissionMethod,
                            Date endDate,
                            String institutionLocationId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertLong(practitionerId),
                convertLong(appointmentId),
                convertDate(clinicalEffectiveDate),
                convertInt(datePrecisionId),
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
                convertBoolean(isPrimary),
                admissionMethod,
                convertDate(endDate),
                institutionLocationId);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "practitioner_id",
                "appointment_id",
                "clinical_effective_date",
                "date_precision_id",
                "episode_of_care_id",
                "service_provider_organization_id",
                "core_concept_id",
                "non_core_concept_id",
                "age_at_event",
                "type",
                "sub_type",
                "is_primary",
                "admission_method",
                "end_date",
                "institution_location_id"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
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
                String.class,
                String.class,
                String.class,
                Boolean.TYPE,
                String.class,
                Date.class,
                String.class
        };
    }
}
