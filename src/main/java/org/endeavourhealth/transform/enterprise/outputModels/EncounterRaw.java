package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class EncounterRaw extends AbstractEnterpriseCsvWriter {

    public EncounterRaw(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            long organisationId,
                            long patientId,
                            long personId,
                            Long practitionerId,
                            Long episodeOfCareId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            Long recordingPractitionerId,
                            Date recordingDate,
                            Long appointmentId,
                            Long serviceProviderOrganisationId,
                            Long locationId,
                            Date endDate,
                            Integer duration,
                            String fhirAdtMessageCode,
                            String fhirClass,
                            String fhirType,
                            String fhirStatus,
                            Long fhirSnomedConceptId,
                            String fhirOriginalCode,
                            String fhirOriginalTerm) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organisationId,
                "" + patientId,
                "" + personId,
                convertLong(practitionerId),
                convertLong(episodeOfCareId),
                convertDate(clinicalEffectiveDate),
                convertInt(datePrecisionId),
                convertLong(recordingPractitionerId),
                convertDate(recordingDate),
                convertLong(appointmentId),
                convertLong(serviceProviderOrganisationId),
                convertLong(locationId),
                convertDate(endDate),
                convertInt(duration),
                fhirAdtMessageCode,
                fhirClass,
                fhirType,
                fhirStatus,
                convertLong(fhirSnomedConceptId),
                fhirOriginalCode,
                fhirOriginalTerm);
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
                "episode_of_care_id",
                "clinical_effective_date",
                "date_precision_id",
                "recording_practitioner_id",
                "recording_date",
                "appointment_id",
                "service_provider_organization_id",
                "location_id",
                "end_date",
                "duration_minutes",
                "fhir_adt_message_code",
                "fhir_class",
                "fhir_type",
                "fhir_status",
                "fhir_snomed_concept_id",
                "fhir_original_code",
                "fhir_original_term"
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
                Date.class,
                Long.class,
                Long.class,
                Long.class,
                Date.class,
                Integer.class,
                String.class,
                String.class,
                String.class,
                String.class,
                Long.class,
                String.class,
                String.class
        };
    }

}
