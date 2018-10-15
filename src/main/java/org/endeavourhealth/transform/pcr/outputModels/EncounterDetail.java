package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class EncounterDetail extends AbstractPcrCsvWriter {

    public EncounterDetail(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            Long completionStatusConceptId,
                            Long healthcareServiceTypeConceptId,
                            Long interactionModeConceptId,
                            Long administrativeActionConceptId,
                            Long purposeConceptId,
                            Long dispositionConceptId,
                            Long siteOfCareTypeConceptId,
                            Long patientStatusConceptId) throws Exception {

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
                convertLong(completionStatusConceptId),
                convertLong(healthcareServiceTypeConceptId),
                convertLong(interactionModeConceptId),
                convertLong(administrativeActionConceptId),
                convertLong(purposeConceptId),
                convertLong(dispositionConceptId),
                convertLong(siteOfCareTypeConceptId),
                convertLong(patientStatusConceptId));
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
                "completion_status_concept_id",
                "healthcare_service_type_concept_id",
                "interaction_mode_concept_id",
                "administrative_action_concept_id",
                "purpose_concept_id",
                "disposition_concept_id",
                "site_of_care_type_concept_id",
                "patient_status_concept_id",

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
                Long.class,
                Long.class,
                Long.class,
                Long.class,
                Long.class,
                Long.class,
                Long.class,
                Long.class
        };
    }

}
