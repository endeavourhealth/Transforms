package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class EncounterEvent extends AbstractEnterpriseCsvWriter {

    public EncounterEvent(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            long encounterId,
                            Long practitionerId,
                            Long appointmentId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            Long snomedConceptId,
                            String originalCode,
                            String originalTerm,
                            Long episodeOfCareId,
                            Long serviceProviderOrganisationId,
                            Date dateRecorded,
                            Long locationId,
                            boolean finished) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organisationId,
                "" + patientId,
                "" + personId,
                "" + encounterId,
                convertLong(practitionerId),
                convertLong(appointmentId),
                convertDateTime(clinicalEffectiveDate), //it's a datetime on this table
                convertInt(datePrecisionId),
                convertLong(snomedConceptId),
                originalCode,
                originalTerm,
                convertLong(episodeOfCareId),
                convertLong(serviceProviderOrganisationId),
                convertDateTime(dateRecorded),
                convertLong(locationId),
                convertBoolean(finished));
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
                "appointment_id",
                "clinical_effective_date",
                "date_precision_id",
                "snomed_concept_id",
                "original_code",
                "original_term",
                "episode_of_care_id",
                "service_provider_organization_id",
                "date_recorded",
                "location_id",
                "finished"
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
                Long.TYPE, //encounter_id
                Long.class,
                Long.class,
                Date.class,
                Integer.class,
                Long.class,
                String.class,
                String.class,
                Long.class,
                Long.class,
                Date.class,
                Long.class, //location_id
                Boolean.TYPE //finished
        };
    }
}
