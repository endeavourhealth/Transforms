package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Encounter extends AbstractEnterpriseCsvWriter {

    private boolean includeDateRecorded = false;

    public Encounter(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void setIncludeDateRecorded(boolean includeDateRecorded) {
        this.includeDateRecorded = includeDateRecorded;
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
                            Long appointmentId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            Long snomedConceptId,
                            String originalCode,
                            String originalTerm,
                            Long episodeOfCareId,
                            Long serviceProviderOrganisationId,
                            Date dateRecorded) throws Exception {

        if(includeDateRecorded) {
            super.printRecord(OutputContainer.UPSERT,
                    "" + id,
                    "" + organisationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(practitionerId),
                    convertLong(appointmentId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionId),
                    convertLong(snomedConceptId),
                    originalCode,
                    originalTerm,
                    convertLong(episodeOfCareId),
                    convertLong(serviceProviderOrganisationId),
                    convertDateTime(dateRecorded));
        } else {
            super.printRecord(OutputContainer.UPSERT,
                    "" + id,
                    "" + organisationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(practitionerId),
                    convertLong(appointmentId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionId),
                    convertLong(snomedConceptId),
                    originalCode,
                    originalTerm,
                    convertLong(episodeOfCareId),
                    convertLong(serviceProviderOrganisationId));
        }
    }

    @Override
    public String[] getCsvHeaders() {
        if (includeDateRecorded) {
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
                    "snomed_concept_id",
                    "original_code",
                    "original_term",
                    "episode_of_care_id",
                    "service_provider_organization_id",
                    "date_recorded"
            };
        } else {
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
                    "snomed_concept_id",
                    "original_code",
                    "original_term",
                    "episode_of_care_id",
                    "service_provider_organization_id"
            };
        }
    }

    @Override
    public Class[] getColumnTypes() {
        if (includeDateRecorded) {
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
                    String.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Date.class
            };
        } else {
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
                    String.class,
                    String.class,
                    Long.class,
                    Long.class
            };
        }
    }
}
