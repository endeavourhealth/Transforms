package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class ProcedureRequest extends AbstractEnterpriseCsvWriter {

    private boolean includeDateRecorded = false;

    public ProcedureRequest(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            Long encounterId,
                            Long practitionerId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            Long snomedConceptId,
                            Integer procedureRequestStatusId,
                            String originalCode,
                            String originalTerm,
                            Date dateRecorded) throws Exception {

        if (includeDateRecorded) {
            super.printRecord(OutputContainer.UPSERT,
                    "" + id,
                    "" + organisationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionId),
                    convertLong(snomedConceptId),
                    convertInt(procedureRequestStatusId),
                    originalCode,
                    originalTerm,
                    convertDateTime(dateRecorded));
        } else {
            super.printRecord(OutputContainer.UPSERT,
                    "" + id,
                    "" + organisationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionId),
                    convertLong(snomedConceptId),
                    convertInt(procedureRequestStatusId),
                    originalCode,
                    originalTerm);
        }
    }

    @Override
    public String[] getCsvHeaders() {
        if (includeDateRecorded) {
            return new String[]{
                    "save_mode",
                    "id",
                    "organization_id",
                    "patient_id",
                    "person_id",
                    "encounter_id",
                    "practitioner_id",
                    "clinical_effective_date",
                    "date_precision_id",
                    "snomed_concept_id",
                    "procedure_request_status_id",
                    "original_code",
                    "original_term",
                    "date_recorded"
            };
        } else {
            return new String[]{
                    "save_mode",
                    "id",
                    "organization_id",
                    "patient_id",
                    "person_id",
                    "encounter_id",
                    "practitioner_id",
                    "clinical_effective_date",
                    "date_precision_id",
                    "snomed_concept_id",
                    "procedure_request_status_id",
                    "original_code",
                    "original_term"
            };
        }
    }

    @Override
    public Class[] getColumnTypes() {
        if (includeDateRecorded) {
            return new Class[]{
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
                    Integer.class,
                    String.class,
                    String.class,
                    Date.class
            };
        } else {
            return new Class[]{
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
                    Integer.class,
                    String.class,
                    String.class
            };
        }
    }
}
