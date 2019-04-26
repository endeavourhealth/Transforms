package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationStatement extends AbstractSubscriberCsvWriter {

    public MedicationStatement(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            Long encounterId,
                            Long practitionerId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            // Long dmdId,
                            Boolean isActive,
                            Date cancellationDate,
                            String dose,
                            BigDecimal quantityValue,
                            String quantityUnit,
                            Integer medicationStatementAuthorisationTypeConceptId,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            // String originalTerm,
                            Integer bnfReference,
                            Double ageAtEvent,
                            String issueMethod) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertLong(encounterId),
                convertLong(practitionerId),
                convertDate(clinicalEffectiveDate),
                convertInt(datePrecisionId),
                // convertLong(dmdId),
                convertBoolean(isActive),
                convertDate(cancellationDate),
                dose,
                convertBigDecimal(quantityValue),
                quantityUnit,
                convertInt(medicationStatementAuthorisationTypeConceptId),
                convertInt(coreConceptId),
                convertInt(nonCoreConceptId),
                // originalTerm,
                convertInt(bnfReference),
                convertDouble(ageAtEvent),
                issueMethod);
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
                "clinical_effective_date",
                "date_precision_id",
                "is_active",
                "cancellation_date",
                "dose",
                "quantity_value",
                "quantity_unit",
                "medication_statement_authorisation_type_id",
                "core_concept_id",
                "non_core_concept_id",
                "bnf_reference",
                "age_at_event",
                "issue_method"
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
                Boolean.class,
                Date.class,
                String.class,
                BigDecimal.class,
                String.class,
                Integer.class,
                Integer.class,
                Integer.class,
                Integer.class,
                String.class,
                String.class
        };
    }
}
