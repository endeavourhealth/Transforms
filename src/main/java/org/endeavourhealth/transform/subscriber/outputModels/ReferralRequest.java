package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class ReferralRequest extends AbstractSubscriberCsvWriter {

    public ReferralRequest(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            // Long snomedConceptId,
                            Long requesterOrganizationId,
                            Long recipientOrganizationId,
                            Integer referralRequestPriorityConceptId,
                            Integer referralRequestTypeConceptId,
                            String mode,
                            Boolean outgoingReferral,
                            // String originalCode,
                            // String originalTerm,
                            boolean isReview,
                            Integer coreConceptId,
                            Integer nonCoreConceptId,
                            Double ageAtEvent,
                            Boolean isPrimary) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                "" + patientId,
                "" + personId,
                convertLong(encounterId),
                convertLong(practitionerId),
                convertDate(clinicalEffectiveDate),
                convertInt(datePrecisionId),
                // convertLong(snomedConceptId),
                convertLong(requesterOrganizationId),
                convertLong(recipientOrganizationId),
                convertInt(referralRequestPriorityConceptId),
                convertInt(referralRequestTypeConceptId),
                mode,
                convertBoolean(outgoingReferral),
                // originalCode,
                // originalTerm,
                convertBoolean(isReview),
                convertInt(coreConceptId),
                convertInt(nonCoreConceptId),
                convertDouble(ageAtEvent),
                convertBoolean(isPrimary));
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
                "requester_organization_id",
                "recipient_organization_id",
                "referral_request_priority_concept_id",
                "referral_request_type_concept_id",
                "mode",
                "outgoing_referral",
                "is_review",
                "core_concept_id",
                "non_core_concept_id",
                "age_at_event",
                "is_primary"
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
                Boolean.class,
                Boolean.TYPE,
                Integer.class,
                Integer.class,
                String.class,
                Boolean.TYPE,
        };
    }
}
