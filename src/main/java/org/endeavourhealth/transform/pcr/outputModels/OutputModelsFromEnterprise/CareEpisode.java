package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;

public class CareEpisode extends AbstractPcrCsvWriter {

    public CareEpisode(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            long patientId,
                            long owningOrganisationId,
                            Date effectiveDate,
                            Integer effectiveDatePrecision,
                            Long effectivePractitionerId,
                            Date endDate,
                            Long encounterLinkId,
                            Long statusConceptId,
                            Long specialityConceptId,
                            Long adminConceptId,
                            Long reasonConceptItd,
                            Long typeConceptId,
                            Long locationId,
                            Long referralRequestId,
                            Boolean isConsent,
                            Long latestCareEpisodeStatusId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientId,
                "" + owningOrganisationId,
                convertDate(effectiveDate),
                "" + effectiveDatePrecision,
                convertLong(effectivePractitionerId),
                convertDate(endDate),
                convertLong(encounterLinkId),
                convertLong(statusConceptId),
                convertLong(specialityConceptId),
                convertLong(adminConceptId),
                convertLong(reasonConceptItd),
                convertLong(typeConceptId),
                convertLong(locationId),
                convertLong(referralRequestId),
                convertBoolean(isConsent),
                convertLong(latestCareEpisodeStatusId));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "owing_organisation_id",
                "effective_date",
                "effective_date_precision",
                "effective_practitioner_id",
                "clinical_effective_date",
                "date_precision_id",
                "snomed_concept_id",
                "original_code",
                "original_term",
                "episode_of_care_id",
                "service_provider_organization_id"
        };
    }

    @Override
    public Class[] getColumnTypes() {
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
                String.class,
                String.class,
                Long.class,
                Long.class
        };
    }
}
