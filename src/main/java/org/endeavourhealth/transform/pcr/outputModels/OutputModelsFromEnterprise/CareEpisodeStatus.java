package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;

public class CareEpisodeStatus extends AbstractPcrCsvWriter {

    public CareEpisodeStatus(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long patientId,
                            long owningOrganisationId,
                            long careEpisodeId,
                            Date startTime,
                            Date endTime,
                            Long careEpisodeStatusConceptId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + patientId,
                "" + owningOrganisationId,
                convertLong(careEpisodeId),
                convertDate(startTime),
                convertDate(endTime),
                convertLong(careEpisodeStatusConceptId));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "patient_id",
                "owing_organisation_id",
                "care_episode_id",
                "start_time",
                "end_time",
                "care_episode_status_concept_id"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                String.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Date.class,
                Date.class,
                Long.class
        };
    }
}
