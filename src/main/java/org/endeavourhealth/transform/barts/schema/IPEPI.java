package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class IPEPI extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(IPEPI.class);

    public IPEPI(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#CDS_BATCH_CONTENT_ID",
                "ACTIVE_IND",
                "EXTRACT_DT_TM",
                "ENCNTR_SLICE_ID",
                "ENCNTR_ID",
                "PERSON_ID",
                "HOSP_PRVDR_IDENT",
                "EPISODE_SEQ_NBR",
                "EPISODE_START_DT_TM",
                "EPISODE_END_DT_TM",
                "FIRST_REG_DAY_NIGHT_NHS_CD_ALIAS",
                "LAST_EPISODE_NHS_CD_ALIAS",
                "OPERATION_STATUS_NHS_CD_ALIAS",
                "PSYCH_PAT_STATUS_NHS_CD_ALIAS",
                "AMBULANCE_INCIDENT_IDENT",
                "ENCNTR_SLICE_UPDT_PRSNL_ID"
        };
    }

    public CsvCell getCDSBatchContentEventId() {
        return super.getCell("#CDS_BATCH_CONTENT_ID");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getEncounterSliceID() {
        return super.getCell("ENCNTR_SLICE_ID");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getPersonId() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getHospitalProviderId() {
        return super.getCell("HOSP_PRVDR_IDENT");
    }

    public CsvCell getEpisodeSequenceNumber() {
        return super.getCell("EPISODE_SEQ_NBR");
    }

    public CsvCell getEpisodeStartDateTime() {
        return super.getCell("EPISODE_START_DT_TM");
    }

    public CsvCell getEpisodeEndDateTime() {
        return super.getCell("EPISODE_END_DT_TM");
    }

    public CsvCell getFirstDayNightAdmissionCode() {
        return super.getCell("FIRST_REG_DAY_NIGHT_NHS_CD_ALIAS");
    }

    public CsvCell getLastEpisodeInSpellIndicatorCode() {
        return super.getCell("LAST_EPISODE_NHS_CD_ALIAS");
    }

    public CsvCell getOperationStatusCode() {
        return super.getCell("OPERATION_STATUS_NHS_CD_ALIAS");
    }

    public CsvCell getPsychiatricPatientStatusCode() {
        return super.getCell("PSYCH_PAT_STATUS_NHS_CD_ALIAS");
    }

    public CsvCell getAmbulanceIncidentIdentifier() {
        return super.getCell("AMBULANCE_INCIDENT_IDENT");
    }

    public CsvCell getEncounterSliceLastUpdatedByPersonnelIdentifier() {
        return super.getCell("ENCNTR_SLICE_UPDT_PRSNL_ID");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}