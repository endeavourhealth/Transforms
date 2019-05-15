package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class IPWDS extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(IPWDS.class);

    public IPWDS(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#CDS_WARD_STAY_KEY",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "CDS_BATCH_CONTENT_ID",
                "ENCNTR_ID",
                "PERSON_ID",
                "WARD_STAY_START_DT_TM",
                "WARD_STAY_END_DT_TM",
                "WARD_STAY_SEQ_NBR",
                "WARD_LOC_CD",
                "WARD_AGE_GRP_INT_NHS_CD_ALIAS",
                "WARD_CC_INTENSITY_NHS_CD_ALIAS",
                "WARD_DAY_AVAIL_NHS_CD_ALIAS",
                "WARD_NIGHT_AVAIL_NHS_CD_ALIAS",
                "WARD_PAT_SEX_NHS_CD_ALIAS",
                "WARD_TREAT_SITE_NHS_CD_ALIAS",
                "ENCNTR_SLICE_ID",
                "WARD_ROOM_CD",
                "WARD_BED_CD",
                "WARD_LOC_TYPE_NHS_CD_ALIAS"
        };
    }

    public CsvCell getCDSWardStayId() {
        return super.getCell("#CDS_WARD_STAY_KEY");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getCDSBatchContentEventId() {
        return super.getCell("CDS_BATCH_CONTENT_ID");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getPatientId() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getWardStayStartDateTime() {
        return super.getCell("WARD_STAY_START_DT_TM");
    }

    public CsvCell getWardStayEndDateTime() {
        return super.getCell("WARD_STAY_END_DT_TM");
    }

    public CsvCell getWardStaySequenceNumber() {
        return super.getCell("WARD_STAY_SEQ_NBR");
    }

    public CsvCell getWardStayLocationCode() {
        return super.getCell("WARD_LOC_CD");
    }

    public CsvCell getWardAgeGroupIntendedCode() {
        return super.getCell("WARD_AGE_GRP_INT_NHS_CD_ALIAS");
    }

    public CsvCell getWardClinicalCareIntensityCode() {
        return super.getCell("WARD_CC_INTENSITY_NHS_CD_ALIAS");
    }

    public CsvCell getWardDayAvailablitlityCode() {
        return super.getCell("WARD_DAY_AVAIL_NHS_CD_ALIAS");
    }

    public CsvCell getWardNightAvailablitlityCode() {
        return super.getCell("WARD_NIGHT_AVAIL_NHS_CD_ALIAS");
    }

    public CsvCell getWardPatientSexCode() {
        return super.getCell("WARD_PAT_SEX_NHS_CD_ALIAS");
    }

    public CsvCell getWardTreatmentSiteCode() {
        return super.getCell("WARD_TREAT_SITE_NHS_CD_ALIAS");
    }

    public CsvCell getEncounterSliceId() {
        return super.getCell("ENCNTR_SLICE_ID");
    }

    public CsvCell getWardRoomCode() {
        return super.getCell("WARD_ROOM_CD");
    }

    public CsvCell getWardBedCode() {
        return super.getCell("WARD_BED_CD");
    }

    public CsvCell getWardLocationTypeCode() {
        return super.getCell("WARD_LOC_TYPE_NHS_CD_ALIAS");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}