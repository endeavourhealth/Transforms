package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CLEVE extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVE.class);

    public CLEVE(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#EVENT_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "ENCNTR_ID",
                "ORDER_ID",
                "PARENT_EVENT_ID",
                "EVENT_CD",
                "ACCESSION_NBR_IDENT",
                "EVENT_START_DT_TM",        //date  The Clinical date and time for the start of this event
                "EVENT_END_DT_TM",          //date  Event end date and time
                "CLIN_SIGNIFICANCE_DT_TM",  //date  Clinical Significant date and time
                "EVENT_CLASS_CD",
                "EVENT_RESULT_STATUS_CD",
                "EVENT_RESULT_TXT",
                "EVENT_RESULT_NBR",
                "EVENT_RESULT_UNITS_CD",
                "EVENT_RESULT_DT",
                "NORMALCY_CD",
                "NORMAL_VALUE_LOW_TXT",
                "NORMAL_VALUE_HIGH_TXT",
                "EVENT_PERFORMED_DT_TM",    //recorded date?
                "EVENT_PERFORMED_PRSNL_ID", //recorded person AND practitioner
                "EVENT_VERIFIED_DT_TM",
                "EVENT_VERIFIED_PRSNL_ID",
                "AUTHENTIC_IND",
                "COLLATION_SEQ_TXT",
                "CRITICAL_HIGH_TXT",
                "CRITICAL_LOW_TXT",
                "ENTRY_MODE_CD",
                "EVENT_RELTN_CD",
                "EVENT_TAG_TXT",
                "EVENT_TITLE_TXT",
                "RECORD_STATUS_CD",
                "REFERENCE_NBR",
                "VALID_FROM_DT_TM",
                "CONTRIBUTOR_SYSTEM_CD",
                "CODE_DISP_TXT",
        };
    }

    public CsvCell getEventId() {
        return super.getCell("#EVENT_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getPersonId() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getOrderId() {
        return super.getCell("ORDER_ID");
    }

    public CsvCell getParentEventId() {
        return super.getCell("PARENT_EVENT_ID");
    }

    public CsvCell getEventCode() {
        return super.getCell("EVENT_CD");
    }

    public CsvCell getAccessionNumberIdentity() {
        return super.getCell("ACCESSION_NBR_IDENT");
    }

    public CsvCell getEventStartDateTime() {
        return super.getCell("EVENT_START_DT_TM");
    }

    public CsvCell getEventEndDateTime() {
        return super.getCell("EVENT_END_DT_TM");
    }

    public CsvCell getClinicallySignificantDateTime() {
        return super.getCell("CLIN_SIGNIFICANCE_DT_TM");
    }

    public CsvCell getEventCodeClass() {
        return super.getCell("EVENT_CLASS_CD");
    }

    public CsvCell getEventResultStatusCode() {
        return super.getCell("EVENT_RESULT_STATUS_CD");
    }

    public CsvCell getEventResultText() {
        return super.getCell("EVENT_RESULT_TXT");
    }

    public CsvCell getEventResultNumber() {
        return super.getCell("EVENT_RESULT_NBR");
    }

    public CsvCell getEventResultUnitsCode() {
        return super.getCell("EVENT_RESULT_UNITS_CD");
    }

    public CsvCell getEventResultDateTime() {
        return super.getCell("EVENT_RESULT_DT");
    }

    public CsvCell getEventNormalcyCode() {
        return super.getCell("NORMALCY_CD");
    }

    public CsvCell getEventNormalRangeLow() {
        return super.getCell("NORMAL_VALUE_LOW_TXT");
    }

    public CsvCell getEventNormalRangeHigh() {
        return super.getCell("NORMAL_VALUE_HIGH_TXT");
    }

    public CsvCell getEventPerformedDateTime() {
        return super.getCell("EVENT_PERFORMED_DT_TM");
    }

    public CsvCell getEventPerformedPersonnelId() {
        return super.getCell("EVENT_PERFORMED_PRSNL_ID");
    }

    public CsvCell getVerifiedDateTime() {
        return super.getCell("EVENT_VERIFIED_DT_TM");
    }

    public CsvCell getVerifiedPersonnelId() {
        return super.getCell("EVENT_VERIFIED_PRSNL_ID");
    }

    public CsvCell getAuthenticationIndicator() {
        return super.getCell("AUTHENTIC_IND");
    }

    public CsvCell getCollationSequenceText() {
        return super.getCell("COLLATION_SEQ_TXT");
    }

    public CsvCell getCriticalHighText() {
        return super.getCell("CRITICAL_HIGH_TXT");
    }

    public CsvCell getCriticalLowText() {
        return super.getCell("CRITICAL_LOW_TXT");
    }

    public CsvCell getEncryModeCode() {
        return super.getCell("ENTRY_MODE_CD");
    }

    public CsvCell getEventRelinCode() {
        return super.getCell("EVENT_RELTN_CD");
    }

    public CsvCell getEventTag() {   //use for display if EventTitleText is null
        return super.getCell("EVENT_TAG_TXT");
    }

    public CsvCell getEventTitleText() {
        return super.getCell("EVENT_TITLE_TXT");
    }

    public CsvCell getRecordStatusReference() {
        return super.getCell("RECORD_STATUS_CD");
    }

    public CsvCell getReferenceNumber() {
        return super.getCell("REFERENCE_NBR");
    }

    public CsvCell getValidFromDateTime() {
        return super.getCell("VALID_FROM_DT_TM");
    }

    public CsvCell getContributorSystemMillenniumCode() {
        return super.getCell("CONTRIBUTOR_SYSTEM_CD");
    }

    public CsvCell getCodeDisplayText() {
        return super.getCell("CODE_DISP_TXT");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}