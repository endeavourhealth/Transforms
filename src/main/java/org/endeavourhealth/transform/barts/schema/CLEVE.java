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
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
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
                "EVENT_START_DT_TM",
                "EVENT_END_DT_TM",
                "CLIN_SIGNIFICANCE_DT_TM",
                "EVENT_CLASS_CD",
                "EVENT_RESULT_STATUS_CD",
                "EVENT_RESULT_TXT",
                "EVENT_RESULT_NBR",
                "EVENT_RESULT_UNITS_CD",
                "EVENT_RESULT_DT",
                "NORMALCY_CD",
                "NORMAL_VALUE_LOW_TXT",
                "NORMAL_VALUE_HIGH_TXT",
                "EVENT_PERFORMED_DT_TM",
                "EVENT_PERFORMED_PRSNL_ID",
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

    public CsvCell getPatientId() {
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

    public CsvCell getEventResultClassCode() {
        return super.getCell("EVENT_RESULT_STATUS_CD");
    }

    public CsvCell getEventResultText() {
        return super.getCell("EVENT_RESULT_TXT");
    }

    //NOTE; numeric results have the value in this field AND in EVENT_RESULT_TXT, except this field
    //is rounded to the nearest int. So DO NOT USE this field, and use the EVENT_RESULT_TXT instead
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

    public CsvCell getRecordStatusreference() {
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
    
    /*public String getEventId() {
        return super.getString("#EVENT_ID");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDate("EXTRACT_DT_TM");
    }

    public String getActiveIndicator() {
        return super.getString("ACTIVE_IND");
    }

    public boolean isActive() {
        return (super.getInt("ACTIVE_IND") == RECORD_ACTIVE);
    }

    public String getPatientId() {
        return super.getString("PERSON_ID");
    }

    public String getEncounterId() {
        return super.getString("ENCNTR_ID");
    }

    public String getEventCode() {
        return super.getString("EVENT_CD");
    }

    public String getEventCodeClass() {
        return super.getString("EVENT_CLASS_CD");
    }

    public String getEventResultClassCode() {
        return super.getString("EVENT_RESULT_STATUS_CD");
    }

    public String getEventResultAsText() {
        return super.getString("EVENT_RESULT_TXT");
    }

    public String getEventUnitsCode() {
        return super.getString("EVENT_RESULT_UNITS_CD");
    }

    public String getEventNormalRangeLow() {
        return super.getString("NORMAL_VALUE_LOW_TXT");
    }

    public String getEventNormalcyCode() {
        return super.getString("NORMALCY_CD");
    }
    public String getEventNormalRangeHigh() {
        return super.getString("NORMAL_VALUE_HIGH_TXT");
    }

    public Date getEffectiveDateTime() throws TransformException {
        return super.getDate("CLIN_SIGNIFICANCE_DT_TM");
    }

    public String getEffectiveDateTimeAsString() throws TransformException {
        return super.getString("EVENT_PERFORMED_DT_TM");
    }

    public String getClinicianID() {
        return super.getString("EVENT_PERFORMED_PRSNL_ID");
    }

    public String getEventTag() {   //use for display if EventTitleText is null
        return super.getString("EVENT_TAG_TXT");
    }

    public String getEventTitleText() {
        return super.getString("EVENT_TITLE_TXT");
    }

    public String getRecordStatusreference() {
        return super.getString("RECORD_STATUS_CD");
    }

    //TODO Do we need some boolean methods on status?
    public String getReferenceNumber() {
        return super.getString("REFERENCE_NBR");
    }

    public Date getValidFromDateTime() throws TransformException {
        return super.getDate("VALID_FROM_DT_TM");
    }

    public String getValidFromDateTimeasString() throws TransformException {
        return super.getString("VALID_FROM_DT_TM");
    }

    public String getContributorSystemMillenniumCode() {
        return super.getString("CONTRIBUTOR_SYSTEM_CD");
    }*/


    @Override
    protected String getFileTypeDescription() {
        return "Cerner clinical events file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}