package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class CLEVE extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVE.class);

    public static final int RECORD_ACTIVE = 1;

    public CLEVE(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "EVENT_ID",
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

    public String getEventId() {
        return super.getString("EVENT_ID");
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
    }
    @Override
    protected String getFileTypeDescription() {
        return "Cerner clinical events file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}