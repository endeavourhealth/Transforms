package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPINF extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPINF.class);

    public PPINF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#PERSON_INFO_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "INFO_TYPE_CD",
                "INFO_SUB_TYPE_CD",
                "VALUE_CD",
                "VALUE_DT_TM",
                "VALUE_NBR",
                "VALUE_LONG_TEXT_ID",
        };

    }

    public CsvCell getMillenniumPersonInformationId() {
        return super.getCell("#PERSON_INFO_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getMillenniumPersonIdentifier() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getBeginEffectiveDate() {
        return super.getCell("BEG_EFFECTIVE_DT_TM");
    }

    public CsvCell getEndEffectiveDater() {
        return super.getCell("END_EFFECTIVE_DT_TM");
    }

    public CsvCell getInfoTypeCode() {
        return super.getCell("INFO_TYPE_CD");
    }

    public CsvCell getInfoSubTypeCode() {
        return super.getCell("INFO_SUB_TYPE_CD");
    }

    public CsvCell getValueMillenniumCode() {
        return super.getCell("VALUE_CD");
    }

    public CsvCell getDateTimeValue() {
        return super.getCell("VALUE_DT_TM");
    }

    public CsvCell getNumericValue() {
        return super.getCell("VALUE_NBR");
    }

    public CsvCell getValueLongTextMillenniumIdentifier() {
        return super.getCell("VALUE_LONG_TEXT_ID");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
