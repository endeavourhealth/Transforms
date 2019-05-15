package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPPHO extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPPHO.class);

    public PPPHO(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#PHONE_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "PHONE_TYPE_CD",
                "PHONE_TYPE_SEQ_NBR",
                "PHONE_NBR_TXT",
                "EXTENSION_TXT",
                "CONTACT_METHOD_CD",
        };
    }

    public CsvCell getMillenniumPhoneId() {
        return super.getCell("#PHONE_ID");
    }

    public CsvCell getExtractDateTime()  {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getMillenniumPersonIdentifier() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getBeginEffectiveDateTime() {
        return super.getCell("BEG_EFFECTIVE_DT_TM");
    }

    public CsvCell getEndEffectiveDateTime() {
        return super.getCell("END_EFFECTIVE_DT_TM");
    }

    public CsvCell getPhoneTypeCode() {
        return super.getCell("PHONE_TYPE_CD");
    }

    public CsvCell getPhoneTypeSequence() {
        return super.getCell("PHONE_TYPE_SEQ_NBR");
    }

    public CsvCell getPhoneNumber() {
        return super.getCell("PHONE_NBR_TXT");
    }

    public CsvCell getExtension() {
        return super.getCell("EXTENSION_TXT");
    }

    public CsvCell getContactMethodCode() {
        return super.getCell("CONTACT_METHOD_CD");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
