package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ENCINF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(ENCINF.class);

    public ENCINF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#ENC_INFO_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "ENCNTR_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "INFO_TYPE_CD",
                "INFO_SUB_TYPE_CD",
                "VALUE_CD",
                "VALUE_NBR",
                "VALUE_DT_TM",
                "VALUE_LONG_TXT"
        };
    }


    public CsvCell getMillenniumEncounterInfoIdentifier() {
        return super.getCell("#ENC_INFO_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getBeginEffectiveDateTime() {
        return super.getCell("BEG_EFFECTIVE_DT_TM");
    }

    public CsvCell getEndEffectiveDateTime() {
        return super.getCell("END_EFFECTIVE_DT_TM");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner encounter info file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }


}