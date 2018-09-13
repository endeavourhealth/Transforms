package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPNAM extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPNAM.class);

    public PPNAM(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#PERSON_NAME_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "PERSON_NAME_TYPE_CD",
                "FIRST_NAME_TXT",
                "MIDDLE_NAME_TXT",
                "LAST_NAME_TXT",
                "TITLE_TXT",
                "NAME_PREFIX_TXT",
                "NAME_SUFFIX_TXT",
                "NAME_TYPE_SEQ_NBR"
        };

    }

    public CsvCell getMillenniumPersonNameId() {
        return super.getCell("#PERSON_NAME_ID");
    }

    public CsvCell getExtractDateTime() throws TransformException {
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

    public CsvCell getNameTypeCode() {
        return super.getCell("PERSON_NAME_TYPE_CD");
    }

    public CsvCell getFirstName() {
        return super.getCell("FIRST_NAME_TXT");
    }

    public CsvCell getMiddleName() {
        return super.getCell("MIDDLE_NAME_TXT");
    }

    public CsvCell getLastName() {
        return super.getCell("LAST_NAME_TXT");
    }

    public CsvCell getTitle() {
        return super.getCell("TITLE_TXT");
    }

    public CsvCell getPrefix() {
        return super.getCell("NAME_PREFIX_TXT");
    }

    public CsvCell getSuffix() {
        return super.getCell("NAME_SUFFIX_TXT");
    }

    public CsvCell getNameTypeSequence() {
        return super.getCell("NAME_TYPE_SEQ_NBR");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}