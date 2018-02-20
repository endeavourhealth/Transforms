package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PPNAM extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPNAM.class);

    public PPNAM(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "PERSON_NAME_ID",
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

    public String getMillenniumPersonNameId() {
        return super.getString("PERSON_NAME_ID");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDate("EXTRACT_DT_TM");
    }

    public String getActiveIndicator() {
        return super.getString("ACTIVE_IND");
    }

    public boolean isActive() {
        int val = super.getInt("ACTIVE_IND");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public String getMillenniumPersonIdentifier() {
        return super.getString("PERSON_ID");
    }

    public Date getBeginEffectiveDate() throws TransformException {
        return super.getDate("BEG_EFFECTIVE_DT_TM");
    }

    public Date getEndEffectiveDater() throws TransformException {
        return super.getDate("END_EFFECTIVE_DT_TM");
    }

    public String getNameTypeCode() {
        return super.getString("PERSON_NAME_TYPE_CD");
    }

    public String getFirstName() {
        return super.getString("FIRST_NAME_TXT");
    }

    public String getMiddleName() {
        return super.getString("MIDDLE_NAME_TXT");
    }

    public String getLastName() {
        return super.getString("LAST_NAME_TXT");
    }

    public String getTitle() {
        return super.getString("TITLE_TXT");
    }

    public String getPrefix() {
        return super.getString("NAME_PREFIX_TXT");
    }

    public String getSuffix() {
        return super.getString("NAME_SUFFIX_TXT");
    }

    public String getNameTypeSequence() {
        return super.getString("NAME_TYPE_SEQ_NBR");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner person name file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}