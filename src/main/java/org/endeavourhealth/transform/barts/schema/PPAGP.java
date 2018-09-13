package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPAGP extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPAGP.class);

    public PPAGP(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#PERSON_PRSNL_RELTN_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "GP_PRSNL_ID",
                "GP_PRAC_ORG_ID",
                "PERSON_PRSNL_R_CD",
        };
    }

    public CsvCell getMillenniumPersonPersonnelRelationId() {
        return super.getCell("#PERSON_PRSNL_RELTN_ID");
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

    public CsvCell getEndEffectiveDate() {
        return super.getCell("END_EFFECTIVE_DT_TM");
    }

    public CsvCell getRegisteredGPMillenniumPersonnelId() {
        return super.getCell("GP_PRSNL_ID");
    }

    public CsvCell getRegisteredGPPracticeMillenniumIdOrganisationCode() {
        return super.getCell("GP_PRAC_ORG_ID");
    }

    public CsvCell getPersonPersonnelRelationCode() {
        return super.getCell("PERSON_PRSNL_R_CD");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}

