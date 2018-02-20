package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PPAGP extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPAGP.class);

    public PPAGP(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "PERSON_PRSNL_RELTN_ID",
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

    public String getMillenniumPersonPersonnelRelationId() {
        return super.getString("PERSON_PRSNL_RELTN_ID");
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

    public String getRegisteredGPMillenniumPersonnelId() {
        return super.getString("GP_PRSNL_ID");
    }

    public String getRegisteredGPPracticeMillenniumIdOrganisationCode() {
        return super.getString("GP_PRAC_ORG_ID");
    }

    public String getPersonPersonnelRelationCode() {
        return super.getString("PERSON_PRSNL_R_CD");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner registered GP file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}

