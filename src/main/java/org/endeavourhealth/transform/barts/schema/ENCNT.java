package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ENCNT extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNT.class);

    //public static final String DATE_FORMAT = "dd/mm/yyyy";
    //public static final String TIME_FORMAT = "hh:mm:ss";
    //public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public ENCNT(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#ENCNTR_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "FIN_NBR_ID",
                "CREATE_DT_TM",
                "ENC_TYPE_CD",
                "ENC_STATUS_CD",
                "VISIT_ID",
                "REFERRAL_WRITTEN_DT_TM",
                "REFERRAL_RECEIVED_DT_TM",
                "SOURCE_OF_REFERRAL_CD",
                "REFERRER_PRSNL_ID",
                "REASON_FOR_VISIT_TXT",
                "MAIN_SPECIALTY_CD",
                "TREATMENT_FUNCTION_CD",
                "LOCAL_SPECIALTY_NHS_CD_ALIAS",
                "CURRENT_LOC_INSTITUTION_CD",
                "ADMIN_CATEGORY_CD",
                "EPISODE_ID",
                "RESP_HCP_PRSNL_ID",
                "REGISTERING_PRSNL_ID",
                "CURRENT_LOC_ID"
        };
    }


    public CsvCell getMillenniumEncounterIdentifier() {
        return super.getCell("#ENCNTR_ID");
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

    public CsvCell getMillenniumFinancialNumberIdentifier() {
        return super.getCell("FIN_NBR_ID");
    }

    public CsvCell getEncounterCreateDateTime() {
        return super.getCell("CREATE_DT_TM");
    }

    public CsvCell getEncounterTypeMillenniumCode() {
        return super.getCell("ENC_TYPE_CD");
    }

    public CsvCell getEncounterStatusMillenniumCode() {
        return super.getCell("ENC_STATUS_CD");
    }

    public CsvCell getMilleniumSourceIdentifierForVisit() {
        return super.getCell("VISIT_ID");
    }

    public CsvCell getReferralWrittenDate() {
        return super.getCell("REFERRAL_WRITTEN_DT_TM");
    }

    public CsvCell getReferalReceivedDate() {
        return super.getCell("REFERRAL_RECEIVED_DT_TM");
    }

    public CsvCell getSourceofReferralMillenniumCode() {
        return super.getCell("SOURCE_OF_REFERRAL_CD");
    }

    public CsvCell getReferrerMillenniumPersonnelIdentifier() {
        return super.getCell("REFERRER_PRSNL_ID");
    }

    public CsvCell getReasonForVisitText() {
        return super.getCell("REASON_FOR_VISIT_TXT");
    }

    public CsvCell getMainSpecialtyMillenniumCode() {
        return super.getCell("MAIN_SPECIALTY_CD");
    }

    public CsvCell getCurrentTreatmentFunctionMillenniumCode() {
        return super.getCell("TREATMENT_FUNCTION_CD");
    }

    public CsvCell getCurrenrLocalSpecialtyCode() {
        return super.getCell("LOCAL_SPECIALTY_NHS_CD_ALIAS");
    }

    public CsvCell getCurrentInstitutionMillenniumLocationCode() {
        return super.getCell("CURRENT_LOC_INSTITUTION_CD");
    }

    public CsvCell getMillenniumAdministrativeCategoty() {
        return super.getCell("ADMIN_CATEGORY_CD");
    }

    public CsvCell getEpisodeIdentifier() {
        return super.getCell("EPISODE_ID");
    }

    public CsvCell getResponsibleHealthCareprovidingPersonnelIdentifier() {
        return super.getCell("RESP_HCP_PRSNL_ID");
    }

    public CsvCell getRegisteringMillenniumPersonnelIdentifier() {
        return super.getCell("REGISTERING_PRSNL_ID");
    }

    public CsvCell getCurrentLocationIdentifier() {
        return super.getCell("CURRENT_LOC_ID");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner encounter file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }


}