package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class ENCNT extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNT.class);

    //public static final String DATE_FORMAT = "dd/mm/yyyy";
    //public static final String TIME_FORMAT = "hh:mm:ss";
    //public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public ENCNT(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
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


    public CsvCell getMillenniumEncounterIdentifier() throws FileFormatException {
        return super.getCell("#ENCNTR_ID");
    }

    public CsvCell getExtractDateTime() throws TransformException {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() throws FileFormatException {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getMillenniumPersonIdentifier() throws FileFormatException {
        return super.getCell("PERSON_ID");}

    public CsvCell getMillenniumFinancialNumberIdentifier() throws FileFormatException {
        return super.getCell("FIN_NBR_ID");}

    public CsvCell getEncounterCreateDateTime() throws TransformException {
        return super.getCell("CREATE_DT_TM");}

    public CsvCell getEncounterTypeMillenniumCode() throws FileFormatException {
        return super.getCell("ENC_TYPE_CD");}

    public CsvCell getEncounterStatusMillenniumCode() throws FileFormatException {
        return super.getCell("ENC_STATUS_CD");}

    public CsvCell getMilleniumSourceIdentifierForVisit() throws FileFormatException {
        return super.getCell("VISIT_ID");}

    public CsvCell getReferralWrittenDate() throws TransformException {
        return super.getCell("REFERRAL_WRITTEN_DT_TM");}

    public CsvCell getReferalReceivedDate() throws TransformException {
        return super.getCell("REFERRAL_RECEIVED_DT_TM");}

    public CsvCell getSourceofReferralMillenniumCode() throws FileFormatException {
        return super.getCell("SOURCE_OF_REFERRAL_CD");}

    public CsvCell getReferrerMillenniumPersonnelIdentifier() throws FileFormatException {
        return super.getCell("REFERRER_PRSNL_ID");}

    public CsvCell getReasonForVisitText() throws FileFormatException {
        return super.getCell("REASON_FOR_VISIT_TXT");}

    public CsvCell getCurrentMainSpecialtyMillenniumCode() throws FileFormatException {
        return super.getCell("MAIN_SPECIALTY_CD");}

    public CsvCell getCurrentTreatmentFunctionMillenniumCode() throws FileFormatException {
        return super.getCell("TREATMENT_FUNCTION_CD");}

    public CsvCell getCurrenrLocalSpecialtyCode() throws FileFormatException {
        return super.getCell("LOCAL_SPECIALTY_NHS_CD_ALIAS");}

    public CsvCell getCurrentInstitutionMillenniumLocationCode() throws FileFormatException {
        return super.getCell("CURRENT_LOC_INSTITUTION_CD");}

    public CsvCell getMillenniumAdministrativeCategoty() throws FileFormatException {
        return super.getCell("ADMIN_CATEGORY_CD");}

    public CsvCell getEpisodeIdentifier() throws FileFormatException {
        return super.getCell("EPISODE_ID");}

    public CsvCell getResponsibleHealthCareprovidingPersonnelIdentifier() throws FileFormatException {
        return super.getCell("RESP_HCP_PRSNL_ID");}

    public CsvCell getRegisteringMillenniumPersonnelIdentifier() throws FileFormatException {
        return super.getCell("REGISTERING_PRSNL_ID");}

    public CsvCell getCurrentLocationIdentifier() throws FileFormatException {
        return super.getCell("CURRENT_LOC_ID");}

    @Override
    protected String getFileTypeDescription() {
        return "Cerner encounter file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }


}