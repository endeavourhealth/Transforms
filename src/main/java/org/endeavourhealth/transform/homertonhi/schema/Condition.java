package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class Condition extends AbstractCsvParser {

    public Condition(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
        HomertonHiCsvToFhirTransformer.CSV_FORMAT,
        HomertonHiCsvToFhirTransformer.DATE_FORMAT,
        HomertonHiCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "CONDITION_ID",
                    "REFERENCE_ID",
                    "DATA_SOURCE_CONDITION_ID",
                    "EMPI_ID",
                    "ENCOUNTER_ID",
                    "CONDITION_CODE",
                    "CONDITION_DISPLAY",
                    "CONDITION_CODING_SYSTEM_ID",
                    "CONDITION_RAW_CODING_SYSTEM_ID",
                    "CONDITION_RAW_CODE",
                    "DESCRIPTION",
                    "EFFECTIVE_DT_TM",
                    "EFFECTIVE_DATE_ID",
                    "CONDITION_TYPE_CODE",
                    "CONDITION_TYPE_DISPLAY",
                    "CONDITION_TYPE_CODING_SYSTEM_ID",
                    "CONDITION_TYPE_RAW_CODING_SYSTEM_ID",
                    "CONDITION_TYPE_RAW_CODE",
                    "CLASSIFICATION_CODE",
                    "CLASSIFICATION_DISPLAY",
                    "CLASSIFICATION_CODING_SYSTEM_ID",
                    "CLASSIFICATION_RAW_CODING_SYSTEM_ID",
                    "CLASSIFICATION_RAW_CODE",
                    "CONFIRMATION_STATUS_CODE",
                    "CONFIRMATION_STATUS_DISPLAY",
                    "CONFIRMATION_STATUS_CODING_SYSTEM_ID",
                    "CONFIRMATION_STATUS_RAW_CODING_SYSTEM_ID",
                    "CONFIRMATION_STATUS_RAW_CODE",
                    "STATUS_CODE",
                    "STATUS_DISPLAY",
                    "STATUS_CODING_SYSTEM_ID",
                    "STATUS_RAW_CODING_SYSTEM_ID",
                    "STATUS_RAW_CODE",
                    "STATUS_DT_TM",
                    "STATUS_DATE_ID",
                    "UPDATE_DT_TM",
                    "UPDATE_DATE_ID",
                    "PRESENT_ON_ADMISSION_CODE",
                    "PRESENT_ON_ADMISSION_DISPLAY",
                    "PRESENT_ON_ADMISSION_CODING_SYSTEM_ID",
                    "PRESENT_ON_ADMISSION_RAW_CODING_SYSTEM_ID",
                    "PRESENT_ON_ADMISSION_RAW_CODE",
                    "PRESENT_ON_ADMISSION_PRIMARY_DISPLAY",
                    "SOURCE_TYPE",
                    "SOURCE_ID",
                    "SOURCE_VERSION",
                    "SOURCE_DESCRIPTION",
                    "RANK_TYPE",
                    "POPULATION_ID",
                    "CONDITION_PRIMARY_DISPLAY",
                    "CONDITION_TYPE_PRIMARY_DISPLAY",
                    "CLASSIFICATION_PRIMARY_DISPLAY",
                    "CONFIRMATION_STATUS_PRIMARY_DISPLAY",
                    "STATUS_PRIMARY_DISPLAY",
                    "RESPONSIBLE_PROVIDER_ID",
                    "UPDATE_PROVIDER_ID",
                    "CLAIM_ID",
                    "SOURCE_TYPE_KEY",
                    "BILLING_RANK_TYPE_KEY",
                    "SUPPORTING_FACT_ID",
                    "SUPPORTING_FACT_TYPE",
                    "SUPPORTING_FACT_ENTITY_ID",
                    "RAW_ENTITY_KEY",
                    "EFFECTIVE_DATE",
                    "EFFECTIVE_TIME_ID",
                    "UPDATE_DATE",
                    "UPDATE_TIME_ID",
                    "ASSERTED_DT_TM",
                    "ASSERTED_DATE_ID",
                    "RECORDER_PROVIDER_ID",
                    "STATUS_TIME_ID",
                    "STATUS_DATE",
                    "ASSERTED_TIME_ID",
                    "ASSERTED_DATE",
                    "HASH_VALUE"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getConditionId() {
        return super.getCell("CONDITION_ID");
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("EMPI_ID");
    }

    public CsvCell getConditionTypeCode() { return super.getCell("CONDITION_TYPE_CODE"); }

    public CsvCell getConditionRawCode() { return super.getCell("CONDITION_RAW_CODE"); }

    public CsvCell getConditionCodingSystemId() { return super.getCell("CONDITION_CODING_SYSTEM_ID"); }

    public CsvCell getConditionConfirmationStatusDisplay() { return super.getCell("CONFIRMATION_STATUS_DISPLAY"); }

    public CsvCell getConditionDisplay() { return super.getCell("CONDITION_DISPLAY"); }

    public CsvCell getProblemStatusDisplay() { return super.getCell("STATUS_DISPLAY"); }

    public CsvCell getProblemStatusDtm() { return super.getCell("STATUS_DT_TM"); }

    public CsvCell getConditionDescription() { return super.getCell("DESCRIPTION"); }

    public CsvCell getEncounterId() { return super.getCell("ENCOUNTER_ID");  }

    public CsvCell getEffectiveDtm() { return super.getCell("EFFECTIVE_DT_TM"); }

    public CsvCell getHashValue() { return super.getCell("HASH_VALUE"); }
}

