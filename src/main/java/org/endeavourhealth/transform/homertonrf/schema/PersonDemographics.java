package org.endeavourhealth.transform.homertonrf.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonrf.HomertonRfCsvToFhirTransformer;

import java.util.UUID;

public class PersonDemographics extends AbstractCsvParser {

    public PersonDemographics(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
        HomertonRfCsvToFhirTransformer.CSV_FORMAT,
        HomertonRfCsvToFhirTransformer.DATE_FORMAT,
        HomertonRfCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "EMPI_ID",
                    "PERSON_SEQ",
                    "REFERENCE_ID",
                    "BIRTH_DATE",
                    "BIRTH_DATE_ID",
                    "GENDER_CODE",
                    "GENDER_DISPLAY",
                    "GENDER_CODING_SYSTEM_ID",
                    "GENDER_RAW_CODING_SYSTEM_ID",
                    "GENDER_RAW_CODE",
                    "MARITAL_STATUS_CODE",
                    "MARITAL_STATUS_DISPLAY",
                    "MARITAL_CODING_SYSTEM_ID",
                    "MARITAL_RAW_CODING_SYSTEM_ID",
                    "MARITAL_RAW_CODE",
                    "ETHNICITY_CODE",
                    "ETHNICITY_DISPLAY",
                    "ETHNICITY_CODING_SYSTEM_ID",
                    "ETHNICITY_RAW_CODING_SYSTEM_ID",
                    "ETHNICITY_RAW_CODE",
                    "RELIGION_CODE",
                    "RELIGION_DISPLAY",
                    "RELIGION_CODING_SYSTEM_ID",
                    "RELIGION_RAW_CODING_SYSTEM_ID",
                    "RELIGION_RAW_CODE",
                    "DECEASED",
                    "DECEASED_DT_TM",
                    "DECEASED_DATE_ID",
                    "CAUSE_OF_DEATH_CODE",
                    "CAUSE_OF_DEATH_DISPLAY",
                    "CAUSE_OF_DEATH_CODING_SYSTEM_ID",
                    "CAUSE_OF_DEATH_RAW_CODING_SYSTEM_ID",
                    "CAUSE_OF_DEATH_RAW_CODE",
                    "SOURCE_TYPE",
                    "SOURCE_ID",
                    "SOURCE_VERSION",
                    "SOURCE_DESCRIPTION",
                    "POPULATION_ID",
                    "GENDER_PRIMARY_DISPLAY",
                    "MARITAL_STATUS_PRIMARY_DISPLAY",
                    "ETHNICITY_PRIMARY_DISPLAY",
                    "RELIGION_PRIMARY_DISPLAY",
                    "CAUSE_OF_DEATH_PRIMARY_DISPLAY",
                    "SOURCE_TYPE_KEY",
                    "RAW_ENTITY_KEY",
                    "SUPPORTING_FACT_ID",
                    "SUPPORTING_FACT_TYPE",
                    "SUPPORTING_FACT_ENTITY_ID",
                    "RECORD_DATA_PARTITION_ID",
                    "RECORD_PERSON_ID",
                    "DEMOGRAPHICS_PERSON_ID",
                    "BIRTH_SEX_RAW_CODE",
                    "BIRTH_SEX_RAW_CODING_SYSTEM_ID",
                    "BIRTH_SEX_DISPLAY",
                    "BIRTH_SEX_CODE",
                    "BIRTH_SEX_PRIMARY_DISPLAY",
                    "BIRTH_SEX_CODING_SYSTEM_ID",
                    "BIRTH_DT_TM",
                    "DECEASED_TIME_ID",
                    "DECEASED_DATE",
                    "BIRTH_TIME_ID",
                    "HASH_VALUE"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("EMPI_ID");
    }

    public CsvCell getMaritalStatusCernerCode() {
        return super.getCell("MARITAL_RAW_CODE");
    }

    public CsvCell getEthnicityCernerCode() {
        return super.getCell("ETHNICITY_RAW_CODE");
    }

    public CsvCell getReligionCernerCode() {
        return super.getCell("RELIGION_RAW_CODE");
    }

    public CsvCell getIsDecesed() { return super.getCell("DECEASED"); }

    public CsvCell getCauseOfDeathCernerCode() { return super.getCell("CAUSE_OF_DEATH_RAW_CODE"); }

    public CsvCell getHashValue() { return super.getCell("HASH_VALUE"); }

}
