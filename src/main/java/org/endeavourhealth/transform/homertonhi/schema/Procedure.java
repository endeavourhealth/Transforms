package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class Procedure extends AbstractCsvParser {

    public Procedure(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
        HomertonHiCsvToFhirTransformer.CSV_FORMAT,
        HomertonHiCsvToFhirTransformer.DATE_FORMAT,
        HomertonHiCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "PROCEDURE_ID",
                    "REFERENCE_ID",
                    "EMPI_ID",
                    "ENCOUNTER_ID",
                    "PROCEDURE_CODE",
                    "PROCEDURE_DISPLAY",
                    "PROCEDURE_CODING_SYSTEM_ID",
                    "PROCEDURE_RAW_CODING_SYSTEM_ID",
                    "PROCEDURE_RAW_CODE",
                    "DESCRIPTION",
                    "SERVICE_START_DT_TM",
                    "SERVICE_START_DATE_ID",
                    "SERVICE_END_DT_TM",
                    "SERVICE_END_DATE_ID",
                    "PLACE_OF_SERVICE_CODE",
                    "PLACE_OF_SERVICE_DISPLAY",
                    "PLACE_OF_SERVICE_CODING_SYSTEM_ID",
                    "PLACE_OF_SERVICE_RAW_CODING_SYSTEM_ID",
                    "PLACE_OF_SERVICE_RAW_CODE",
                    "CLAIM_UID",
                    "CLAIM_ID",
                    "UPDATE_DT_TM",
                    "UPDATE_DATE_ID",
                    "SOURCE_TYPE",
                    "SOURCE_ID",
                    "SOURCE_VERSION",
                    "SOURCE_DESCRIPTION",
                    "RANK_TYPE",
                    "POPULATION_ID",
                    "PROCEDURE_PRIMARY_DISPLAY",
                    "PLACE_OF_SERVICE_PRIMARY_DISPLAY",
                    "UPDATE_PROVIDER_ID",
                    "PRINCIPAL_PROVIDER_ID",
                    "SOURCE_TYPE_KEY",
                    "BILLING_RANK_TYPE_KEY",
                    "RAW_ENTITY_KEY",
                    "SERVICE_START_DATE",
                    "SERVICE_START_TIME_ID",
                    "SERVICE_END_DATE",
                    "SERVICE_END_TIME_ID",
                    "UPDATE_DATE",
                    "UPDATE_TIME_ID",
                    "STATUS_RAW_CODE_ID",
                    "STATUS_RAW_CODING_SYSTEM_ID",
                    "STATUS_RAW_CODE_DISPLAY",
                    "STATUS_CODE_ID",
                    "STATUS_CODING_SYSTEM_ID",
                    "STATUS_PRIMARY_DISPLAY",
                    "HASH_VALUE"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getProcedureId() {
        return super.getCell("PROCEDURE_ID");
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("EMPI_ID");
    }

    public CsvCell getProcedureRawCode() { return super.getCell("PROCEDURE_RAW_CODE");  }

    //TODO: create transform functions to extract fields


    public CsvCell getHashValue() { return super.getCell("HASH_VALUE"); }
}

