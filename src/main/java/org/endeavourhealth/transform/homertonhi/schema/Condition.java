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
                    "condition_id",
                    "reference_id",
                    "data_source_condition_id",
                    "empi_id",
                    "encounter_id",
                    "condition_code",
                    "condition_display",
                    "condition_coding_system_id",
                    "condition_raw_coding_system_id",
                    "condition_raw_code",
                    "description",
                    "effective_dt_tm",
                    "effective_date_id",
                    "condition_type_code",
                    "condition_type_display",
                    "condition_type_coding_system_id",
                    "condition_type_raw_coding_system_id",
                    "condition_type_raw_code",
                    "classification_code",
                    "classification_display",
                    "classification_coding_system_id",
                    "classification_raw_coding_system_id",
                    "classification_raw_code",
                    "confirmation_status_code",
                    "confirmation_status_display",
                    "confirmation_status_coding_system_id",
                    "confirmation_status_raw_coding_system_id",
                    "confirmation_status_raw_code",
                    "status_code",
                    "status_display",
                    "status_coding_system_id",
                    "status_raw_coding_system_id",
                    "status_raw_code",
                    "status_dt_tm",
                    "status_date_id",
                    "update_dt_tm",
                    "update_date_id",
                    "present_on_admission_code",
                    "present_on_admission_display",
                    "present_on_admission_coding_system_id",
                    "present_on_admission_raw_coding_system_id",
                    "present_on_admission_raw_code",
                    "present_on_admission_primary_display",
                    "source_type",
                    "source_id",
                    "source_version",
                    "source_description",
                    "rank_type",
                    "population_id",
                    "condition_primary_display",
                    "condition_type_primary_display",
                    "classification_primary_display",
                    "confirmation_status_primary_display",
                    "status_primary_display",
                    "responsible_provider_id",
                    "update_provider_id",
                    "claim_id",
                    "source_type_key",
                    "billing_rank_type_key",
                    "supporting_fact_id",
                    "supporting_fact_type",
                    "supporting_fact_entity_id",
                    "raw_entity_key",
                    "effective_date",
                    "effective_time_id",
                    "update_date",
                    "update_time_id",
                    "asserted_dt_tm",
                    "asserted_date_id",
                    "recorder_provider_id",
                    "status_time_id",
                    "status_date",
                    "asserted_time_id",
                    "asserted_date",
                    "hash_value"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getConditionId() {
        return super.getCell("condition_id");
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("empi_id");
    }

    public CsvCell getConditionTypeCode() { return super.getCell("condition_type_code"); }

    public CsvCell getConditionRawCode() { return super.getCell("condition_raw_code"); }

    public CsvCell getConditionCodingSystemId() { return super.getCell("condition_coding_system_id"); }

    public CsvCell getConditionConfirmationStatusDisplay() { return super.getCell("confirmation_status_display"); }

    public CsvCell getConditionDisplay() { return super.getCell("condition_display"); }

    public CsvCell getProblemStatusDisplay() { return super.getCell("status_display"); }

    public CsvCell getProblemStatusDtm() { return super.getCell("status_dt_tm"); }

    public CsvCell getConditionDescription() { return super.getCell("description"); }

    public CsvCell getEncounterId() { return super.getCell("encounter_id");  }

    public CsvCell getEffectiveDtm() { return super.getCell("effective_dt_tm"); }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}