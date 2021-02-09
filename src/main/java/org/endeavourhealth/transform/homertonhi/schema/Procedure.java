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
                    "procedure_id",
                    "reference_id",
                    "empi_id",
                    "encounter_id",
                    "procedure_code",
                    "procedure_display",
                    "procedure_coding_system_id",
                    "procedure_raw_coding_system_id",
                    "procedure_raw_code",
                    "description",
                    "service_start_dt_tm",
                    "service_start_date_id",
                    "service_end_dt_tm",
                    "service_end_date_id",
                    "place_of_service_code",
                    "place_of_service_display",
                    "place_of_service_coding_system_id",
                    "place_of_service_raw_coding_system_id",
                    "place_of_service_raw_code",
                    "claim_uid",
                    "claim_id",
                    "update_dt_tm",
                    "update_date_id",
                    "source_type",
                    "source_id",
                    "source_version",
                    "source_description",
                    "rank_type",
                    "population_id",
                    "procedure_primary_display",
                    "place_of_service_primary_display",
                    "update_provider_id",
                    "principal_provider_id",
                    "source_type_key",
                    "billing_rank_type_key",
                    "raw_entity_key",
                    "service_start_date",
                    "service_start_time_id",
                    "service_end_date",
                    "service_end_time_id",
                    "update_date",
                    "update_time_id",
                    "status_raw_code_id",
                    "status_raw_coding_system_id",
                    "status_raw_code_display",
                    "status_code_id",
                    "status_coding_system_id",
                    "status_primary_display",
                    "hash_value"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getProcedureId() {
        return super.getCell("procedure_id");
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("empi_id");
    }

    public CsvCell getProcedureRawCode() { return super.getCell("procedure_raw_code");  }

    public CsvCell getProcedureDisplayTerm() { return super.getCell("procedure_display");  }

    public CsvCell getProcedureCodingSystem() { return super.getCell("procedure_coding_system_id"); }

    public CsvCell getProcedureDescription() { return super.getCell("description");  }

    public CsvCell getProcedureStartDate() { return super.getCell("service_start_dt_tm");  }

    public CsvCell getProcedureEndDate() { return super.getCell("service_end_dt_tm");  }

    public CsvCell getEncounterId() { return super.getCell("encounter_id");  }

    public CsvCell getRankType() { return super.getCell("rank_type");  }

    public CsvCell getSourceDescription() { return super.getCell("source_description"); }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}

