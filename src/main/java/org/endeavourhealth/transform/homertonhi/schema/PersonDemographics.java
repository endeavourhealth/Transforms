package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class PersonDemographics extends AbstractCsvParser {

    public PersonDemographics(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
        HomertonHiCsvToFhirTransformer.CSV_FORMAT,
        HomertonHiCsvToFhirTransformer.DATE_FORMAT,
        HomertonHiCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "empi_id",
                    "person_seq",
                    "reference_id",
                    "birth_date",
                    "birth_date_id",
                    "gender_code",
                    "gender_display",
                    "gender_coding_system_id",
                    "gender_raw_coding_system_id",
                    "gender_raw_code",
                    "marital_status_code",
                    "marital_status_display",
                    "marital_coding_system_id",
                    "marital_raw_coding_system_id",
                    "marital_raw_code",
                    "ethnicity_code",
                    "ethnicity_display",
                    "ethnicity_coding_system_id",
                    "ethnicity_raw_coding_system_id",
                    "ethnicity_raw_code",
                    "religion_code",
                    "religion_display",
                    "religion_coding_system_id",
                    "religion_raw_coding_system_id",
                    "religion_raw_code",
                    "deceased",
                    "deceased_dt_tm",
                    "deceased_date_id",
                    "cause_of_death_code",
                    "cause_of_death_display",
                    "cause_of_death_coding_system_id",
                    "cause_of_death_raw_coding_system_id",
                    "cause_of_death_raw_code",
                    "source_type",
                    "source_id",
                    "source_version",
                    "source_description",
                    "population_id",
                    "gender_primary_display",
                    "marital_status_primary_display",
                    "ethnicity_primary_display",
                    "religion_primary_display",
                    "cause_of_death_primary_display",
                    "source_type_key",
                    "raw_entity_key",
                    "supporting_fact_id",
                    "supporting_fact_type",
                    "supporting_fact_entity_id",
                    "record_data_partition_id",
                    "record_person_id",
                    "demographics_person_id",
                    "birth_sex_raw_code",
                    "birth_sex_raw_coding_system_id",
                    "birth_sex_display",
                    "birth_sex_code",
                    "birth_sex_primary_display",
                    "birth_sex_coding_system_id",
                    "birth_dt_tm",
                    "deceased_time_id",
                    "deceased_date",
                    "birth_time_id",
                    "hash_value"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPersonEmpiId() {
        CsvCell id = super.getCell("empi_id");
        String newId = "empi_id-" + id.getString();
        CsvCell ret = new CsvCell(id.getPublishedFileId(), id.getRecordNumber(), id.getColIndex(), newId, id.getParentParser());
        return ret;
    }

    public CsvCell getMaritalStatusCernerCode() {
        return super.getCell("marital_raw_code");
    }

    public CsvCell getMaritalStatusCode() {
        return super.getCell("marital_status_code");
    }

    public CsvCell getEthnicityCernerCode() {
        return super.getCell("ethnicity_raw_code");
    }

    public CsvCell getEthnicityCode() {
        return super.getCell("ethnicity_code");
    }

    public CsvCell getReligionCernerCode() {
        return super.getCell("religion_raw_code");
    }

    public CsvCell getReligionDisplay() {
        return super.getCell("religion_display");
    }

    public CsvCell getIsDeceased() { return super.getCell("deceased"); }

    public CsvCell getDeceasedDtm() { return super.getCell("deceased_dt_tm"); }

    public CsvCell getCauseOfDeathCernerCode() { return super.getCell("cause_of_death_raw_code"); }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}