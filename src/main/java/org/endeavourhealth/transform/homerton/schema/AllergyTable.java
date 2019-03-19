package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;

import java.util.UUID;

public class AllergyTable extends AbstractCsvParser {

    public AllergyTable(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, HomertonCsvToFhirTransformer.CSV_FORMAT,
                HomertonCsvToFhirTransformer.DATE_FORMAT, HomertonCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        return new String[]{
                "ALLERGY_INSTANCE_ID",
                "ALLERGY_ID",
                "PERSON_ID",
                "ENCNTR_ID",
                "SUBSTANCE_NOM_ID",
                "SUBSTANCE_FTDESC",
                "SUBSTANCE_TYPE_CD",
                "REACTION_CLASS_CD",
                "SEVERITY_CD",
                "SOURCE_OF_INFO_CD",
                "SOURCE_OF_INFO_FT",
                "ONSET_DT_TM",
                "REACTION_STATUS_CD",
                "CREATED_DT_TM",
                "CREATED_PRSNL_ID",
                "CANCEL_REASON_CD",
                "CANCEL_DT_TM",
                "CANCEL_PRSNL_ID",
                "ACTIVE_IND",
                "ACTIVE_STATUS_CD",
                "ACTIVE_STATUS_DT_TM",
                "ACTIVE_STATUS_PRSNL_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "CONTRIBUTOR_SYSTEM_CD",
                "DATA_STATUS_CD",
                "DATA_STATUS_DT_TM",
                "DATA_STATUS_PRSNL_ID",
                "UPDT_APPLCTX",
                "UPDT_CNT",
                "UPDT_DT_TM",
                "UPDT_ID",
                "UPDT_TASK",
                "VERIFIED_STATUS_FLAG",
                "REC_SRC_VOCAB_CD",
                "REC_SRC_IDENTIFER",
                "REC_SRC_STRING",
                "REACTION_STATUS_DT_TM",
                "ONSET_PRECISION_CD",
                "ONSET_PRECISION_FLAG",
                "REVIEWED_DT_TM",
                "REVIEWED_PRSNL_ID",
                "ORIG_PRSNL_ID",
                "ONSET_TZ",
                "BEG_EFFECTIVE_TZ",
                "REVIEWED_TZ",
                "CMB_INSTANCE_ID",
                "CMB_PRSNL_ID",
                "CMB_DT_TM",
                "ORGANIZATION_ID",
                "CMB_TZ",
                "CMB_PERSON_ID",
                "CMB_FLAG",
                "SUB_CONCEPT_CKI",
                "LAST_UPDATED",
                "IMPORT_ID",
                "HASH",
                "substance_type",
                "reaction_class",
                "severity",
                "source_of_info",
                "reaction_status",
                "source_identifier",
                "source_vocabulary_cd",
                "source_vocabulary",
                "nom_ver_grp_id",
                "primary_vterm_ind",
                "nomenclature_description",
                "principle_type_cd",
                "principle_type"
        };

    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getAllergyId() {
        return super.getCell("ALLERGY_ID");
    }

    public CsvCell getPersonId() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getRecordedDate() {
        return super.getCell("CREATED_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getRecordedByClinicianID() {
        return super.getCell("CREATED_PRSNL_ID");
    }

    public CsvCell getOriginalClinicianID() {
        return super.getCell("ORIG_PRSNL_ID");
    }

    public CsvCell getVocabulary() {
        return super.getCell("source_vocabulary");
    }

    public CsvCell getAllergyCode() {
        return super.getCell("source_identifier");
    }

    public CsvCell getAllergyDescriptionText() {
        return super.getCell("nomenclature_description");
    }

    public CsvCell getAllergyDate() {
        return super.getCell("REACTION_STATUS_DT_TM");
    }

    public CsvCell getReactionStatus() {
        return super.getCell("reaction_status");
    }

    public CsvCell getAllergySeverity() {
        return super.getCell("severity");
    }
}
