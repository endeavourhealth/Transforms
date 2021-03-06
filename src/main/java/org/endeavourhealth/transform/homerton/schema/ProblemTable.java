package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;

import java.util.UUID;

public class ProblemTable extends AbstractCsvParser {

    public ProblemTable(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, HomertonCsvToFhirTransformer.CSV_FORMAT, HomertonCsvToFhirTransformer.DATE_FORMAT, HomertonCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "PROBLEM_INSTANCE_ID",
                    "PROBLEM_ID",
                    "NOMENCLATURE_ID",
                    "PROBLEM_FTDESC",
                    "PERSON_ID",
                    "ESTIMATED_RESOLUTION_DT_TM",
                    "ACTUAL_RESOLUTION_DT_TM",
                    "CLASSIFICATION_CD",
                    "PERSISTENCE_CD",
                    "CONFIRMATION_STATUS_CD",
                    "LIFE_CYCLE_STATUS_CD",
                    "LIFE_CYCLE_DT_TM",
                    "ONSET_DT_CD",
                    "ONSET_DT_TM",
                    "RANKING_CD",
                    "CERTAINTY_CD",
                    "PROBABILITY",
                    "PERSON_AWARE_CD",
                    "PROGNOSIS_CD",
                    "PERSON_AWARE_PROGNOSIS_CD",
                    "FAMILY_AWARE_CD",
                    "SENSITIVITY",
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
                    "COURSE_CD",
                    "CANCEL_REASON_CD",
                    "ONSET_DT_FLAG",
                    "STATUS_UPDT_PRECISION_CD",
                    "STATUS_UPDT_FLAG",
                    "STATUS_UPDT_DT_TM",
                    "QUALIFIER_CD",
                    "ANNOTATED_DISPLAY",
                    "SEVERITY_CLASS_CD",
                    "SEVERITY_CD",
                    "SEVERITY_FTDESC",
                    "ONSET_TZ",
                    "BEG_EFFECTIVE_TZ",
                    "LIFE_CYCLE_TZ",
                    "DEL_IND",
                    "ORGANIZATION_ID",
                    "PROBLEM_UUID",
                    "PROBLEM_INSTANCE_UUID",
                    "LIFE_CYCLE_DT_FLAG",
                    "LIFE_CYCLE_DT_CD",
                    "COND_TYPE_FLAG",
                    "PROBLEM_TYPE_FLAG",
                    "SHOW_IN_PM_HISTORY_IND",
                    "LATERALITY_CD",
                    "ORIGINATING_NOMENCLATURE_ID",
                    "LAST_UPDATED",
                    "IMPORT_ID",
                    "HASH",
                    "life_cycle_status",
                    "cancel_reason",
                    "prognosis",
                    "classification",
                    "ranking",
                    "certainty",
                    "persistence",
                    "severity_class",
                    "laterality",
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

    public CsvCell getProblemId() {
        return super.getCell("PROBLEM_ID");
    }
    public CsvCell getPersonId() {
        return super.getCell("PERSON_ID");
    }
    public CsvCell getOnsetDate() {
        return super.getCell("ONSET_DT_TM");
    }
    public CsvCell getStatusLifecycle() {
        return super.getCell("life_cycle_status");
    }
    public CsvCell getProblemDescriptionText() {
        return super.getCell("nomenclature_description");
    }
    public CsvCell getProblemAnnotatedDisplay() {
        return super.getCell("ANNOTATED_DISPLAY");
    }
    public CsvCell getProblemCode() { return super.getCell("source_identifier"); }
    public CsvCell getVocabulary() {
        return super.getCell("source_vocabulary");
    }
    public CsvCell getLifeCycleDateTime() {
        return super.getCell("LIFE_CYCLE_DT_TM");
    }


}
