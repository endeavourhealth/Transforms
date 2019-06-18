package org.endeavourhealth.transform.barts.schema;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;

import java.util.UUID;

public class ORDER extends AbstractCsvParser {

    public ORDER(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#ORDER_KEY",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "ORDER_ID",
                "ENCNTR_ID",
                "PERSON_ID",
                "ACCESSION_NBR_IDENT",
                "ORDER_DT_TM",
                "ORDER_MNEM_TXT",
                "LAST_ORDER_STATUS_CD",
                "LAST_ORDER_STATUS_DT_TM",
                "ORDERABLE_TYPE_REF",
                "PRIORITY_CD",
                "ORDER_DOC_TRAN_PRSNL_ID",
                "REQUESTED_START_DT_TM",
                "CANCELED_DT_TM",
                "CANCELED_TRAN_PRSNL_ID",
                "CANCELED_REASON_CD",
                "COMPLETED_DT_TM",
                "DISCONTINUE_DT_TM",
                "ORDER_COMMENTS_TXT",
                "CLINICAL_CATEGORY_CD",
                "ACTIVITY_TYPE_CD",
                "SCH_EVENT_ID",
                "CONCEPT_CKI_IDENT",
                "PRIM_OPCS_CONCEPT_CKI_IDENT",
                "RES_ORG_ID",
                "GP_ORG_ID"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
