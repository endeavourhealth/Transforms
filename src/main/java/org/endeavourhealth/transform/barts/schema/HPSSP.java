package org.endeavourhealth.transform.barts.schema;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class HPSSP extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(HPSSP.class);

    public HPSSP(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {

        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#HOSP_PRVDR_IDENT",
                "ACTIVE_IND",
                "EXTRACT_DT_TM",
                "ENCNTR_ID",
                "PERSON_ID",
                "PAT_CLASS_NHS_CD_ALIAS",
                "PRE_REG_DT_TM",
                "ADMISSION_DT_TM",
                "ADMISSION_METHOD_CD",
                "SOURCE_OF_ADMISSION_CD",
                "LEGAL_STATUS_NHS_CD_ALIAS",
                "EST_DISCH_DT",
                "PENDING_EST_DISCH_DT_TM",
                "FIT_FOR_DISCH_DT_TM",
                "DISCH_READY_DT_TM",
                "DISCH_SUM_DT_TM",
                "DISCH_DISP_CD",
                "DISCH_DT_TM",
                "REFERRER_PRSNL_ID",
                "REFERRER_ORG_ID",
                "DELAY_DISCH_REASON_CD",
                "DISCH_DEST_CD",
                "REFERRER_NHS_ORG_ALIAS",
                "ENCNTR_CREATE_PRSNL_ID",
                "ENCNTR_UPDT_PRSNL_ID"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
