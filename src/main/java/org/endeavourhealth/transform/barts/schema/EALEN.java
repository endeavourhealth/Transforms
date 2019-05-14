package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class EALEN extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(EALEN.class);

    public EALEN(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#CDS_BATCH_CONTENT_ID",
                "ACTIVE_IND",
                "EXTRACT_DT_TM",
                "PERSON_ID",
                "ENCNTR_ID",
                "EAL_ENTRY_ID",
                "RMVL_CDS_BATCH_CONTENT_ID",
                "ORIG_DECIDED_TO_ADMIT_DT",
                "DECIDED_TO_ADMIT_DT",
                "REFERRER_PRSNL_ID",
                "REFERRER_NHS_ORG_ALIAS",
                "ADMIT_BOOKING_CD",
                "WAITLIST_STATUS_CD",
                "EAL_TYPE_CD",
                "PRIORITY_TYPE_CD",
                "INTENDED_MANAGEMENT_CD",
                "LIST_TYPE_CD",
                "INTENDED_PROC_STATUS_NHS_CD_ALIAS",
                "GUARANTEED_ADMISSION_DT_TM",
                "INTNDD_TREAT_SITE_NHS_ORG_ALIAS",
                "LAST_DNA_CANCELLED_DT_TM",
                "REMOVAL_DT",
                "REASON_FOR_REMOVAL_CD",
                "LOCATION_CLASS_NHS_CD_ALIAS",
                "PRE_ADMIT_WARD_CD",
                "LAST_STATUS_REVIEW_DT_TM",
                "WL_ENTRY_LAST_REVIEW_DT_TM",
                "STATUS_REASON_CHANGE_CD",
                "STAND_BY_CD",
                "ANAESTHESIA_TYPE_CD",
                "ADJUSTED_WAIT_START_DT",
                "COMMENT_TXT",
                "ACTIVITY_LOC_TYPE_NHS_CD_ALIAS",
                "ENCNTR_CREATE_PRSNL_ID",
                "ENCNTR_UPDT_PRSNL_ID"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
