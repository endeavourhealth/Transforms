package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class OPREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(OPREF.class);

    public OPREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#FIN_NBR_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "ENCNTR_ID",
                "WAIT_LIST_ID",
                "UBRN_IDENT",
                "OP_REF_IND",
                "REF_RCVD_DT",
                "REF_WRITTEN_DT",
                "REFERRAL_ID",
                "REFERRER_NHS_ORG_ALIAS",
                "WAIT_LIST_TYPE_CD",
                "TRG_WAIT_GROUP_CD",
                "BOOKING_TYPE_CD",
                "REF_SOURCE_CD",
                "PRIORITY_TYPE_CD",
                "SERVICE_TYPE_REQ_CD",
                "URG_CANCER_REF_CD",
                "REF_REMOVAL_DT_TM",
                "WAITING_END_DT_TM",
                "REMOVAL_REASON_CD",
                "CHANGE_REASON_CD",
                "REASON_FOR_VISIT_TXT",
                "RESP_HCP_PRSNL_ID",
                "TREAT_FUNCTION_CD",
                "MAIN_SPECIALTY_CD",
                "PERSON_ID",
                "GUARANTEED_APPT_DT",
                "WAIT_LIST_STATUS_CD",
                "ADJ_WAIT_START_DT",
                "ENCNTR_CREATE_PRSNL_ID",
                "PAS_REFERRAL_ID"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
