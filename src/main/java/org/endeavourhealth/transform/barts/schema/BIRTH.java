package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class BIRTH extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(BIRTH.class);

    public BIRTH(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#CDS_BATCH_CONTENT_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "ENCNTR_ID",
                "PERSON_ID",
                "DEL_CDS_BACTH_CONTENT_ID",
                "ACTUAL_DEL_LOC_CLS_NHS_CD_ALIAS",
                "ACTUAL_DEL_PLACE_TYPE_NHS_CD_ALIAS",
                "BIRTH_NBR",
                "BIRTH_ORDER_NBR",
                "BIRTH_WEIGHT_NBR",
                "DELIVERY_METHOD_NHS_CD_ALIAS",
                "GEST_ASSESS_NBR",
                "LIVE_STILL_BIRTH_IND_NHS_CD_ALIAS",
                "RESUS_METHOD_NHS_CD_ALIAS",
                "DEL_PRSNL_STATUS_NHS_CD_ALIAS",
                "SUSPECT_ABNORM_IND_CD",
                "MIDWIFE_PRSNL_ID",
                "ACTIVITY_LOC_TYPE_NHS_CD_ALIAS",
                "BABY_OVERSEAS_VS_NHS_CD_ALIAS"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}
