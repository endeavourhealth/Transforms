package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DELIV extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(DELIV.class);

    public DELIV(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#CDS_BATCH_CONTENT_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "ENCNTR_ID",
                "PERSON_ID",
                "FIRST_ANTE_ASSESS_DT_TM",
                "DELIVERY_DT",
                "ANAES_DURING_LABOUR_NHS_CD_ALIAS",
                "ANAES_POST_LABOUR_NHS_CD_ALIAS",
                "DEL_LOC_CHANGE_REASON_NHS_CD_ALIAS",
                "GEST_LABOUR_ONSET_WKS_NBR",
                "ANTENATAL_GMP_IDENT",
                "ANTENATAL_GP_PRAC_ORG_NHS",
                "LOC_CLASS_NHS_CD_ALIAS",
                "INTEND_DEL_LOC_TYPE_NHS_CD_ALIAS",
                "LABOUR_DEL_ONSET_METHD_NHS_CD_ALIAS",
                "NBR_OF_BABIES_NBR",
                "NBR_PREVIOUS_PREGNANCIES_NBR",
                "ACTIVITY_LOC_TYPE_NHS_CD_ALIAS",
                "M_OVERSEAS_VS_NHS_CD_ALIAS"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
