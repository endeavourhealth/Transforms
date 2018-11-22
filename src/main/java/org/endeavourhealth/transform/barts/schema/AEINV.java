package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class AEINV extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(AEINV.class);

    public AEINV(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#AE_INVEST_KEY",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "CDS_BATCH_CONTENT_ID",
                "INVEST_SEQ_NBR",
                "INVEST_NHS_CD_ALIAS",
                "INVEST_SCHEME_NHS_CD_ALIAS"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
