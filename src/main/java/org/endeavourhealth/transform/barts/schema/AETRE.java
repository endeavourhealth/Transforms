package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class AETRE extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(AETRE.class);

    public AETRE(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#AE_TREAT_KEY",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "CDS_BATCH_CONTENT_ID",
                "TREAT_SEQ_NBR",
                "TREAT_NHS_CD_ALIAS",
                "TREAT_DT_TM",
                "TREAT_SCHEME_NHS_CD_ALIAS"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
