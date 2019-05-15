package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPATH extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(PPATH.class);

    public PPATH(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#EPISODE_ID",
                "ACTIVE_IND",
                "EXTRACT_DT_TM",
                "PERSON_ID",
                "PAT_PATHWAY_IDENT",
                "PAT_PATHWAY_ISSUER_NHS_ORG_ALIAS"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
