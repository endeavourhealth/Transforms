package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class EALOF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(EALOF.class);

    public EALOF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "SCHEDULE_ID",
                "PERSON_ID",
                "ENCNTR_ID",
                "EAL_ENTRY_ID",
                "OFFER_FOR_ADMISSION_DT",
                "ADM_OFFER_OUTCOME_NHS_CD_ALIAS",
                "SURGERY_CANCEL_REASON_TXT",
                "REFUSED_DATE_IND_TXT",
                "FIRST_OFFERED_DT",
                "SECOND_OFFERED_DT",
                "SCH_EVENT_ID",
                "TCI_LOC_TXT",
                "APPT_LOCATION_ID"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
