package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CLEVEPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVEPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch here, since any failure here means we don't want to continue
                processRecord((CLEVE)parser, fhirResourceFiler, csvHelper);
            }
        }
    }


    public static void processRecord(CLEVE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //observations have bi-directional child-parent links. We can create the child-to-parent link when processing
        //the CLEVE file normally, but need to pre-cache the links in order to create the parent-to-child ones
        CsvCell eventIdCell = parser.getEventId();
        CsvCell parentEventIdCell = parser.getParentEventId();
        if (!BartsCsvHelper.isEmptyOrIsZero(parentEventIdCell)) {
            csvHelper.cacheParentChildClinicalEventLink(eventIdCell, parentEventIdCell);
        }
    }

}
