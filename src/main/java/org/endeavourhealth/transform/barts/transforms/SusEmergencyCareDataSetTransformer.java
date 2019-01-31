package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusEmergencyCareDataSet;
import org.endeavourhealth.transform.barts.schema.SusInpatientTail;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SusEmergencyCareDataSetTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusInpatientTransformer.class);


    public static void transformProcedures(List<ParserI> parsers,
                                           FhirResourceFiler fhirResourceFiler,
                                           BartsCsvHelper csvHelper,
                                           Map<String, List<ParserI>> parserMap) throws Exception {

        for (ParserI parser: parsers) {

            //parse corresponding tails file first
            Map<String, SusTailCacheEntry> tailsCache = processTails(parser, parserMap);

            while (parser.nextRecord()) {
                try {
                    processRecordProcedures((SusEmergencyCareDataSet)parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static Map<String, SusTailCacheEntry> processTails(ParserI parser, Map<String, List<ParserI>> parserMap) throws Exception {
        SusInpatientTail tailParser = (SusInpatientTail)BartsCsvToFhirTransformer.findTailFile(parserMap, "SusEmergencyCareDataSetTail", parser.getFilePath());
        Map<String, SusTailCacheEntry> tailsCache = new HashMap<>();
        SusInpatientTailPreTransformer.transform(tailParser, tailsCache);
        return tailsCache;
    }

    private static void processRecordProcedures(SusEmergencyCareDataSet parser, BartsCsvHelper csvHelper) throws Exception {

        //not doing anything with this file yet
    }

}
