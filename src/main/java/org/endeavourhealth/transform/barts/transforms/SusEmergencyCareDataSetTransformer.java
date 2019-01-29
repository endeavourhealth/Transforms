package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusEmergencyCareDataSet;
import org.endeavourhealth.transform.barts.schema.SusInpatient;
import org.endeavourhealth.transform.barts.schema.SusInpatientTail;
import org.endeavourhealth.transform.common.CsvCell;
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

      //TODO - copy from inpatient transform
    }

    private static void processProcedure(SusEmergencyCareDataSet parser, CsvCell code, CsvCell date, boolean isPrimary) throws Exception {

//TODO
    }

    private static void validateRecordType(SusInpatient parser) throws Exception {
        // CDS V6-2 Type 010 - Accident and Emergency CDS
        // CDS V6-2 Type 020 - Outpatient CDS
        // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
        // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
        // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
        // CDS V6-2 Type 160 - Admitted Patient Care - Other Delivery Event CDS
        // CDS V6-2 Type 180 - Admitted Patient Care - Unfinished Birth Episode CDS
        // CDS V6-2 Type 190 - Admitted Patient Care - Unfinished General Episode CDS
        // CDS V6-2 Type 200 - Admitted Patient Care - Unfinished Delivery Episode CDS
        CsvCell recordTypeCell = parser.getCDSRecordType();
        int recordType = recordTypeCell.getInt();
        if (recordType != 10 &&
                recordType != 20 &&
                recordType != 120 &&
                recordType != 130 &&
                recordType != 140 &&
                recordType != 160 &&
                recordType != 180 &&
                recordType != 190 &&
                recordType != 200) {

            throw new TransformException("Unexpected CDS record type " + recordType);
        }
    }

}
