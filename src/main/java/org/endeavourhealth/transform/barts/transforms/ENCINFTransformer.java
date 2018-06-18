package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.ENCINF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ENCINFTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCINFTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createEncounter((ENCINF)parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createEncounter(ENCINF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell beginEffectiveCell = parser.getBeginEffectiveDateTime();
        CsvCell endEffectiveCell = parser.getEndEffectiveDateTime();

        //TODO - look at the various code fields to work out what this record is telling us

        /*EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(encounterIdCell, per csvHelper, encounterIdCell.getString());



        if (encounterBuilder != null && encounterBuilder.getPeriod() == null) {
            if (beginEffectiveCell != null && beginEffectiveCell.getString().length() > 0) {
                Date d = BartsCsvHelper.parseDate(beginEffectiveCell);
                encounterBuilder.setPeriodStart(d, beginEffectiveCell);
            }
            if (endEffectiveCell != null && endEffectiveCell.getString().length() > 0) {
                Date d = BartsCsvHelper.parseDate(endEffectiveCell);
                encounterBuilder.setPeriodEnd(d, endEffectiveCell);
            }
        }*/

    }

}
