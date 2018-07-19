package org.endeavourhealth.transform.barts.transformsOld;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.ENCINF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.hl7.fhir.instance.model.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createEncounter(ENCINF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell beginEffectiveCell = parser.getBeginEffectiveDateTime();
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(beginEffectiveCell)) {

            EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, null, activeCell, csvHelper);
            if (encounterBuilder == null) {
                return;
            }

            Date d = BartsCsvHelper.parseDate(beginEffectiveCell);

            //if the encounter doesn't have any date set on it, use the ENCINF date to set something
            Period period = encounterBuilder.getPeriod();
            if (period == null
                    || !period.hasStart()) {
                //if the encounter doesn't have a start date, use our date
                encounterBuilder.setPeriodStart(d, beginEffectiveCell);
            } else {
                //if the encounter has a start date but our ENCINF date is before it, then apply it
                Date encounterStart = period.getStart();
                if (d.before(encounterStart)) {
                    encounterBuilder.setPeriodStart(d, beginEffectiveCell);
                }
            }

            //we don't save immediately, but return the Encounter builder to the cache
            csvHelper.getEncounterCache().returnEncounterBuilder(encounterIdCell, encounterBuilder);
        }
    }

}
