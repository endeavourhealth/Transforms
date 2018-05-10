package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.ENCINF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ENCINFTransformer extends BartsBasisTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ENCINFTransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry((ENCINF)parser);
                if (valStr == null) {
                    createEncounter((ENCINF)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(ENCINF parser) {
        return null;
    }


    /*
     *
     */
    public static void createEncounter(ENCINF parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell beginEffectiveCell = parser.getBeginEffectiveDateTime();
        CsvCell endEffectiveCell = parser.getEndEffectiveDateTime();

        if (activeCell != null && activeCell.getBoolean() == true) {
            EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(csvHelper, encounterIdCell.getString());

            if (encounterBuilder != null && encounterBuilder.getPeriod() == null) {
                if (beginEffectiveCell != null && beginEffectiveCell.getString().length() > 0) {
                    encounterBuilder.setPeriodStart(beginEffectiveCell.getDate(), beginEffectiveCell);
                }
                if (endEffectiveCell != null && endEffectiveCell.getString().length() > 0) {
                    encounterBuilder.setPeriodEnd(endEffectiveCell.getDate(), endEffectiveCell);
                }
            }
        }

    }

}
