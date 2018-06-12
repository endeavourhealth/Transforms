package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.ENCINF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

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
                createEncounter((ENCINF)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
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
                    Date d = BartsCsvHelper.parseDate(beginEffectiveCell);
                    encounterBuilder.setPeriodStart(d, beginEffectiveCell);
                }
                if (endEffectiveCell != null && endEffectiveCell.getString().length() > 0) {
                    Date d = BartsCsvHelper.parseDate(endEffectiveCell);
                    encounterBuilder.setPeriodEnd(d, endEffectiveCell);
                }
            }
        }

    }

}
