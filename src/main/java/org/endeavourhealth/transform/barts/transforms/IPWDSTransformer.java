package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.IPWDS;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPWDSTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(IPWDSTransformer.class);


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
                createEpisodeEventWardStay((IPWDS)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createEpisodeEventWardStay(IPWDS parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        CsvCell wardStayId = parser.getCDSWardStayId();
        CsvCell patientId = parser.getPatientId();
        CsvCell episodeEventId = parser.getCDSBatchContentEventId();

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            return;
        }

        // get the associated encounter
        CsvCell encounterIdCell = parser.getEncounterId();
        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(csvHelper, encounterIdCell.getString());

        // update the encounter
        //TODO: additional data here

        // save / cache the resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }
}
