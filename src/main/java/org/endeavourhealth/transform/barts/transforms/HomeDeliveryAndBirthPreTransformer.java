package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingHomeDelBirthCds;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.HomeDeliveryAndBirth;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HomeDeliveryAndBirthPreTransformer extends CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(HomeDeliveryAndBirthPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingHomeDelBirthCds> homeDelBirthCdsBatch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue

                //new function to call into emergency attendances
                processHomeDelBirthCdsRecords((HomeDeliveryAndBirth)parser, csvHelper, homeDelBirthCdsBatch);
            }
        }

        saveHomeDelBirthCdsBatch(homeDelBirthCdsBatch, true, csvHelper);
    }
}
