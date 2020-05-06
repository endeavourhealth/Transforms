package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
import org.endeavourhealth.transform.bhrut.schema.PMI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EpisodesTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EpisodesTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Episodes.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResources((Episodes) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResources(Episodes parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getExternalId().toString());

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, encounterBuilder);
    }
}
