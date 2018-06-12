package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.PROVIDER;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PROVIDERTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PROVIDERTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(PROVIDER.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((PROVIDER) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(PROVIDER parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        //get Patient Resource builder from cache
        PatientBuilder patientBuilder = new PatientBuilder();

        CsvCell patientId = parser.getPatientId();
        CsvCell caseId = parser.getCaseId();


    }
}
