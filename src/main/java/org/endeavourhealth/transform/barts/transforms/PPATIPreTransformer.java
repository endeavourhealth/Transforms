package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.barts.schema.PPREL;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPATIPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATIPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }

                    //no try/catch as failures here meant we should abort
                    processRecord((PPATI)parser, fhirResourceFiler, csvHelper);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    private static void processRecord(PPATI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //all this pre-transformer does is to pre-load any FHIR patients that we know we might want
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonId();

        PPRELPreTransformCallable callable = new PPRELPreTransformCallable(parser.getCurrentState(), personIdCell, csvHelper);
        csvHelper.submitToThreadPool(callable);
    }


    static class PPRELPreTransformCallable extends AbstractCsvCallable {

        private CsvCell personIdCell = null;
        private BartsCsvHelper csvHelper = null;

        public PPRELPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell personIdCell,
                                         BartsCsvHelper csvHelper) {

            super(parserState);
            this.personIdCell = personIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {
                //pre-cache the patient resource
                csvHelper.getPatientCache().preCachePatientBuilder(personIdCell);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


