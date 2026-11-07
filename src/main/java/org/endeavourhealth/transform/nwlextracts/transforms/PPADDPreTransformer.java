package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPADD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPADDPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPADDPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }

                    //no try/catch as failures here meant we should abort
                    processRecord((PPADD)parser, fhirResourceFiler, csvHelper,sourceSystem);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(PPADD parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        //if non-active (i.e. deleted) we should REMOVE the identifier, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the identifier
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        CsvCell addressIdCell = parser.getMillenniumAddressId();
        CsvCell personIdCell = parser.getPersonId();

        PPADDPreTransformCallable callable = new PPADDPreTransformCallable(parser.getCurrentState(), addressIdCell, personIdCell, csvHelper, sourceSystem);
        csvHelper.submitToThreadPool(callable);
    }


    static class PPADDPreTransformCallable extends AbstractCsvCallable {

        private CsvCell addressIdCell = null;
        private CsvCell personIdCell = null;
        private NwlExtractsCsvHelper csvHelper = null;
        private String sourceSystem = null;

        public PPADDPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell addressIdCell,
                                         CsvCell personIdCell,
                                         NwlExtractsCsvHelper csvHelper, String sourceSystem) {
            super(parserState);

            this.addressIdCell = addressIdCell;
            this.personIdCell = personIdCell;
            this.csvHelper = csvHelper;
            this.sourceSystem=sourceSystem;
        }

        @Override
        public Object call() throws Exception {

            try {
                //pre-cache the patient resource
                csvHelper.getPatientCache().preCachePatientBuilder(personIdCell,sourceSystem);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


