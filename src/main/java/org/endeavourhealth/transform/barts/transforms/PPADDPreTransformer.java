package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PPADD;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPADDPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPADDPreTransformer.class);

    //public static final String PPADD_ID_TO_PERSON_ID = "PPADD_ID_TO_PERSON_ID";

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
                    processRecord((PPADD)parser, fhirResourceFiler, csvHelper);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(PPADD parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //if non-active (i.e. deleted) we should REMOVE the identifier, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the identifier
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        CsvCell addressIdCell = parser.getMillenniumAddressId();
        CsvCell personIdCell = parser.getPersonId();

        PPADDPreTransformCallable callable = new PPADDPreTransformCallable(parser.getCurrentState(), addressIdCell, personIdCell, csvHelper);
        csvHelper.submitToThreadPool(callable);
    }


    static class PPADDPreTransformCallable extends AbstractCsvCallable {

        private CsvCell addressIdCell = null;
        private CsvCell personIdCell = null;
        private BartsCsvHelper csvHelper = null;

        public PPADDPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell addressIdCell,
                                         CsvCell personIdCell,
                                         BartsCsvHelper csvHelper) {
            super(parserState);

            this.addressIdCell = addressIdCell;
            this.personIdCell = personIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                //we need to store the PPADD ID -> PERSON ID mapping so that if the address is ever deleted,
                //we can find the person it belonged to, since the deleted records only give us the ID
                //wrong - we don't need this
                //csvHelper.saveInternalId(PPADD_ID_TO_PERSON_ID, addressIdCell.getString(), personIdCell.getString());

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


