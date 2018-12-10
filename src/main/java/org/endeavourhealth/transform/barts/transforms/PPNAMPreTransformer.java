package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PPNAM;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPNAMPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMPreTransformer.class);

    public static final String PPNAM_ID_TO_PERSON_ID = "PPNAM_ID_TO_PERSON_ID";

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
                    processRecord((PPNAM)parser, fhirResourceFiler, csvHelper);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(PPNAM parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //all this pre-transformer does is quickly save the name ID -> person ID mappings, so we can use that mapping
        //if the PPNAM record is ever deleted (in which case we get the name ID but not the person ID)
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        CsvCell nameIdCell = parser.getMillenniumPersonNameId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        PPNAMPreTransformCallable callable = new PPNAMPreTransformCallable(parser.getCurrentState(), nameIdCell, personIdCell, csvHelper);
        csvHelper.submitToThreadPool(callable);
    }


    static class PPNAMPreTransformCallable extends AbstractCsvCallable {

        private CsvCell nameIdCell = null;
        private CsvCell personIdCell = null;
        private BartsCsvHelper csvHelper = null;

        public PPNAMPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell nameIdCell,
                                         CsvCell personIdCell,
                                         BartsCsvHelper csvHelper) {

            super(parserState);
            this.nameIdCell = nameIdCell;
            this.personIdCell = personIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                //we need to store the PPNAM ID -> PERSON ID mapping so that if the address is ever deleted,
                //we can find the person it belonged to, since the deleted records only give us the ID
                csvHelper.saveInternalId(PPNAM_ID_TO_PERSON_ID, nameIdCell.getString(), personIdCell.getString());

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

    }
}


