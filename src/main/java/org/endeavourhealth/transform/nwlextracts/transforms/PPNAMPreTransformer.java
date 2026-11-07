package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPNAM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPNAMPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPNAMPreTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper,String sourceSystem) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }

                    //no try/catch as failures here meant we should abort
                    processRecord((PPNAM)parser, fhirResourceFiler, csvHelper,sourceSystem);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(PPNAM parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        //all this pre-transformer does is quickly save the name ID -> person ID mappings, so we can use that mapping
        //if the PPNAM record is ever deleted (in which case we get the name ID but not the person ID)
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        CsvCell nameIdCell = parser.getMillenniumPersonNameId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        PPNAMPreTransformCallable callable = new PPNAMPreTransformCallable(parser.getCurrentState(), nameIdCell, personIdCell, csvHelper, sourceSystem);
        csvHelper.submitToThreadPool(callable);
    }


    static class PPNAMPreTransformCallable extends AbstractCsvCallable {

        private CsvCell nameIdCell = null;
        private CsvCell personIdCell = null;
        private NwlExtractsCsvHelper csvHelper = null;
        private String  sourceSystem = null;

        public PPNAMPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell nameIdCell,
                                         CsvCell personIdCell,
                                         NwlExtractsCsvHelper csvHelper, String sourceSystem) {

            super(parserState);
            this.nameIdCell = nameIdCell;
            this.personIdCell = personIdCell;
            this.csvHelper = csvHelper;
            this.sourceSystem = sourceSystem;
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


