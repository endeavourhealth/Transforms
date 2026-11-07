package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPATI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PPATIPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATIPreTransformer.class);

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
                    processRecord((PPATI)parser,csvHelper,sourceSystem);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    private static void processRecord(PPATI parser, NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        //all this pre-transformer does is to pre-load any FHIR patients that we know we might want
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }
        CsvCell personIdCell = parser.getMillenniumPersonId();
        CsvCell mrnCell = parser.getLocalPatientId();

        List<CsvCell> patientIdentifiers = new ArrayList<CsvCell>();
        patientIdentifiers.add(personIdCell);
        if(!mrnCell.getString().isEmpty()){
            patientIdentifiers.add(mrnCell);
        }

        PPRELPreTransformCallable callable = new PPRELPreTransformCallable(parser.getCurrentState(), patientIdentifiers, csvHelper,sourceSystem);
        csvHelper.submitToThreadPool(callable);
    }


    static class PPRELPreTransformCallable extends AbstractCsvCallable {

        private List<CsvCell> patientIdentifiers = null;
        private NwlExtractsCsvHelper csvHelper = null;

        private String sourceSystem = null;

        public PPRELPreTransformCallable(CsvCurrentState parserState,
                                         List<CsvCell> patientIdentifiers,
                                         NwlExtractsCsvHelper csvHelper, String sourceSystem) {

            super(parserState);
            this.patientIdentifiers = patientIdentifiers;
            this.csvHelper = csvHelper;
            this.sourceSystem=sourceSystem;
        }

        @Override
        public Object call() throws Exception {

            try {
                //pre-cache the patient resource
                csvHelper.getPatientCache().preCachePatientBuilder(patientIdentifiers,sourceSystem);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


