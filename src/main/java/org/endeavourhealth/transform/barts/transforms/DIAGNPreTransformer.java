package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.DIAGN;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DIAGNPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGNPreTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {
                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }
                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((DIAGN)parser, fhirResourceFiler, csvHelper);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(DIAGN parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell diagnosisIdCell = parser.getDiagnosisID();
        CsvCell encounterIdCell = parser.getEncounterId();

        PreTransformCallable callable = new PreTransformCallable(parser.getCurrentState(), diagnosisIdCell, encounterIdCell, csvHelper);
        csvHelper.submitToThreadPool(callable);
    }


    static class PreTransformCallable extends AbstractCsvCallable {

        private CsvCell diagnosisIdCell;
        private CsvCell encounterIdCell;
        private BartsCsvHelper csvHelper;

        public PreTransformCallable(CsvCurrentState parserState, CsvCell diagnosisIdCell, CsvCell encounterIdCell, BartsCsvHelper csvHelper) {
            super(parserState);
            this.diagnosisIdCell = diagnosisIdCell;
            this.encounterIdCell = encounterIdCell;
            this.csvHelper = csvHelper;
        }


        @Override
        public Object call() throws Exception {

            try {
                csvHelper.cacheNewConsultationChildRelationship(encounterIdCell, diagnosisIdCell, ResourceType.Condition);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

}
