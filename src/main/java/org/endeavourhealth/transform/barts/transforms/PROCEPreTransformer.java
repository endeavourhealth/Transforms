package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PROCEPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCEPreTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {
//                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
//                        continue;
//                    }
                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((PROCE)parser, fhirResourceFiler, csvHelper);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

    }

    public static void processRecord(PROCE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell procedureIdCell = parser.getProcedureID();
        CsvCell encounterIdCell = parser.getEncounterId();


        PreTransformCallable callable = new PreTransformCallable(parser.getCurrentState(), procedureIdCell, encounterIdCell, csvHelper);
        csvHelper.submitToThreadPool(callable);
    }


    static class PreTransformCallable extends AbstractCsvCallable {

        private CsvCell procedureIdCell;
        private CsvCell encounterIdCell;
        private BartsCsvHelper csvHelper;

        public PreTransformCallable(CsvCurrentState parserState, CsvCell procedureIdCell, CsvCell encounterIdCell, BartsCsvHelper csvHelper) {
            super(parserState);
            this.procedureIdCell = procedureIdCell;
            this.encounterIdCell = encounterIdCell;
            this.csvHelper = csvHelper;
        }


        @Override
        public Object call() throws Exception {

            try {
                csvHelper.cacheNewConsultationChildRelationship(encounterIdCell, procedureIdCell, ResourceType.Procedure);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


