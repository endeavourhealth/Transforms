package org.endeavourhealth.transform.emis.csv.transforms.coding;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.coding.DrugCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class DrugCodeTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DrugCodeTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //inserting the entries into the IdCodeMap table is a lot slower than the rest of this
        //file, so split up the saving over a few threads
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 50000);

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(DrugCode.class);
            while (parser.nextRecord()) {

                try {
                    transform((DrugCode)parser, fhirResourceFiler, csvHelper, threadPool);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }
    }

    private static void transform(DrugCode parser,
                                  FhirResourceFiler fhirResourceFiler,
                                  EmisCsvHelper csvHelper,
                                  ThreadPool threadPool) throws Exception {

        CsvCell codeId = parser.getCodeId();
        CsvCell term = parser.getTerm();
        CsvCell dmdId = parser.getDmdProductCodeId();

        List<ThreadPoolError> errors = threadPool.submit(new DrugSaveCallable(parser.getCurrentState(), csvHelper, codeId, dmdId, term));
        handleErrors(errors);
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        DrugSaveCallable callable = (DrugSaveCallable)first.getCallable();
        Throwable exception = first.getException();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    static class DrugSaveCallable implements Callable {

        private CsvCurrentState parserState = null;
        private EmisCsvHelper csvHelper = null;
        private CsvCell codeId = null;
        private CsvCell dmdId = null;
        private CsvCell term = null;

        public DrugSaveCallable(CsvCurrentState parserState,
                                EmisCsvHelper csvHelper,
                                CsvCell codeId,
                                CsvCell dmdId,
                                CsvCell term) {

            this.parserState = parserState;
            this.csvHelper = csvHelper;
            this.codeId = codeId;
            this.dmdId = dmdId;
            this.term = term;
        }

        @Override
        public Object call() throws Exception {

            try {
                //we need to generate the audit of the source cells to FHIR so we can apply it when we create resources
                ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

                //audit where the code came from, if we have one
                if (!dmdId.isEmpty()) {
                    auditWrapper.auditValue(dmdId.getRowAuditId(), dmdId.getColIndex(), EmisCodeHelper.AUDIT_DRUG_CODE);
                }

                //audit where the term came from
                auditWrapper.auditValue(term.getRowAuditId(), term.getColIndex(), EmisCodeHelper.AUDIT_DRUG_TERM);

                EmisCsvCodeMap mapping = new EmisCsvCodeMap();
                mapping.setMedication(true);
                mapping.setCodeId(codeId.getLong());
                mapping.setSnomedConceptId(dmdId.getLong());
                mapping.setSnomedTerm(term.getString());
                mapping.setAudit(auditWrapper);

                csvHelper.saveClinicalOrDrugCode(mapping);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }
    }
}
