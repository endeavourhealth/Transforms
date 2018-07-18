package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3LookupDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRCtv3Transformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3Transformer.class);

    private static TppCtv3LookupDalI repository = DalProvider.factoryTppCtv3LookupDal();
    public static final String ROW_ID = "RowId";
    public static final String CTV3_CODE = "ctv3Code";
    public static final String CTV3_TEXT = "ctv3Text";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {


        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            AbstractCsvParser parser = parsers.get(SRCtv3.class);
            while (parser.nextRecord()) {

                try {
                    createResource((SRCtv3) parser, threadPool);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        Throwable exception = first.getException();
        SRCtv3Transformer.WebServiceLookup callable = (SRCtv3Transformer.WebServiceLookup)first.getCallable();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    public static void createResource(SRCtv3 parser, ThreadPool threadPool) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        if (rowId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}",
                    rowId.getString(), parser.getFilePath());
            return;
        }
        CsvCell ctv3Code = parser.getCtv3Code();
        CsvCell ctv3Text = parser.getCtv3Text();

        List<ThreadPoolError> errors =
                threadPool.submit(new SRCtv3Transformer.WebServiceLookup(
                        parser.getCurrentState(), rowId, ctv3Code, ctv3Text));

        handleErrors(errors);
    }

    static class WebServiceLookup implements Callable {

        private CsvCurrentState parserState = null;
        private CsvCell rowId = null;
        private CsvCell ctv3Code = null;
        private CsvCell ctv3Text = null;

        public WebServiceLookup(CsvCurrentState parserState, CsvCell rowId, CsvCell ctv3Code, CsvCell ctv3Text) {
            this.parserState = parserState;
            this.rowId = rowId;
            this.ctv3Code = ctv3Code;
            this.ctv3Text = ctv3Text;
        }

        @Override
        public Object call() throws Exception {

            try {
                ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

                auditWrapper.auditValue(rowId.getRowAuditId(), rowId.getColIndex(), ROW_ID);
                auditWrapper.auditValue(ctv3Code.getRowAuditId(), ctv3Code.getColIndex(), CTV3_CODE);
                auditWrapper.auditValue(ctv3Text.getRowAuditId(), ctv3Text.getColIndex(), CTV3_TEXT);

                TppCtv3Lookup lookup =
                        new TppCtv3Lookup(rowId.getLong(), ctv3Code.getString(), ctv3Text.getString(), auditWrapper);

                //save to the DB
                repository.save(lookup);

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
