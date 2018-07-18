package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3HierarchyRefDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3HierarchyRef;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3Hierarchy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRCtv3HierarchyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3HierarchyTransformer.class);

    private static TppCtv3HierarchyRefDalI repository = DalProvider.factoryTppCtv3HierarchyRefDal();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);


        try {
            AbstractCsvParser parser = parsers.get(SRCtv3Hierarchy.class);
            while (parser.nextRecord()) {

                try {
                    createResource((SRCtv3Hierarchy)parser, threadPool);
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
        SRCtv3HierarchyTransformer.WebServiceLookup callable = (SRCtv3HierarchyTransformer.WebServiceLookup)first.getCallable();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    public static void createResource(SRCtv3Hierarchy parser, ThreadPool threadPool) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        if (rowId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}",
                    rowId.getString(), parser.getFilePath());
            return;
        }
        CsvCell ctv3ParentReadCode = parser.getCtv3CodeParent();
        if (ctv3ParentReadCode.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: Parent Read code missing: {} for rowId{} in file : {}",
                    ctv3ParentReadCode.getString(),rowId.getString(), parser.getFilePath());
            return;
        }
        CsvCell ctv3ChildReadCode = parser.getCtv3CodeChild();
        if (ctv3ChildReadCode.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: Child Read code missing: {} for rowId{} in file : {}",
                    ctv3ChildReadCode.getString(),rowId.getString(), parser.getFilePath());
            return;
        }
        CsvCell ctv3ChildLevel = parser.getChildLevel();
        if (ctv3ChildLevel.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: Child level Read code missing: {} for rowId{} in file : {}",
                    ctv3ChildLevel.getString(),rowId.getString(), parser.getFilePath());
            return;
        }

        List<ThreadPoolError> errors =
                threadPool.submit(new SRCtv3HierarchyTransformer.WebServiceLookup(
                        parser.getCurrentState(), rowId, ctv3ParentReadCode, ctv3ChildReadCode, ctv3ChildLevel));

        handleErrors(errors);
    }

    static class WebServiceLookup implements Callable {

        private CsvCurrentState parserState = null;
        private CsvCell rowId = null;
        private CsvCell ctv3ParentReadCode = null;
        private CsvCell ctv3ChildReadCode = null;
        private CsvCell ctv3ChildLevel = null;

        public WebServiceLookup(CsvCurrentState parserState, CsvCell rowId, CsvCell ctv3ParentReadCode,
                                CsvCell ctv3ChildReadCode, CsvCell ctv3ChildLevel) {

            this.parserState = parserState;
            this.rowId = rowId;
            this.ctv3ParentReadCode = ctv3ParentReadCode;
            this.ctv3ChildReadCode = ctv3ChildReadCode;
            this.ctv3ChildLevel = ctv3ChildLevel;
        }

        @Override
        public Object call() throws Exception {

            try {
                TppCtv3HierarchyRef ref = new TppCtv3HierarchyRef(rowId.getLong(),
                        ctv3ParentReadCode.getString(),
                        ctv3ChildReadCode.getString(),
                        ctv3ChildLevel.getInt());

                //save to the DB
                repository.save(ref);

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
