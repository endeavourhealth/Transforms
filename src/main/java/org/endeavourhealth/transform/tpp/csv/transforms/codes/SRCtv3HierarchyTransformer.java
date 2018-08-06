package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3HierarchyRefDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3HierarchyRef;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3Hierarchy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRCtv3HierarchyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3HierarchyTransformer.class);

    private static TppCtv3HierarchyRefDalI repository = DalProvider.factoryTppCtv3HierarchyRefDal();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) throws Exception {

        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            List<TppCtv3HierarchyRef> mappingsToSave = new ArrayList<>();

            AbstractCsvParser parser = parsers.get(SRCtv3Hierarchy.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processRecord((SRCtv3Hierarchy) parser, threadPool, csvHelper, mappingsToSave);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }

            //and save any still pending
            if (!mappingsToSave.isEmpty()) {
                List<ThreadPoolError> errors = threadPool.submit(new Task(mappingsToSave));
                handleErrors(errors);
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
        throw new TransformException("", exception);
    }

    public static void processRecord(SRCtv3Hierarchy parser, ThreadPool threadPool, TppCsvHelper csvHelper, List<TppCtv3HierarchyRef> mappingsToSave) throws Exception {

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

        TppCtv3HierarchyRef mapping = new TppCtv3HierarchyRef(rowId.getLong(),
                ctv3ParentReadCode.getString(),
                ctv3ChildReadCode.getString(),
                ctv3ChildLevel.getInt());

        mappingsToSave.add(mapping);

        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<TppCtv3HierarchyRef> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();
            List<ThreadPoolError> errors = threadPool.submit(new Task(copy));
            handleErrors(errors);
        }
    }

    static class Task implements Callable {

        private List<TppCtv3HierarchyRef> mappingsToSave;

        public Task(List<TppCtv3HierarchyRef> mappingsToSave) {

            this.mappingsToSave = mappingsToSave;
        }

        @Override
        public Object call() throws Exception {

            try {
                //save to the DB
                repository.save(mappingsToSave);

            } catch (Throwable t) {
                String msg = "Error saving CTV3 hierarchy records for row IDs ";
                for (TppCtv3HierarchyRef mapping: mappingsToSave) {
                    msg += mapping.getRowId();
                    msg += ", ";
                }

                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }
}
