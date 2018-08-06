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

import java.util.ArrayList;
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

        List<TppCtv3Lookup> mappingsToSave = new ArrayList<>();

        try {
            AbstractCsvParser parser = parsers.get(SRCtv3.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processRecord((SRCtv3) parser, threadPool, mappingsToSave);
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

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
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

    public static void processRecord(SRCtv3 parser, ThreadPool threadPool, List<TppCtv3Lookup> mappingsToSave) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        if (rowId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}",
                    rowId.getString(), parser.getFilePath());
            return;
        }
        CsvCell ctv3Code = parser.getCtv3Code();
        CsvCell ctv3Text = parser.getCtv3Text();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(rowId.getRowAuditId(), rowId.getColIndex(), ROW_ID);
        auditWrapper.auditValue(ctv3Code.getRowAuditId(), ctv3Code.getColIndex(), CTV3_CODE);
        auditWrapper.auditValue(ctv3Text.getRowAuditId(), ctv3Text.getColIndex(), CTV3_TEXT);

        TppCtv3Lookup lookup = new TppCtv3Lookup(rowId.getLong(), ctv3Code.getString(), ctv3Text.getString(), auditWrapper);

        mappingsToSave.add(lookup);

        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<TppCtv3Lookup> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();
            List<ThreadPoolError> errors = threadPool.submit(new Task(copy));
            handleErrors(errors);
        }
    }

    static class Task implements Callable {

        private List<TppCtv3Lookup> mappingsToSave = null;

        public Task(List<TppCtv3Lookup> mappingsToSave) {
            this.mappingsToSave = mappingsToSave;
        }

        @Override
        public Object call() throws Exception {

            try {
                //save to the DB
                repository.save(mappingsToSave);

            } catch (Throwable t) {
                String msg = "Error saving CTV3 lookup records for row IDs ";
                for (TppCtv3Lookup mapping: mappingsToSave) {
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
