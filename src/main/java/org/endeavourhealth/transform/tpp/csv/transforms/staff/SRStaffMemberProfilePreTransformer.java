package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRStaffMemberProfilePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfile.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 50000);

        List<InternalIdMap> mappingsToSave = new ArrayList<>();

        try {
            AbstractCsvParser parser = parsers.get(SRStaffMemberProfile.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processRecord((SRStaffMemberProfile) parser, fhirResourceFiler, csvHelper, threadPool, mappingsToSave);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }

            if (!mappingsToSave.isEmpty()) {
                List<ThreadPoolError> errors = threadPool.submit(new Task(mappingsToSave, csvHelper));
                handleErrors(errors);
            }

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
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

    private static void processRecord(SRStaffMemberProfile parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper,
                                      ThreadPool threadPool,
                                      List<InternalIdMap> mappingsToSave) throws Exception {

        CsvCell staffProfileIdCell = parser.getRowIdentifier();

        CsvCell staffId = parser.getIDStaffMember(); //NB = rowId in SRStaffMember
        if (staffId.isEmpty()) {
            return;
        }

        InternalIdMap mapping = new InternalIdMap();
        mapping.setServiceId(csvHelper.getServiceId());
        mapping.setIdType(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID);
        mapping.setSourceId(staffProfileIdCell.getString());
        mapping.setDestinationId(staffId.getString());

        mappingsToSave.add(mapping);

        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<InternalIdMap> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();

            List<ThreadPoolError> errors = threadPool.submit(new Task(copy, csvHelper));
            handleErrors(errors);
        }
    }

    static class Task implements Callable {

        private List<InternalIdMap> mappings;
        private TppCsvHelper csvHelper;

        public Task(List<InternalIdMap> mappings, TppCsvHelper csvHelper) {
            this.mappings = mappings;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {
                csvHelper.saveInternalIds(mappings);

            } catch (Throwable t) {
                String msg = "Error saving internal ID maps for staff member profile IDs ";
                for (InternalIdMap mapping: mappings) {
                    msg += mapping.getSourceId();
                    msg += ", ";
                }

                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }
}

