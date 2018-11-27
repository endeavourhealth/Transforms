package org.endeavourhealth.transform.common;

import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.PublishedFileDalI;
import org.endeavourhealth.core.database.dal.audit.models.PublishedFileRecord;
import org.endeavourhealth.core.database.dal.audit.models.PublishedFileType;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class PublishedFileAuditHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PublishedFileAuditHelper.class);

    private static final PublishedFileDalI dal = DalProvider.factoryPublishedFileDal();

    public static int auditPublishedFileRecord(ParserI parser, boolean firstRecordContainsHeaders, PublishedFileType publishedFileType) throws Exception {

        String filePath = parser.getFilePath();
        LOG.info("Auditing " + filePath);

        //create file type audit
        int fileTypeId = dal.findOrCreateFileTypeId(publishedFileType);
        //LOG.debug("File type for " + parser.getFilePath() + " = " + fileTypeId);

        //create file audit
        int fileAuditId;
        boolean haveProcessedFileBefore = false;

        UUID serviceId = parser.getServiceId();
        UUID systemId = parser.getSystemId();
        UUID exchangeId = parser.getExchangeId();

        Integer existingId = dal.findFileAudit(serviceId, systemId, exchangeId, fileTypeId, filePath);
        if (existingId != null) {
            fileAuditId = existingId.intValue();
            //LOG.debug("Existing file ID = " + existingId);

            //if we've already been through this file at some point, then we need to re-load the audit IDs
            //for each of the cells in this file, so set this to true
            haveProcessedFileBefore = true;

        } else {
            fileAuditId = dal.auditFile(serviceId, systemId, exchangeId, fileTypeId, filePath);
            //LOG.debug("Created new file ID = " + fileAuditId);
        }

        //use the threadpool for saving these record audit objects, so we can get better throughput
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 5000);

        InputStream inputStream = FileHelper.readFileFromSharedStorage(filePath);
        try {

            AuditRowTask nextTask = new AuditRowTask(parser, haveProcessedFileBefore, fileAuditId);

            //loop through the file
            long currentRecordStart = 0;
            long currentBytePos = 0;
            int recordNumber = 1; //these start at one
            byte[] buffer = new byte[500 * 1024]; //use a 500KB buffer

            while (true) {

                int bytesRead = inputStream.read(buffer);
                if (bytesRead == -1){
                    //if we've hit the end of the file, we're done
                    break;
                }

                for (int i=0; i<bytesRead; i++) {
                    byte b = buffer[i];
                    currentBytePos ++;

                    //when we hit a newline character, then audit that record
                    if (b == '\n') {
                        //LOG.debug("Found newline at " + currentBytePos + " on record " + recordNumber);

                        //don't audit record 0 if we know it contains headers
                        if (recordNumber > 1 || !firstRecordContainsHeaders) {

                            PublishedFileRecord fileRecord = new PublishedFileRecord();
                            fileRecord.setPublishedFileId(fileAuditId);
                            fileRecord.setRecordNumber(recordNumber);
                            fileRecord.setByteStart(currentRecordStart);
                            fileRecord.setByteLength((int) (currentBytePos - currentRecordStart));

                            nextTask.addRecord(fileRecord);
                            if (nextTask.isFull()) {
                                List<ThreadPoolError> errors = threadPool.submit(nextTask);
                                handleErrors(errors);
                                nextTask = new AuditRowTask(parser, haveProcessedFileBefore, fileAuditId);
                            }
                        }

                        recordNumber ++;
                        currentRecordStart = currentBytePos;

                        if (recordNumber % 50000 == 0) {
                            LOG.trace("Auditing Line " + recordNumber + " of " + filePath);
                        }
                    }
                }

                //if we have a trailing record, audit that
                if (currentRecordStart != currentBytePos) {
                    PublishedFileRecord fileRecord = new PublishedFileRecord();
                    fileRecord.setPublishedFileId(fileAuditId);
                    fileRecord.setRecordNumber(recordNumber);
                    fileRecord.setByteStart(currentRecordStart);
                    fileRecord.setByteLength((int) (currentBytePos - currentRecordStart));

                    nextTask.addRecord(fileRecord);
                    if (nextTask.isFull()) {
                        List<ThreadPoolError> errors = threadPool.submit(nextTask);
                        handleErrors(errors);
                        nextTask = new AuditRowTask(parser, haveProcessedFileBefore, fileAuditId);
                    }
                }
            }

            if (!nextTask.isEmpty()) {
                List<ThreadPoolError> errors = threadPool.submit(nextTask);
                handleErrors(errors);
            }

        } finally {
            inputStream.close();

            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }

        return fileAuditId;
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        AuditRowTask callable = (AuditRowTask) first.getCallable();
        Throwable exception = first.getException();
        throw new TransformException("", exception);
    }


    static class AuditRowTask implements Callable {

        private final ParserI parser;
        private final boolean haveProcessedFileBefore;
        private final int fileAuditId;
        private List<PublishedFileRecord> records = new ArrayList<>();

        public AuditRowTask(ParserI parser, boolean haveProcessedFileBefore, int fileAuditId) {
            this.parser = parser;
            this.haveProcessedFileBefore = haveProcessedFileBefore;
            this.fileAuditId = fileAuditId;
        }

        @Override
        public Object call() throws Exception {

            try {

                //if we've done this file before, re-load the past audit
                List<PublishedFileRecord> recordsToInsert = null;

                if (haveProcessedFileBefore) {
                    recordsToInsert = new ArrayList<>();

                    for (PublishedFileRecord record : records) {
                        int recordNumber = record.getRecordNumber();
                        PublishedFileRecord existingRecord = dal.findRecordAuditForRow(fileAuditId, recordNumber);
                        if (existingRecord == null) {
                            recordsToInsert.add(record);
                        }
                    }
                } else {
                    recordsToInsert = records;
                }

                if (!recordsToInsert.isEmpty()) {
                    dal.auditFileRows(recordsToInsert);
                }

                return null;
            } catch (Exception ex) {

                String err = "Exception auditing rows ";
                for (PublishedFileRecord record : records) {
                    err += record.getRecordNumber();
                    err += ", ";
                }
                LOG.error(err, ex);
                throw ex;
            }
        }

        /*public Object call() throws Exception {

            try {

                //if we've done this file before, re-load the past audit
                List<PublishedFileRecord> recordsToInsert = null;

                if (haveProcessedFileBefore) {
                    recordsToInsert = new ArrayList<>();

                    for (PublishedFileRecord record : records) {
                        int recordNumber = record.getRecordNumber();
                        Long existingId = dal.findRecordAuditIdForRow(fileAuditId, recordNumber);
                        if (existingId != null) {
                            long rowAuditId = existingId.longValue();
                            parser.setRowAuditId(recordNumber, rowAuditId);

                        } else {
                            recordsToInsert.add(record);
                        }
                    }
                } else {
                    recordsToInsert = records;
                }

                if (!recordsToInsert.isEmpty()) {
                    dal.auditFileRows(recordsToInsert);

                    //the above call will have set the IDs in each of the record objects
                    for (PublishedFileRecord record: recordsToInsert) {
                        Long rowAuditId = record.getId();
                        int recordNumber = record.getRecordNumber();
                        parser.setRowAuditId(recordNumber, rowAuditId);
                    }
                }

                return null;
            } catch (Exception ex) {

                String err = "Exception auditing rows ";
                for (PublishedFileRecord record : records) {
                    err += record.getRecordNumber();
                    err += ", ";
                }
                LOG.error(err, ex);
                throw ex;
            }
        }*/

        public void addRecord(PublishedFileRecord fileRecord) {
            this.records.add(fileRecord);
        }

        public boolean isFull() {
            return this.records.size() >= TransformConfig.instance().getResourceSaveBatchSize();
        }

        public boolean isEmpty() {
            return this.records.isEmpty();
        }
    }
}
