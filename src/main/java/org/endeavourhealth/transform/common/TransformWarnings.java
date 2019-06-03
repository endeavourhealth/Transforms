package org.endeavourhealth.transform.common;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.TransformWarningDalI;
import org.endeavourhealth.core.database.dal.audit.models.TransformWarning;
import org.endeavourhealth.core.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformWarnings {
    private static final Logger LOG = LoggerFactory.getLogger(TransformWarnings.class);

    private TransformWarningDalI dal = DalProvider.factoryTransformWarningDal();
    private List<TransformWarning> batch = new ArrayList<>();
    private TransformWarningsRunnable runnable = null;
    private Thread thread = null;


    //singleton
    private static TransformWarnings instance;
    private static ReentrantLock lock = new ReentrantLock();

    public static TransformWarnings instance() {
        if (instance == null) {
            try {
                lock.lock();
                if (instance == null) {
                    instance = new TransformWarnings();
                }
            } finally {
                lock.unlock();
            }
        }
        return instance;
    }

    private TransformWarnings() {
    }

    public static void log(Logger logger, HasServiceSystemAndExchangeIdI impl, String warningText, Object... parameters) throws Exception {
        instance().logObjectsImpl(logger, impl, warningText, parameters);
    }

    public static void log(Logger logger, UUID serviceId, UUID systemId, UUID exchangeId, Integer publishedFileId, Integer recordNumber, String warningText, String... parameters) throws Exception {
        instance().logStringsImpl(logger, serviceId, systemId, exchangeId, publishedFileId, recordNumber, warningText, parameters);
    }


    private void logObjectsImpl(Logger logger, HasServiceSystemAndExchangeIdI impl, String warningText, Object... parameters) throws Exception {

        //get the current source file record ID fromthe parser, which will return -1 if the file isn't audited and we have no ID
        Integer publishedFileId = null;
        Integer recordNumber = null;

        //convert the object parameters to Strings using toString() unless it's a CsvCell in which case we can't use the toString()
        String[] stringParameters = new String[parameters.length];
        for (int i=0; i<parameters.length; i++) {
            Object parameter = parameters[i];
            if (parameter instanceof CsvCell) {
                CsvCell cell = (CsvCell)parameter;
                stringParameters[i] = cell.getString();

                //link the warning back to the record the CSV cell came from
                if (publishedFileId == null
                        && cell.getPublishedFileId() > 0) {
                    publishedFileId = new Integer(cell.getPublishedFileId());
                    recordNumber = new Integer(cell.getRecordNumber());
                }
            } else {
                stringParameters[i] = parameter.toString();
            }
        }

        logStringsImpl(logger, impl.getServiceId(), impl.getSystemId(), impl.getExchangeId(), publishedFileId, recordNumber, warningText, stringParameters);
    }

    private void startThreadIfNecessary() {
        if (this.thread == null) {
            try {
                lock.lock();
                if (this.thread == null) {
                    this.runnable = new TransformWarningsRunnable();
                    this.thread = new Thread(this.runnable);
                    this.thread.start();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void logStringsImpl(Logger logger, UUID serviceId, UUID systemId, UUID exchangeId, Integer publishedFileId, Integer recordNumber, String warningText, String... parameters) throws Exception {

        //write to passed in logger
        logger.warn(warningText, (Object[])parameters);

        //record in the DB
        TransformWarning toSave = new TransformWarning();
        toSave.setServiceId(serviceId);
        toSave.setSystemId(systemId);
        toSave.setExchangeId(exchangeId);
        toSave.setPublishedFileId(publishedFileId);
        toSave.setRecordNumber(recordNumber);
        toSave.setWarningText(warningText);
        toSave.setWarningParams(parameters);

        startThreadIfNecessary();
        runnable.addEntry(toSave);

        //dal.recordWarning(serviceId, systemId, exchangeId, publishedFileId, recordNumber, warningText, parameters);

        //substitute the parameters into the warning String
        for (String parameter: parameters) {
            int index = warningText.indexOf("{}");
            if (index > -1) {
                String prefix = warningText.substring(0, index);
                String suffix = warningText.substring(index+2);
                warningText = prefix + parameter + suffix;

            } else {
                //if the warning text and parameters don't match up, just break out
                break;
            }
        }

        //see if we want to continue without an exception
        boolean fail = false;
        List<Pattern> warningsToContinueOn = TransformConfig.instance().getWarningsToFailOn();
        for (Pattern pattern: warningsToContinueOn) {
            Matcher matcher = pattern.matcher(warningText);
            if (matcher.matches()) {
                fail = true;
                break;
            }
        }

        if (fail) {
            String err = "Exchange " + exchangeId + " for Service " + serviceId + ": " + warningText;
            throw new TransformException(err);
        }
    }

    class TransformWarningsRunnable implements Runnable {

        private List<TransformWarning> queue = new ArrayList<>();

        @Override
        public void run() {
            while (true) {
                try {
                    runQueue();
                    Thread.sleep(1000);
                } catch (Throwable t) {
                    LOG.error("", t);
                }
            }
        }

        private void runQueue() throws Exception {
            List<TransformWarning> pending = null;
            try {
                lock.lock();
                pending = new ArrayList<>(queue);
                queue = new ArrayList<>();

            } finally {
                lock.unlock();
            }

            if (pending.isEmpty()) {
                return;
            }

            List<TransformWarning> batch = new ArrayList<>();
            for (TransformWarning warning: pending) {
                batch.add(warning);

                if (batch.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
                    dal.recordWarnings(batch);
                    batch = new ArrayList<>();
                }
            }

            //save any remaining ones
            if (!batch.isEmpty()) {
                dal.recordWarnings(batch);
            }
        }

        public void addEntry(TransformWarning toSave) {
            try {
                lock.lock();
                queue.add(toSave);

            } finally {
                lock.unlock();
            }
        }
    }
}
