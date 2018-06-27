package org.endeavourhealth.transform.common;

import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.exceptions.TransformException;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * base class for the thread pool callables used when processing CSV files
 */
public abstract class AbstractCsvCallable implements Callable {

    private CsvCurrentState parserState = null;

    public AbstractCsvCallable(CsvCurrentState parserState) {
        this.parserState = parserState;
    }

    public CsvCurrentState getParserState() {
        return parserState;
    }

    /**
     * utility used to check for errors when we've got some results back from a thread pool
     */
    public static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        Throwable exception = first.getException();
        AbstractCsvCallable callable = (AbstractCsvCallable)first.getCallable();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }
}
