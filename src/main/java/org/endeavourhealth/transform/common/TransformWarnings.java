package org.endeavourhealth.transform.common;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.TransformWarningDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.slf4j.Logger;

import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformWarnings {

    private static TransformWarningDalI dal = DalProvider.factoryTransformWarningDal();

    public static void log(Logger logger, HasServiceSystemAndExchangeIdI impl, String warningText, Object... parameters) throws Exception {

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

        log(logger, impl.getServiceId(), impl.getSystemId(), impl.getExchangeId(), publishedFileId, recordNumber, warningText, stringParameters);
    }

    public static void log(Logger logger, UUID serviceId, UUID systemId, UUID exchangeId, Integer publishedFileId, Integer recordNumber, String warningText, String... parameters) throws Exception {

        //write to passed in logger
        logger.warn(warningText, (Object[])parameters);

        //record in the DB
        dal.recordWarning(serviceId, systemId, exchangeId, publishedFileId, recordNumber, warningText, parameters);

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
}
