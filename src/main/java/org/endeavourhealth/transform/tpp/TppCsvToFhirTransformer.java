package org.endeavourhealth.transform.tpp;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public abstract class TppCsvToFhirTransformer {

       //

    private static final Logger LOG = LoggerFactory.getLogger(TppCsvToFhirTransformer.class);


    public static final String DATE_FORMAT = "dd MMM yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();   //EMIS csv files always contain a header

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors) throws Exception {

    }




}
