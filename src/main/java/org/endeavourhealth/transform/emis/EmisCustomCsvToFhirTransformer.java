package org.endeavourhealth.transform.emis;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.ExchangePayloadFile;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.custom.helpers.EmisCustomCsvHelper;
import org.endeavourhealth.transform.emis.custom.schema.RegistrationStatus;
import org.endeavourhealth.transform.emis.custom.transforms.RegistrationStatusTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class EmisCustomCsvToFhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCustomCsvToFhirTransformer.class);

    public static final String DATE_FORMAT = "dd/MM/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.TDF.withHeader();

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds) throws Exception {

        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        LOG.info("Invoking EMIS CUSTOM CSV transformer for " + files.size() + " files and service " + serviceId);

        if (files.size() != 1) {
            throw new TransformException("Expecting just a single file for EMIS custom transsform");
        }

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

        EmisCustomCsvHelper csvHelper = new EmisCustomCsvHelper();

        ExchangePayloadFile fileObj = files.get(0);
        String filePath = fileObj.getPath();
        AbstractCsvParser parser = new RegistrationStatus(serviceId, systemId, exchangeId, null, filePath, CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);

        try {
            RegistrationStatusTransformer.transform(parser, fhirResourceFiler, csvHelper);

            csvHelper.saveRegistrationStatues(fhirResourceFiler);

        } finally {
            try {
                parser.close();
            } catch (IOException ex) {
                //don't worry if this fails, as we're done anyway
            }
        }

        LOG.trace("Completed transform for service " + serviceId + " - waiting for resources to commit to DB");
        fhirResourceFiler.waitToFinish();
    }

}

