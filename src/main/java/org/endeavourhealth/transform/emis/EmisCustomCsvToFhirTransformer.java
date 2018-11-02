package org.endeavourhealth.transform.emis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.ExchangePayloadFile;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.custom.helpers.EmisCustomCsvHelper;
import org.endeavourhealth.transform.emis.custom.schema.OriginalTerms;
import org.endeavourhealth.transform.emis.custom.schema.RegistrationStatus;
import org.endeavourhealth.transform.emis.custom.transforms.OriginalTermsTransformer;
import org.endeavourhealth.transform.emis.custom.transforms.RegistrationStatusTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class EmisCustomCsvToFhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCustomCsvToFhirTransformer.class);

    private static final String DATE_FORMAT = "dd/MM/yyyy";
    private static final String TIME_FORMAT = "hh:mm:ss";
    private static final CSVFormat CSV_FORMAT = CSVFormat.TDF
                                                .withHeader()
                                                .withEscape((Character)null)
                                                .withQuote((Character)null)
                                                .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mdde NONE, but validation in the library means we need to use this;


    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds) throws Exception {

        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        LOG.info("Invoking EMIS CUSTOM CSV transformer for " + files.size() + " files and service " + serviceId);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);
        EmisCustomCsvHelper csvHelper = new EmisCustomCsvHelper();

        for (ExchangePayloadFile fileObj: files) {
            String filePath = fileObj.getPath();

            AbstractCsvParser parser = null;
            try {

                if (fileObj.getType().equals("RegistrationStatus")) {
                    parser = new RegistrationStatus(serviceId, systemId, exchangeId, null, filePath, CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
                    RegistrationStatusTransformer.transform(parser, fhirResourceFiler, csvHelper);

                } else if (fileObj.getType().equals("OriginalTerms")) {
                    parser = new OriginalTerms(serviceId, systemId, exchangeId, null, filePath, CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
                    OriginalTermsTransformer.transform(parser, fhirResourceFiler, csvHelper);

                } else {
                    throw new Exception("Unsupported file type " + fileObj.getType());
                }

            } finally {
                try {
                    parser.close();
                } catch (IOException ex) {
                    //don't worry if this fails, as we're done anyway
                }
            }
        }

        csvHelper.saveRegistrationStatues(fhirResourceFiler);

        LOG.trace("Completed transform for service " + serviceId + " - waiting for resources to commit to DB");
        fhirResourceFiler.waitToFinish();
    }

}

