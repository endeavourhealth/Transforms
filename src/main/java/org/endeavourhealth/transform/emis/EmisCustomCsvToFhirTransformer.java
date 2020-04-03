package org.endeavourhealth.transform.emis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.exceptions.TransformException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class EmisCustomCsvToFhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCustomCsvToFhirTransformer.class);

    private static final String DATE_FORMAT = "dd/MM/yyyy";
    private static final String TIME_FORMAT = "HH:mm:ss";
    private static final CSVFormat CSV_FORMAT = CSVFormat.TDF
                                                .withHeader()
                                                .withEscape((Character)null)
                                                .withQuote((Character)null)
                                                .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mdde NONE, but validation in the library means we need to use this;


    public static void transform(Exchange exchange, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        String exchangeBody = exchange.getBody();
        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        UUID serviceId = fhirResourceFiler.getServiceId();
        Service service = DalProvider.factoryServiceDal().getById(serviceId);
        ExchangeHelper.filterFileTypes(files, service, fhirResourceFiler.getExchangeId());
        LOG.info("Invoking EMIS CUSTOM CSV transformer for " + files.size() + " files and service " + service.getName() + " " + service.getId());

        //the processor is responsible for saving FHIR resources
        EmisCustomCsvHelper csvHelper = new EmisCustomCsvHelper(serviceId);

        for (ExchangePayloadFile fileObj: files) {
            String filePath = fileObj.getPath();

            AbstractCsvParser parser = null;
            try {

                if (fileObj.getType().equals("RegistrationStatus")) {
                    //we've had two versions of this file, so need to detect which we've got
                    version = detectRegStatusFileVersion(filePath);
                    parser = new RegistrationStatus(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId(), version, filePath, CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
                    RegistrationStatusTransformer.transform(parser, fhirResourceFiler, csvHelper);

                } else if (fileObj.getType().equals("OriginalTerms")) {
                    parser = new OriginalTerms(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId(), null, filePath, CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
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

        //close down the utility thread pool
        csvHelper.stopThreadPool();
    }

    private static String detectRegStatusFileVersion(String filePath) throws Exception {
        List<String> possibleVersions = new ArrayList<>();
        possibleVersions.add(RegistrationStatus.VERSION_WITH_PROCESSING_ID);
        //possibleVersions.add(RegistrationStatus.VERSION_WITHOUT_PROCESSING_ID);

        RegistrationStatus testParser = new RegistrationStatus(null, null, null, null, filePath, CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
        possibleVersions = testParser.testForValidVersions(possibleVersions);

        if (!possibleVersions.isEmpty()) {
            return possibleVersions.get(0);
        }

        throw new TransformException("Unable to determine version for EMIS Custom file");
    }

}

