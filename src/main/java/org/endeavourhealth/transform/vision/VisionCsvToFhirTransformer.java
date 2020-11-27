package org.endeavourhealth.transform.vision;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.ExchangePayloadFile;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.vision.schema.*;
import org.endeavourhealth.transform.vision.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

public abstract class VisionCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(VisionCsvToFhirTransformer.class);

    public static final String VERSION_TEST_PACK = "TEST_PACK";
    public static final String VERSION_0_18 = "0.18";

    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "HHmm"; //changed from hhmm SD-109
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;   //Vision files do not contain a header, so set on in each parsers constructor

    public static void transform(String exchangeBody, FhirResourceFiler processor, String version) throws Exception {

        //get service
        UUID serviceId = processor.getServiceId();
        Service service = DalProvider.factoryServiceDal().getById(serviceId);

        //get files to process
        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        ExchangeHelper.filterFileTypes(files, service, processor.getExchangeId());
        LOG.info("Invoking Vision CSV transformer for " + files.size() + " files and service " + service.getName() + " " + service.getId());

        if (files.isEmpty()) {
            LOG.info("No files, so returning out");
            return;
        }

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        ExchangePayloadFile.validateFilesAreInSameDirectory(files);

        Map<Class, AbstractCsvParser> parsers = new HashMap<>();

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            createParsers(processor.getServiceId(), processor.getSystemId(), processor.getExchangeId(), files, version, parsers);
            transformParsers(version, parsers, processor);

        } finally {

            closeParsers(parsers.values());
        }
    }


    private static void closeParsers(Collection<AbstractCsvParser> parsers) {
        for (AbstractCsvParser parser : parsers) {
            try {
                parser.close();
            } catch (IOException ex) {
                //don't worry if this fails, as we're done anyway
            }
        }
    }

    public static void createParsers(UUID serviceId, UUID systemId, UUID exchangeId, List<ExchangePayloadFile> files, String version, Map<Class, AbstractCsvParser> parsers) throws Exception {

        for (ExchangePayloadFile fileObj : files) {

            AbstractCsvParser parser = createParserForFile(serviceId, systemId, exchangeId, version, fileObj);
            if (parser != null) {
                Class cls = parser.getClass();
                parsers.put(cls, parser);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static AbstractCsvParser createParserForFile(UUID serviceId, UUID systemId, UUID exchangeId, String version, ExchangePayloadFile fileObj) throws Exception {

        String fileType = fileObj.getType();
        String filePath = fileObj.getPath();

        Class cls = null;
        if (fileType.equals("active_user_data_extract")) {
            //this file type isn't processed (and confirmed we don't need it https://endeavourhealth.atlassian.net/browse/SD-229)
            return null;
        } else if (fileType.equals("patient_check_sum_data_extract")) {
            //this file type isn't processed (doesn't contain anything useful)
            return null;
        } else if (fileType.equals("practice_data_extract")) {
            cls = Practice.class;
        } else if (fileType.equals("staff_data_extract")) {
            cls = Staff.class;
        } else if (fileType.equals("encounter_data_extract")) {
            cls = Encounter.class;
        } else if (fileType.equals("journal_data_extract")) {
            cls = Journal.class;
        } else if (fileType.equals("patient_data_extract")) {
            cls = Patient.class;
        } else if (fileType.equals("referral_data_extract")) {
            cls = Referral.class;
        } else {
            throw new Exception("Unexpected Vision file type [" + fileType + "]");
        }

        //now construct an instance of the parser for the file we've found
        Constructor<AbstractCsvParser> constructor = cls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
        return constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);
    }

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler) throws Exception {

        VisionCsvHelper csvHelper = new VisionCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId());

        //pre-cache various things from the Journal file
        JournalPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        csvHelper.saveCodeAndTermMaps(fhirResourceFiler);
        csvHelper.saveCodeToSnomedMaps(fhirResourceFiler);

        //pre-cache links from referrals to problems and encounters
        ReferralPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        //pre-cache previous linked resources to Encounters
        EncounterPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //run the transforms for non-patient resources
        PracticeTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        StaffTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        //then for the patient resources - note the order of these transforms is important, as encounters should be before journal obs etc.
        PatientTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        EncounterTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        ReferralTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        JournalTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        //if we've received ethnicity journal records but no update to the patient, we
        //need to explicitly update those patients
        csvHelper.processRemainingEthnicities(fhirResourceFiler);

        //if we've received new items linked to problems we've not transformed, we need to explicitly
        //update those problems now
        csvHelper.processRemainingProblemItems(fhirResourceFiler);

        //if we've received new items linked to encounters we've not transformed, we need to explicitly
        //update those encounters now
        csvHelper.processRemainingEncounterItems(fhirResourceFiler);

        //close down the utility thread pool
        csvHelper.stopThreadPool();
    }
}
