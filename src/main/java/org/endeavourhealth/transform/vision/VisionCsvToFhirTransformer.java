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
            //this file type isn't processed
            return null;
        } else if (fileType.equals("patient_check_sum_data_extract")) {
            //this file type isn't processed
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

    /*private static void validateAndOpenParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version, Map<Class, AbstractCsvParser> parsers) throws Exception {
        //admin - practice
        findFileAndOpenParser(Practice.class, serviceId, systemId, exchangeId, files, version, parsers);
        //admin - staff
        findFileAndOpenParser(Staff.class, serviceId, systemId, exchangeId, files, version, parsers);
        //patient - demographics
        findFileAndOpenParser(Patient.class, serviceId, systemId, exchangeId, files, version, parsers);
        //clinical - encounters
        findFileAndOpenParser(Encounter.class, serviceId, systemId, exchangeId, files, version, parsers);
        //clinical - referrals
        findFileAndOpenParser(Referral.class, serviceId, systemId, exchangeId, files, version, parsers);
        //clinical - journal (observations, medication, problems etc.)
        findFileAndOpenParser(Journal.class, serviceId, systemId, exchangeId, files, version, parsers);

        //then validate there are no extra, unexpected files in the folder, which would imply new data
        Set<String> expectedFiles = parsers
                .values()
                .stream()
                .map(T -> T.getFilePath())
                .collect(Collectors.toSet());

        for (String filePath: files) {
            if (!ignoreKnownFile(filePath)
                    && !expectedFiles.contains(filePath)
                    && !FilenameUtils.getExtension(filePath).equalsIgnoreCase("csv")) {

                throw new FileFormatException(filePath, "Unexpected file " + filePath + " in Vision CSV extract");
            }
        }
    }

    // these files comes with the Vision extract but we currently do not transform, so ignore them in the unexpected file check
    private static boolean ignoreKnownFile(String filePath) {
        String name = FilenameUtils.getName(filePath);
        return name.contains("active_user_data") || name.contains("patient_check_sum_data");
    }

    @SuppressWarnings("unchecked")
    public static void findFileAndOpenParser(Class parserCls, UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version, Map<Class, AbstractCsvParser> ret) throws Exception {

        String name = parserCls.getSimpleName();

        int fileCount = 0;

        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);

            fileCount++;

            //we're only interested in CSV files
            String extension = Files.getFileExtension(fName);
            if (!extension.equalsIgnoreCase("csv")) {
                continue;
            }

            //Vision files are format:  FULL_33333_encounter_data_extract-2017-09-23-165206.csv
            String[] toks = fName.split("_");
            if (toks.length != 5) {
                continue;
            }

            //No file matching the class, i.e. Encounter
            if (!toks[2].equalsIgnoreCase(name)) {
                continue;
            }

            //now construct an instance of the parser for the file we've found
            Constructor<AbstractCsvParser> constructor = parserCls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
            AbstractCsvParser parser = constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);

            ret.put(parserCls, parser);
            return;
        }

        //if the file count is 4, manage the scenario below
        if (fileCount == 4) {

            //patient files exist (4) but no admin files.  Must have skipped a day
            if (name.equalsIgnoreCase("Practice") || name.equalsIgnoreCase("Staff")) {
                LOG.trace("Failed to find CSV file for " + name + ". Missing admin file...continuing with transform.");
                return;
            }

            //admin files exist (4) but no patient files.  Admin batch only
            if (name.equalsIgnoreCase("Patient") || name.equalsIgnoreCase("Encounter")
                    || name.equalsIgnoreCase("Journal") || name.equalsIgnoreCase("Referral")) {
                LOG.trace("Failed to find CSV file for " + name + ". Admin batch only...continuing with transform.");
                return;
            }
        }

        throw new FileNotFoundException("Failed to find CSV file for " + name);
    }*/

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler) throws Exception {

        VisionCsvHelper csvHelper = new VisionCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId());

        //these transforms do not create resources themselves, but cache data that the subsequent ones rely on
        JournalProblemPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        JournalDrugPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        JournalPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        EncounterPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        csvHelper.saveCodeAndTermMaps(fhirResourceFiler);
        csvHelper.saveCodeToSnomedMaps(fhirResourceFiler);

        //run the transforms for non-patient resources
        PracticeTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        StaffTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        //then for the patient resources - note the order of these transforms is important, as encounters should be before journal obs etc.
        PatientTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        EncounterTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        ReferralTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        JournalTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        csvHelper.processRemainingEthnicities(fhirResourceFiler);
    }
}
