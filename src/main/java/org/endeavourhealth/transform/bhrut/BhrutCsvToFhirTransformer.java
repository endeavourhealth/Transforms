package org.endeavourhealth.transform.bhrut;

import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class BhrutCsvToFhirTransformer {

    //
    //TODO  Warning. Placeholder copied from Vision.
    //


    private static final Logger LOG = LoggerFactory.getLogger(BhrutCsvToFhirTransformer.class);

    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();   //BHRUT files contain a header

    public static void transform(String exchangeBody, FhirResourceFiler processor, String version) throws Exception {

        //the exchange body will be a list of files received
        String[] files = ExchangeHelper.parseExchangeBodyOldWay(exchangeBody);

        if (files.length == 0) {
            LOG.warn("Exchange {} file list is empty for service {} - skipping", processor.getExchangeId().toString(), processor.getServiceId().toString());
            return;
        }

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        LOG.info("Invoking Bhrut CSV transformer for " + files.length + " files using service " + processor.getServiceId());

        Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            validateAndOpenParsers(processor.getServiceId(), processor.getSystemId(), processor.getExchangeId(), files, version, allParsers);

            LOG.trace("Transforming Bhrut CSV content in {}", orgDirectory);
            transformParsers(version, allParsers, processor);

        } finally {

            closeParsers(allParsers.values());
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

    private static void validateAndOpenParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version, Map<Class, AbstractCsvParser> parsers) throws Exception {
        //admin - practice
//        findFileAndOpenParser(Practice.class, serviceId, systemId, exchangeId, files, version, parsers);
//        //admin - staff
//        findFileAndOpenParser(Staff.class, serviceId, systemId, exchangeId, files, version, parsers);
//        //patient - demographics
//        findFileAndOpenParser(Patient.class, serviceId, systemId, exchangeId, files, version, parsers);
//        //clinical - encounters
//        findFileAndOpenParser(Encounter.class, serviceId, systemId, exchangeId, files, version, parsers);
//        //clinical - referrals
//        findFileAndOpenParser(Referral.class, serviceId, systemId, exchangeId, files, version, parsers);
//        //clinical - journal (observations, medication, problems etc.)
//        findFileAndOpenParser(Journal.class, serviceId, systemId, exchangeId, files, version, parsers);
//
//        //then validate there are no extra, unexpected files in the folder, which would imply new data
//        Set<String> expectedFiles = parsers
//                .values()
//                .stream()
//                .map(T -> T.getFilePath())
//                .collect(Collectors.toSet());
//
//        for (String filePath: files) {
//            if (!ignoreKnownFile(filePath)
//                    && !expectedFiles.contains(filePath)
//                    && !FilenameUtils.getExtension(filePath).equalsIgnoreCase("csv")) {
//
//                throw new FileFormatException(filePath, "Unexpected file " + filePath + " in Vision CSV extract");
//            }
//        }
    }

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
    }

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler) throws Exception {

        BhrutCsvHelper csvHelper
                = new BhrutCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId());

        //order will be:  PMI, ALERTS, A&E, SPELLS, EPISODES, Out patients
        //if there are any reference files or references created as part of a pre-transform, run those first

//        //these transforms do not create resources themselves, but cache data that the subsequent ones rely on
//        JournalProblemPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        JournalDrugPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        JournalPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        EncounterPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//
//        //run the transforms for non-patient resources
//        PracticeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        StaffTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//
//        //then for the patient resources - note the order of these transforms is important, as encounters should be before journal obs etc.
//        PatientTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        EncounterTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        ReferralTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        JournalTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

    }
}
