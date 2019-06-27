package org.endeavourhealth.transform.adastra;

import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.adastra.csv.schema.*;
import org.endeavourhealth.transform.adastra.csv.transforms.*;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AdastraCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AdastraCsvToFhirTransformer.class);

    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "HH:mm:ss";

    public static final String VERSION_1 = "1"; //version pre users added
    public static final String VERSION_2 = "2"; //version post users added


    //Adastra files do not contain a header, so set on in each parsers constructor.  Delimiter is |
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withDelimiter('|');

    public static void transform(String exchangeBody, FhirResourceFiler processor, String version) throws Exception {

        //the exchange body will be a list of files received
        String[] files = ExchangeHelper.parseExchangeBodyOldWay(exchangeBody);
        LOG.info("Invoking Adastra CSV transformer for " + files.length + " files using service " + processor.getServiceId());

        //determine the version from the csv file headers
        version = determineVersion(files);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            validateAndOpenParsers(processor.getServiceId(), processor.getSystemId(), processor.getExchangeId(), files, version, allParsers);

            LOG.trace("Transforming Adastra CSV content in {}", orgDirectory);
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

        if (version.equalsIgnoreCase(VERSION_1) || version.equalsIgnoreCase(VERSION_2)) {

            findFileAndOpenParser(PROVIDER.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(PATIENT.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(CASE.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(CASEQUESTIONS.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(NOTES.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(OUTCOMES.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(CONSULTATION.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(CLINICALCODES.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(PRESCRIPTIONS.class, serviceId, systemId, exchangeId, files, version, parsers);
        }

        //open and add additional version 2 files
        if (version.equalsIgnoreCase(VERSION_2)) {

            findFileAndOpenParser(USERS.class, serviceId, systemId, exchangeId, files, version, parsers);
            findFileAndOpenParser(ElectronicPrescriptions.class, serviceId, systemId, exchangeId, files, version, parsers);
        }

        Set<String> expectedFiles = parsers
                .values()
                .stream()
                .map(T -> T.getFilePath())
                .collect(Collectors.toSet());

        for (String filePath: files) {
            if (!expectedFiles.contains(filePath)
                && !FilenameUtils.getExtension(filePath).equalsIgnoreCase("csv")) {

                throw new FileFormatException(filePath, "Unexpected file " + filePath + " in Adastra CSV extract");
            }
        }
    }

    public static void findFileAndOpenParser(Class parserCls, UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version, Map<Class, AbstractCsvParser> ret) throws Exception {

        String name = parserCls.getSimpleName();

        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);

            //we're only interested in CSV files
            String extension = Files.getFileExtension(fName);
            if (!extension.equalsIgnoreCase("csv")) {
                continue;
            }

            //Adastra files are of format:  Adastra_ODSCODE_CONTENTTYPE_yyyymmddhhmmss.csv
            String[] toks = fName.split("_");
            if (toks.length != 4) {
                continue;
            }

            //No file matching the CONTENTTYPE, i.e. CASE, PATIENT, CONSULTATION
            if (!toks[2].equalsIgnoreCase(name)) {
                continue;
            }

            //now construct an instance of the parser for the file we've found
            Constructor<AbstractCsvParser> constructor = parserCls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
            AbstractCsvParser parser = constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);

            ret.put(parserCls, parser);
            return;
        }

        throw new FileNotFoundException("Failed to find CSV file for " + name);
    }

    public static String determineVersion(String[] files) throws Exception {

        List<String> possibleVersions = new ArrayList<>();
        possibleVersions.add(VERSION_2);
        possibleVersions.add(VERSION_1);


        for (String filePath : files) {

            String fileName = FilenameUtils.getName(filePath);
            String[] toks = fileName.split("_");
            String className = toks[2];
            String clsName = "org.endeavourhealth.transform.adastra.csv.schema." + className;
            Class parserCls = Class.forName(clsName);

            //create a parser for the file but with a v2 version, which will be fine since we never actually parse any data from it
            Constructor<AbstractCsvParser> constructor = parserCls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
            AbstractCsvParser parser = constructor.newInstance(null, null, null, VERSION_2, filePath);

            //calling this will return the possible versions that apply to this parser
            possibleVersions = parser.testForValidVersions(possibleVersions);
            if (possibleVersions.isEmpty()) {
                break;
            }
        }

        //if we end up with one or more possible versions that do apply, then
        //return the first, since that'll be the most recent one
        if (!possibleVersions.isEmpty()) {
            return possibleVersions.get(0);
        }

        throw new TransformException("Unable to determine version for Adastra CSV");
    }

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler) throws Exception {

        AdastraCsvHelper csvHelper
                = new AdastraCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId());

        try {
            //these transforms do not create resources themselves, but cache data that the subsequent ones rely on
            CASEPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            CLINICALCODESPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            PRESCRIPTIONSPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

            //then the admin transforms
            //v2 USERS
            if (version.equalsIgnoreCase(VERSION_2)) {
                USERSTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            }
            PROVIDERTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

            //then for the patient resources - note the order of these transforms is important
            CASETransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            OUTCOMESTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            CASEQUESTIONSTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            csvHelper.getQuestionnaireResponseCache().fileQuestionnaireResponseResources(fhirResourceFiler, csvHelper);

            //Patient transform also finalises and files the Episode data created by case and outcomes
            PATIENTTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            NOTESTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            CONSULTATIONTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            CLINICALCODESTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            PRESCRIPTIONSTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

            //TODO: ELECTRONICPRESCRIPTIONSTransformer - usefull and where map resource?
        }
        finally {
            csvHelper.getOrganisationCache().cleanUpResourceCache();
            csvHelper.getPatientCache().cleanUpResourceCache();
            csvHelper.getEpisodeOfCareCache().cleanUpResourceCache();
        }
    }


}
