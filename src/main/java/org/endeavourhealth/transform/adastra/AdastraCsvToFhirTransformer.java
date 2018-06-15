package org.endeavourhealth.transform.adastra;

import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.adastra.cache.EpisodeOfCareResourceCache;
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

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";

    //Adastra files do not contain a header, so set on in each parsers constructor.  Delimiter is |
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withDelimiter('|');

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String version) throws Exception {

        //the exchange body will be a list of files received
        String[] files = ExchangeHelper.parseExchangeBodyIntoFileList(exchangeBody);
        LOG.info("Invoking Adastra CSV transformer for " + files.length + " files using service " + serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

        Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            validateAndOpenParsers(serviceId, systemId, exchangeId, files, version, allParsers);

            LOG.trace("Transforming Adastra CSV content in {}", orgDirectory);
            transformParsers(version, allParsers, processor, previousErrors);

        } finally {

            closeParsers(allParsers.values());
        }

        LOG.trace("Completed transform for service {} - waiting for resources to commit to DB", serviceId);
        processor.waitToFinish();
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

        findFileAndOpenParser(PATIENT.class, serviceId, systemId, exchangeId, files, version, parsers);
        findFileAndOpenParser(PROVIDER.class, serviceId, systemId, exchangeId, files, version, parsers);
        findFileAndOpenParser(CASE.class, serviceId, systemId, exchangeId, files, version, parsers);
        findFileAndOpenParser(CASEQUESTIONS.class, serviceId, systemId, exchangeId, files, version, parsers);
        findFileAndOpenParser(NOTES.class, serviceId, systemId, exchangeId, files, version, parsers);
        findFileAndOpenParser(OUTCOMES.class, serviceId, systemId, exchangeId, files, version, parsers);
        findFileAndOpenParser(CONSULTATION.class, serviceId, systemId, exchangeId, files, version, parsers);
        findFileAndOpenParser(CLINICALCODES.class, serviceId, systemId, exchangeId, files, version, parsers);
        findFileAndOpenParser(PRESCRIPTIONS.class, serviceId, systemId, exchangeId, files, version, parsers);

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

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors) throws Exception {

        AdastraCsvHelper csvHelper
                = new AdastraCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId());

        //these transforms do not create resources themselves, but cache data that the subsequent ones rely on
        CASEPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        CLINICALCODESPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        PRESCRIPTIONSPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //then for the patient resources - note the order of these transforms is important
        CASETransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        OUTCOMESTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        PATIENTTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        EpisodeOfCareResourceCache.clear();

        NOTESTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        CONSULTATIONTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        CLINICALCODESTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        PRESCRIPTIONSTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
    }


}
