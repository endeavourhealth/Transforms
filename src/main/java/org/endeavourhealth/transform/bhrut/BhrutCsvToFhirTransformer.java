package org.endeavourhealth.transform.bhrut;

import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.transform.bhrut.schema.*;
import org.endeavourhealth.transform.bhrut.transforms.*;
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

public abstract class BhrutCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(BhrutCsvToFhirTransformer.class);

    public static final String DATE_FORMAT = "dd/MM/yyyy";
    //public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader().withQuoteMode(QuoteMode.MINIMAL);
    public static final CSVFormat CSV_FORMAT_NEW = CSVFormat.RFC4180.withFirstRecordAsHeader().withQuote('"')
            .withSkipHeaderRecord();
    // Strings for calls to IM. The first 3 should be constant for all calls for a publisher
    public final static  String  IM_PROVIDER_CONCEPT_ID ="CM_Org_BHRUT";
    public final static  String  IM_SYSTEM_CONCEPT_ID = "CM_Sys_Medway";
    public final static  String  IM_SCHEMA = "MedwayBI";
    // Table names used by IM for individual transformers
    public final static  String  IM_PMI_TABLE_NAME ="PMI";
    public final static  String  IM_AEATTENDANCE_TABLE_NAME  ="AandeAttendances";
    public final static  String  IM_EPISODES_TABLE_NAME  ="Episodes";
    public final static  String  IM_OUTPATIENTS_TABLE_NAME  ="Outpatients";
    public final static  String  IM_SPELLS_TABLE_NAME  ="Spells";
    //
    public final static  String  IM_CAUSE_OF_DEATH = "CAUSEOFDEATH";
    public final static  String  IM_CAUSE_OF_DEATH_1B = "CAUSEOFDEATH 1B";
    public final static  String  IM_CAUSE_OF_DEATH_1C = "CAUSEOFDEATH 1c";
    public final static  String  IM_CAUSE_OF_DEATH_2 = "CAUSEOFDEATH 2";
    public final static  String  IM_INFECTION_STATUS = "INFECTION_STATUS";
    public final static  String  IM_PATIENT_CLASS = "PATIENT_CLASS";
    public final static  String  IM_ADMISSION_SOURCE_CODE = "ADMISSION_SOURCE_CODE";
    public final static  String  IM_ADMISSION_METHOD_CODE = "ADMISSION_METHOD_CODE";
    public final static  String  IM_ADMISSION_WARD_CODE = "ADMISSION_WARD_CODE";
    public final static  String  IM_DISCHARGE_WARD_CODE = "DISCHARGE_WARD_CODE";
    public final static  String  IM_DISCHARGE_METHOD_CODE = "DISCHARGE_METHOD_CODE";
    public final static  String  IM_DISCHARGE_DEST_CODE = "DISCHARGE_DESTINATION_CODE";

    public static final String BHRUT_ORG_ODS_CODE = "RF4";

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
            //validate the files and, if this the first batch, open the parsers to validate the file formats
            validateAndOpenParsers(processor.getServiceId(), processor.getSystemId(), processor.getExchangeId(), files, version, allParsers);

            LOG.trace("Transforming Bhrut CSV content encounn {}", orgDirectory);
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


        findFileAndOpenParser(PMI.class, serviceId, systemId, exchangeId, files, version, parsers);

        findFileAndOpenParser(Alerts.class, serviceId, systemId, exchangeId, files, version, parsers);

        findFileAndOpenParser(Outpatients.class, serviceId, systemId, exchangeId, files, version, parsers);

        findFileAndOpenParser(Spells.class, serviceId, systemId, exchangeId, files, version, parsers);

        findFileAndOpenParser(Episodes.class, serviceId, systemId, exchangeId, files, version, parsers);

        findFileAndOpenParser(AandeAttendances.class, serviceId, systemId, exchangeId, files, version, parsers);

        //then validate there are no extra, unexpected files in the folder, which would imply new data we don't know about
        Set<String> expectedFiles = parsers
                .values()
                .stream()
                .map(T -> T.getFilePath())
                .collect(Collectors.toSet());
        for (String filePath : files) {
            if (!expectedFiles.contains(filePath)
                    && !FilenameUtils.getExtension(filePath).equalsIgnoreCase("csv")) {

                throw new FileFormatException(filePath, "Unexpected file " + filePath + " in Bhrut CSV extract");
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void findFileAndOpenParser(Class parserCls, UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version, Map<Class, AbstractCsvParser> ret) throws Exception {

        String className = parserCls.getSimpleName();
        for (String filePath : files) {
            String fName = FilenameUtils.getName(filePath);
            //we're only interested in CSV files
            String extension = Files.getFileExtension(fName);
            if (!extension.equalsIgnoreCase("csv")) {
                continue;
            }

            String[] toks = fName.split("_");
            if (toks.length == 5) {
                String fileType = toks[2];
                if (className.equalsIgnoreCase("PMI") && fileType.equalsIgnoreCase("PMI")) {
                    // Class and file match
                } else {
                    continue;
                }
            } else if (toks.length == 6) {
                String fileType = toks[2] + "_" + toks[3];
                if ((className.equalsIgnoreCase("Alerts") && fileType.equalsIgnoreCase("PATIENT_ALERTS"))
                        || (className.equalsIgnoreCase("Episodes") && fileType.equalsIgnoreCase("INPATIENT_EPISODES"))
                        || (className.equalsIgnoreCase("Spells") && fileType.equalsIgnoreCase("INPATIENT_SPELLS"))
                        || (className.equalsIgnoreCase("Outpatients") && fileType.equalsIgnoreCase("OUTPATIENT_APPOINTMENTS"))
                        || (className.equalsIgnoreCase("AandeAttendances") && fileType.equalsIgnoreCase("AE_ATTENDANCES"))) {
                    //Class and file match
                } else {
                    continue;
                }

            }
            //now construct an instance of the parser for the file we've found which matches the className
            Constructor<AbstractCsvParser> constructor = parserCls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
            AbstractCsvParser parser = constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);
            ret.put(parserCls, parser);
            return;

        }
        throw new FileNotFoundException("Failed to find CSV file match for " + className);
    }

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler) throws Exception {

        BhrutCsvHelper csvHelper = new BhrutCsvHelper(fhirResourceFiler.getServiceId(),
                fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId());

        //these pre-transforms create Organization and Practitioner resources which subsequent transforms will reference
        PMIPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        OutpatientsPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        SpellsPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        EpisodesPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //then the patient resources - note the order of these transforms is important, as Patients should be before Encounters
        PMITransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        fhirResourceFiler.waitUntilEverythingIsSaved();
        AlertsTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        fhirResourceFiler.waitUntilEverythingIsSaved();
        OutpatientsTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        SpellsTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        fhirResourceFiler.waitUntilEverythingIsSaved();
        EpisodesTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        AndEAttendanceTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
    }
}
