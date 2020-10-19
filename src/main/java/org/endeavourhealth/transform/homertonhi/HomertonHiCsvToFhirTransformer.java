package org.endeavourhealth.transform.homertonhi;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.homertonhi.schema.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class HomertonHiCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(HomertonHiCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "hh:mm:ss.SSSSSSS";
    public static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180.withHeader();  //TODO check files
    public static final String PRIMARY_ORG_ODS_CODE = "RQX";

    public static void transform(String exchangeBody, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        String[] files = ExchangeHelper.parseExchangeBodyOldWay(exchangeBody);
        LOG.info("Invoking HomertonRf CSV transformer for " + files.length + " files using and service " + fhirResourceFiler.getServiceId());

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //the processor is responsible for saving FHIR resources
        HomertonHiCsvHelper csvHelper
                = new HomertonHiCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId(), version);

        LOG.trace("Transforming HomertonRf CSV content in {}", orgDirectory);

        Map<String, List<String>> fileMap = hashFilesByType(files);
        Map<String, List<ParserI>> parserMap = new HashMap<>();

        try {
            // non-patient transforms

            //process the deletions first by extracting all the deletion hash values to use in each transform

            // process the patient files first, using the Resource caching to collect data from all file before filing
            //PersonTransformer.transform(createParser());.......
            //PersonDemographicsTransformer.transform(createParser());.......
            //PersonAliasTransformer.transform(createParser());.......
            //PersonLanguageTransformer.transform(createParser());.......
            //PersonPhoneTransformer.transform(createParser());.......
            csvHelper.getPatientCache().filePatientResources(fhirResourceFiler);

            //subsequent transforms may refer to Patient resources, so ensure they're all on the DB before continuing
            fhirResourceFiler.waitUntilEverythingIsSaved();

            // clinical pre-transformers

            // clinical transformers

            // if we've got any updates to existing resources that haven't been handled in an above transform,
            // apply them now, i.e. encounter items for previous created encounters


        } finally {
            //if we had any exception that caused us to bomb out of the transform, we'll have
            //potentially cached resources in the DB, so tidy them up now
            csvHelper.getPatientCache().cleanUpResourceCache();
        }
    }

    private static Map<String, List<String>> hashFilesByType(String[] files) throws TransformException {
        Map<String, List<String>> ret = new HashMap<>();

        for (String file: files) {
            String fileName = FilenameUtils.getBaseName(file);
            String type = identifyFileType(fileName);

            //always force into upper case, just in case
            type = type.toUpperCase();

            LOG.trace("Identifying file " + file + " baseName is " + fileName + " type is " + type);

            List<String> list = ret.get(type);
            if (list == null) {
                list = new ArrayList<>();
                ret.put(type, list);
            }
            list.add(file);
        }

        return ret;
    }

    private static List<ParserI> createParsers(Map<String, List<String>> fileMap, Map<String, List<ParserI>> parserMap, String type, HomertonHiCsvHelper csvHelper) throws Exception {
        List<ParserI> ret = parserMap.get(type);
        if (ret == null) {
            ret = new ArrayList<>();

            List<String> files = fileMap.get(type);
            if (files != null) {
                for (String file: files) {
                    ParserI parser = createParser(file, type, csvHelper);
                    ret.add(parser);
                }
            }

            parserMap.put(type, ret);
        }
        return ret;
    }

    private static ParserI createParser(String file, String type, HomertonHiCsvHelper csvHelper) throws Exception {

        UUID serviceId = csvHelper.getServiceId();
        UUID systemId = csvHelper.getSystemId();
        UUID exchangeId = csvHelper.getExchangeId();
        String version = csvHelper.getVersion();

        if (type.equalsIgnoreCase("person")) {
            return new Person(serviceId, systemId, exchangeId, version, file);


        } else {
            throw new TransformException("Unknown file type [" + type + "]");
        }
    }

    private static CSVFormat getFormatType(String file) throws Exception {
        return HomertonHiCsvToFhirTransformer.CSV_FORMAT;
    }

    //TODO: from file name structure
    private static String identifyFileType(String filename) {
        return  filename.split("_")[0].toUpperCase();
    }
}
