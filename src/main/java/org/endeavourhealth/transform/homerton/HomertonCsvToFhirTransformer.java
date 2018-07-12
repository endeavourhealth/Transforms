package org.endeavourhealth.transform.homerton;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.homerton.cache.EncounterResourceCache;
import org.endeavourhealth.transform.homerton.cache.PatientResourceCache;
import org.endeavourhealth.transform.homerton.schema.*;
import org.endeavourhealth.transform.homerton.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class HomertonCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(HomertonCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    //public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "hh:mm:ss.SSSSSSS";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();
    public static final String PRIMARY_ORG_ODS_CODE = "RQX";
    public static final String HOMERTON_RESOURCE_ID_SCOPE = "H";
    /*public static final int CODE_SYSTEM_SNOMED = 1000;
    public static final int CODE_SYSTEM_ICD_10 = 1001;
    public static final int CODE_SYSTEM_OPCS_4 = 1002;
    public static final String CODE_SYSTEM_CDS_UNIQUE_ID = "http://cerner.com/fhir/cds-unique-id";
    public static final String CODE_SYSTEM_DIAGNOSIS_ID = "http://cerner.com/fhir/diagnosis-id";
    public static final String CODE_SYSTEM_PROBLEM_ID = "http://cerner.com/fhir/problem-id";
    public static final String CODE_SYSTEM_FIN_NO = "http://cerner.com/fhir/fin-no";
    public static final String CODE_SYSTEM_EPISODE_ID = "http://cerner.com/fhir/episodeid";
    public static final String CODE_SYSTEM_ENCOUNTER_ID = "http://cerner.com/fhir/encounterid";
    public static final String CODE_SYSTEM_CONDITION_CATEGORY = "http://hl7.org/fhir/condition-category";
    public static final String CODE_SYSTEM_NHS_NO = "http://fhir.nhs.net/Id/nhs-number";*/

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String version) throws Exception {

        String[] files = ExchangeHelper.parseExchangeBodyOldWay(exchangeBody);
        LOG.info("Invoking Homerton CSV transformer for " + files.length + " files using and service " + serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);
        HomertonCsvHelper csvHelper = new HomertonCsvHelper(serviceId, systemId, exchangeId, null, version);

        //Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        LOG.trace("Transforming Homerton CSV content in {}", orgDirectory);
        //transformParsers(serviceId, systemId, exchangeId, files, version, fhirResourceFiler, previousErrors, csvHelper);

        Map<String, List<String>> fileMap = hashFilesByType(files);
        Map<String, List<ParserI>> parserMap = new HashMap<>();

        CodeTransformer.transform(version, createParsers(fileMap, parserMap, "CODES", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);

        PatientTransformer.transform(version, createParsers(fileMap, parserMap, "PATIENT", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);
        PatientResourceCache.filePatientResources(fhirResourceFiler);

        EncounterTransformer.transform(version, createParsers(fileMap, parserMap, "ENCOUNTER", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);
        EncounterResourceCache.fileEncounterResources(fhirResourceFiler);

        DiagnosisTransformer.transform(version, createParsers(fileMap, parserMap, "DIAGNOSIS", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);

        ProblemTransformer.transform(version, createParsers(fileMap, parserMap, "PROBLEM", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);

        ProcedureTransformer.transform(version, createParsers(fileMap, parserMap, "PROCEDURE", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);

        LOG.trace("Completed transform for service {} - waiting for resources to commit to DB", serviceId);
        fhirResourceFiler.waitToFinish();
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

    private static List<ParserI> createParsers(Map<String, List<String>> fileMap, Map<String, List<ParserI>> parserMap, String type, HomertonCsvHelper csvHelper) throws Exception {
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

    private static ParserI createParser(String file, String type, HomertonCsvHelper csvHelper) throws Exception {

        UUID serviceId = csvHelper.getServiceId();
        UUID systemId = csvHelper.getSystemId();
        UUID exchangeId = csvHelper.getExchangeId();
        String version = csvHelper.getVersion();

        if (type.equalsIgnoreCase("CODES")) {
            return new CodeTable(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("DIAGNOSIS")) {
            return new DiagnosisTable(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("ENCOUNTER")) {
            return new EncounterTable(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PATIENT")) {
            return new PatientTable(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PROBLEM")) {
            return new ProblemTable(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PROCEDURE")) {
            return new ProcedureTable(serviceId, systemId, exchangeId, version, file);
        } else {
            throw new TransformException("Unknown file type [" + type + "]");
        }
    }

    private static CSVFormat getFormatType(String file) throws Exception {
        return HomertonCsvToFhirTransformer.CSV_FORMAT;
    }

    private static String identifyFileType(String filename) {
        return  filename.split("_")[0].toUpperCase();
    }
}
