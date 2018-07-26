package org.endeavourhealth.transform.homerton;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.homerton.schema.*;
import org.endeavourhealth.transform.homerton.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class HomertonCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(HomertonCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "hh:mm:ss.SSSSSSS";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();
    public static final String PRIMARY_ORG_ODS_CODE = "RQX";
    public static final String HOMERTON_RESOURCE_ID_SCOPE = "H";

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, String version) throws Exception {

        String[] files = ExchangeHelper.parseExchangeBodyOldWay(exchangeBody);
        LOG.info("Invoking Homerton CSV transformer for " + files.length + " files using and service " + serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);
        HomertonCsvHelper csvHelper = new HomertonCsvHelper(serviceId, systemId, exchangeId, null, version);

        LOG.trace("Transforming Homerton CSV content in {}", orgDirectory);

        Map<String, List<String>> fileMap = hashFilesByType(files);
        Map<String, List<ParserI>> parserMap = new HashMap<>();

        try {
            // non-patient transforms
            CodeTransformer.transform(createParsers(fileMap, parserMap, "CODES", csvHelper), fhirResourceFiler, csvHelper);
            //TODO - locations

            // process the bulk patient file first if it exists in the batch, usually in the baseline folder
            PatientTransformer.transform(createParsers(fileMap, parserMap, "PATIENTSFULL", csvHelper), fhirResourceFiler, csvHelper);
            csvHelper.getPatientCache().filePatientResources(fhirResourceFiler);

            //subsequent transforms may refer to Patient resources, so ensure they're all on the DB before continuing
            fhirResourceFiler.waitUntilEverythingIsSaved();

            // process any incremental/delta patients
            PatientTransformer.transform(createParsers(fileMap, parserMap, "PATIENT", csvHelper), fhirResourceFiler, csvHelper);
            csvHelper.getPatientCache().filePatientResources(fhirResourceFiler);

            // clinical pre-transformers
            DiagnosisPreTransformer.transform(createParsers(fileMap, parserMap, "DIAGNOSIS", csvHelper), fhirResourceFiler, csvHelper);
            ProcedurePreTransformer.transform(createParsers(fileMap, parserMap, "PROCEDURE", csvHelper), fhirResourceFiler, csvHelper);

            // clinical transforms
            EncounterTransformer.transform(createParsers(fileMap, parserMap, "ENCOUNTER", csvHelper), fhirResourceFiler, csvHelper);

            fhirResourceFiler.waitUntilEverythingIsSaved();

            DiagnosisTransformer.transform(createParsers(fileMap, parserMap, "DIAGNOSIS", csvHelper), fhirResourceFiler, csvHelper);
            ProcedureTransformer.transform(createParsers(fileMap, parserMap, "PROCEDURE", csvHelper), fhirResourceFiler, csvHelper);


            //TODO - Problems, Allergies

            // if we've got any updates to existing resources that haven't been handled in an above transform,
            // apply them now, i.e. encounter items for previous created encounters
            csvHelper.processRemainingNewConsultationRelationships(fhirResourceFiler);

            LOG.trace("Completed transform for service {} - waiting for resources to commit to DB", serviceId);
            fhirResourceFiler.waitToFinish();
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
        } else if (type.equalsIgnoreCase("PATIENTSFULL")) {
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
