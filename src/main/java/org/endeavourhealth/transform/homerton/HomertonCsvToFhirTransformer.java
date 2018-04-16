package org.endeavourhealth.transform.homerton;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.homerton.schema.CVREF;
import org.endeavourhealth.transform.homerton.schema.Diagnosis;
import org.endeavourhealth.transform.homerton.schema.Patient;
import org.endeavourhealth.transform.homerton.schema.Problem;
import org.endeavourhealth.transform.homerton.schema.Procedure;
import org.endeavourhealth.transform.homerton.transforms.CVREFTransformer;
import org.endeavourhealth.transform.homerton.transforms.DiagnosisTransformer;
import org.endeavourhealth.transform.homerton.transforms.PatientTransformer;
import org.endeavourhealth.transform.homerton.transforms.ProblemTransformer;
import org.endeavourhealth.transform.homerton.transforms.ProcedureTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public abstract class HomertonCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(HomertonCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;
    public static final String PRIMARY_ORG_ODS_CODE = "RQX";
    public static final String HOMERTON_RESOURCE_ID_SCOPE = "H";
    public static final int CODE_SYSTEM_SNOMED = 1000;
    public static final int CODE_SYSTEM_ICD_10 = 1001;
    public static final int CODE_SYSTEM_OPCS_4 = 1002;
    /*public static final String CODE_SYSTEM_CDS_UNIQUE_ID = "http://cerner.com/fhir/cds-unique-id";
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

        String[] files = ExchangeHelper.parseExchangeBodyIntoFileList(exchangeBody);
        LOG.info("Invoking Homerton CSV transformer for " + files.length + " files using and service " + serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);
        HomertonCsvHelper csvHelper = new HomertonCsvHelper(serviceId, systemId, exchangeId, null, version);

        //Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        LOG.trace("Transforming Homerton CSV content in {}", orgDirectory);
        transformParsers(serviceId, systemId, exchangeId, files, version, processor, previousErrors, csvHelper);

        LOG.trace("Completed transform for service {} - waiting for resources to commit to DB", serviceId);
        processor.waitToFinish();
    }


    /*
    private static File validateAndFindCommonDirectory(String sharedStoragePath, String[] files) throws Exception {
        String organisationDir = null;

        for (String file: files) {
            File f = new File(sharedStoragePath, file);
            if (!f.exists()) {
                LOG.error("Failed to find file {} in shared storage {}", file, sharedStoragePath);
                throw new FileNotFoundException("" + f + " doesn't exist");
            }
            LOG.info("Successfully found file {} in shared storage {}", file, sharedStoragePath);

            try {
                File orgDir = f.getParentFile();

                if (organisationDir == null) {
                    organisationDir = orgDir.getAbsolutePath();
                } else {
                    if (!organisationDir.equalsIgnoreCase(orgDir.getAbsolutePath())) {
                        throw new Exception();
                    }
                }

            } catch (Exception ex) {
                throw new FileNotFoundException("" + f + " isn't in the expected directory structure within " + organisationDir);
            }

        }
        return new File(organisationDir);
    }
    */


    private static void transformParsers(UUID serviceId, UUID systemId, UUID exchangeId,
                                         String[] files, String version,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors,
                                         HomertonCsvHelper csvHelper) throws Exception {

        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);
            LOG.debug("currFile:" + filePath + " Type:" + fileType);

            if (fileType.compareTo("PATIENT") == 0) {
                Patient parser = new Patient(serviceId, systemId, exchangeId, version, filePath);
                PatientTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);
                parser.close();
            } else if (fileType.compareTo("PROBLEM") == 0) {
                Problem parser = new Problem(serviceId, systemId, exchangeId, version, filePath);
                ProblemTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);
                parser.close();
            } else if (fileType.compareTo("DIAGNOSIS") == 0) {
                Diagnosis parser = new Diagnosis(serviceId, systemId, exchangeId, version, filePath);
                DiagnosisTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);
                parser.close();
            } else if (fileType.compareTo("CVREF") == 0) {
                CVREF parser = new CVREF(serviceId, systemId, exchangeId, version, filePath);
                CVREFTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);
                parser.close();
            } else if (fileType.compareTo("PROCEDURE") == 0) {
                Procedure parser = new Procedure(serviceId, systemId, exchangeId, version, filePath);
                ProcedureTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE);
                parser.close();
            }
        }
    }

    private static String identifyFileType(String filename) {
        return  filename.split("\\.")[0].toUpperCase();
    }
}
