package org.endeavourhealth.transform.barts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.barts.schema.*;
import org.endeavourhealth.transform.barts.transforms.*;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public abstract class BartsCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd"; //EMIS spec says "dd/MM/yyyy", but test data is different
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;
    public static final String PRIMARY_ORG_ODS_CODE = "R1H";
    public static final String PRIMARY_ORG_HL7_OID = "2.16.840.1.113883.3.2540.1";
    public static final String BARTS_RESOURCE_ID_SCOPE = "B";
    //public static final int CODE_SYSTEM_SNOMED = 1000;
    //public static final int CODE_SYSTEM_ICD_10 = 1001;
    //public static final int CODE_SYSTEM_OPCS_4 = 1002;
    public static final String CODE_SYSTEM_CDS_UNIQUE_ID = "http://cerner.com/fhir/cds-unique-id";
    public static final String CODE_SYSTEM_DIAGNOSIS_ID = "http://cerner.com/fhir/diagnosis-id";
    public static final String CODE_SYSTEM_OBSERVATION_ID = "http://cerner.com/fhir/observation-id";
    public static final String CODE_SYSTEM_PROBLEM_ID = "http://cerner.com/fhir/problem-id";
    public static final String CODE_SYSTEM_EPISODE_ID = "http://cerner.com/fhir/episodeid";
    public static final String CODE_SYSTEM_ENCOUNTER_ID = "http://cerner.com/fhir/encounterid";
    public static final String CODE_CONTEXT_DIAGNOSIS = "BARTSCSV_CLIN_DIAGNOSIS";
    public static final String CODE_CONTEXT_PROCEDURE = "BARTSCSV_CLIN_PROCEDURE";

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String version) throws Exception {

        String[] files = ExchangeHelper.parseExchangeBodyIntoFileList(exchangeBody);
        LOG.info("Invoking Barts CSV transformer for " + files.length + " files using and service " + serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

        //Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        LOG.trace("Transforming Barts CSV content in {}", orgDirectory);
        transformParsers(files, version, processor, previousErrors);

        LOG.trace("Completed transform for service {} - waiting for resources to commit to DB", serviceId);
        processor.waitToFinish();
    }


    /*private static File validateAndFindCommonDirectory(String sharedStoragePath, String[] files) throws Exception {
        String organisationDir = null;

        for (String file: files) {
            File f = new File(sharedStoragePath, file);
            if (!f.exists()) {
                LOG.error("Failed to find file {} in shared storage {}", file, sharedStoragePath);
                throw new FileNotFoundException("" + f + " doesn't exist");
            }
            //LOG.info("Successfully found file {} in shared storage {}", file, sharedStoragePath);

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
    }*/



    private static void transformParsers(String[] files, String version,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors) throws Exception {

        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);
            //LOG.debug("currFile:" + currFile.getAbsolutePath() + " Type:" + fileType);

            if (fileType.compareTo("BULKPROBLEMS") == 0) {
                BulkProblem parser = new BulkProblem(version, filePath, true);
                BulkProblemTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("BULKDIAGNOSES") == 0) {
                BulkDiagnosis parser = new BulkDiagnosis(version, filePath, true);
                BulkDiagnosisTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("BULKPROCEDURES") == 0) {
                BulkProcedure parser = new BulkProcedure(version, filePath, true);
                BulkProcedureTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("SUSOPA") == 0) {
                String tailFilePath = findTailFile(files, "tailopa_DIS." + fName.split("_")[1].split("\\.")[1]);
                //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                Tails tailsParser = new Tails(version, tailFilePath, true);
                TailsPreTransformer.transform(version, tailsParser);

                SusOutpatient parser = new SusOutpatient(version, filePath, true);
                SusOutpatientTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("SUSIP") == 0) {
                    String tailFilePath = findTailFile(files, "tailip_DIS." + fName.split("_")[2] + "_susrnj.dat");
                    //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                    Tails tailsParser = new Tails(version, tailFilePath, true);
                    TailsPreTransformer.transform(version, tailsParser);

                    SusInpatient parser = new SusInpatient(version, filePath, true);
                    SusInpatientTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                    parser.close();
            } else if (fileType.compareTo("SUSAEA") == 0) {
                        String tailFilePath = findTailFile(files, "tailaea_DIS." + fName.split("_")[1].split("\\.")[1]);
                        //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                        Tails tailsParser = new Tails(version, tailFilePath, true);
                        TailsPreTransformer.transform(version, tailsParser);

                        SusEmergency parser = new SusEmergency(version, filePath, true);
                        SusEmergencyTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                        parser.close();
            } else if (fileType.compareTo("PROB") == 0) {
                            Problem parser = new Problem(version, filePath, true);
                            ProblemTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                            parser.close();
            } else if (fileType.compareTo("PROC") == 0) {
                                Procedure parser = new Procedure(version, filePath, true);
                                ProcedureTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                                parser.close();
            } else if (fileType.compareTo("DIAG") == 0) {
                                    Diagnosis parser = new Diagnosis(version, filePath, true);
                                    DiagnosisTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                                    parser.close();
            }
        }
    }

    private static String findTailFile(String[] files, String expectedName) throws TransformException {
        for (String filePath: files) {
            String name = FilenameUtils.getName(filePath);
            if (name.equalsIgnoreCase(expectedName)) {
                return filePath;
            }
        }

        throw new TransformException("Failed to find tail file for expected name " + expectedName);
    }

    private static String identifyFileType(String filename) {
        String[] parts = filename.split("_");
        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];

        if (filenamePart1.compareToIgnoreCase("PC") == 0) {
            // Bulk
            if (filenamePart2.compareToIgnoreCase("PROBLEMS") == 0) {
                return "BULKPROBLEMS";
            } else if (filenamePart2.compareToIgnoreCase("DIAGNOSES") == 0) {
                return "BULKDIAGNOSES";
            } else if (filenamePart2.compareToIgnoreCase("PROCEDURES") == 0) {
                return "BULKPROCEDURES";
            } else {
                return "UNKNOWN";
            }
        } else if (filenamePart1.compareToIgnoreCase("susopa") == 0) {
                return "SUSOPA";
        } else if (filenamePart1.compareToIgnoreCase("susaea") == 0) {
                return "SUSAEA";
        } else if (filenamePart1.compareToIgnoreCase("tailopa") == 0) {
                    return filenamePart1.toUpperCase();
        } else if (filenamePart1.compareToIgnoreCase("tailaea") == 0) {
                        return filenamePart1.toUpperCase();
        } else {
            String filenamePart3 = parts[2];

            if (filenamePart1.compareToIgnoreCase("tailip") == 0) {
                return filenamePart1.toUpperCase();
            } else {
                String filenamePart4 = parts[3];

                if (filenamePart1.compareToIgnoreCase("ip") == 0) {
                    return "SUSIP";
                } else if (filenamePart1.compareToIgnoreCase("rnj") == 0) {
                    return filenamePart3.toUpperCase();
                } else {
                    //String filenamePart5 = parts[4];
                    String filenamePart6 = parts[5];

                    if (filenamePart1.compareToIgnoreCase("GETL") == 0) {
                        return filenamePart3.toUpperCase();
                    } else {
                        return "UNKNOWN";
                    }
                }
            }
        }
    }
}
