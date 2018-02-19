package org.endeavourhealth.transform.barts;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
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
    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
                                                .withDelimiter('|')
                                                .withEscape('^')
                                                .withQuoteMode(QuoteMode.NONE)
                                                .withQuote((Character)null);
    public static final String PRIMARY_ORG_ODS_CODE = "R1H";
    public static final String PRIMARY_ORG_HL7_OID = "2.16.840.1.113883.3.2540.1";
    public static final String BARTS_RESOURCE_ID_SCOPE = "B";
    //public static final int CODE_SYSTEM_SNOMED = 1000;
    //public static final int CODE_SYSTEM_ICD_10 = 1001;
    //public static final int CODE_SYSTEM_OPCS_4 = 1002;
    public static final String CODE_SYSTEM_CDS_UNIQUE_ID = "http://cerner.com/fhir/cds-unique-id";
    public static final String CODE_SYSTEM_DIAGNOSIS_ID = "http://cerner.com/fhir/diagnosis-id";
    public static final String CODE_SYSTEM_DIAGNOSIS_TYPE = "http://cerner.com/fhir/diagnosis-type";
    public static final String CODE_SYSTEM_PROCEDURE_ID = "http://cerner.com/fhir/procedure-id";
    public static final String CODE_SYSTEM_PROCEDURE_TYPE = "http://cerner.com/fhir/procedure-type";
    public static final String CODE_SYSTEM_OBSERVATION_ID = "http://cerner.com/fhir/observation-id";
    public static final String CODE_SYSTEM_PROBLEM_ID = "http://cerner.com/fhir/problem-id";
    public static final String CODE_SYSTEM_EPISODE_ID = "http://cerner.com/fhir/episodeid";
    public static final String CODE_SYSTEM_ENCOUNTER_ID = "http://cerner.com/fhir/encounterid";
    public static final String CODE_SYSTEM_ENCOUNTER_SLICE_ID = "http://cerner.com/fhir/encounter-slice-id";
    public static final String CODE_SYSTEM_NOMENCLATURE_ID = "http://cerner.com/fhir/nomenclature-id";
    public static final String CODE_SYSTEM_PERSONNEL_POSITION_TYPE = "http://cerner.com/fhir/personnel-position-type";
    public static final String CODE_SYSTEM_PERSONNEL_SPECIALITY_TYPE = "http://cerner.com/fhir/speciality-type";
    public static final String CODE_CONTEXT_DIAGNOSIS = "BARTSCSV_CLIN_DIAGNOSIS";
    public static final String CODE_CONTEXT_PROCEDURE = "BARTSCSV_CLIN_PROCEDURE";

    public static final int VERSION_2_2_FILE_COUNT = 14;

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
        transformAdminAndPatientParsers(serviceId, systemId, exchangeId, files, version, processor, previousErrors);
        transformClinicalParsers(serviceId, systemId, exchangeId, files, version, processor, previousErrors);

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


    private static void transformAdminAndPatientParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors) throws Exception {

        BartsCsvHelper csvHelper = new BartsCsvHelper();
        String [] v2_2Files = new String[VERSION_2_2_FILE_COUNT];

        // loop through file list.  If the file is v2.2, add to a separate list to process in order later
        // Only processing the Admin and Patient files initially
        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);
            //LOG.debug("currFile:" + currFile.getAbsolutePath() + " Type:" + fileType);

            // 2.2 files - put into a separate ordered list for processing later
            if (fileType.compareTo("LOREF") == 0) {
                v2_2Files [0] = filePath;
            } else if (fileType.compareTo("PRSNLREF") == 0) {
                v2_2Files [1] = filePath;
            } else if (fileType.compareTo("PPATI") == 0) {
                v2_2Files [2] = filePath;
            } else if (fileType.compareTo("PPADD") == 0) {
                v2_2Files [3] = filePath;
            } else if (fileType.compareTo("PPALI") == 0) {
                v2_2Files [4] = filePath;
            } else if (fileType.compareTo("PPINF") == 0) {
                v2_2Files [5] = filePath;
            } else if (fileType.compareTo("PPNAM") == 0) {
                v2_2Files [6] = filePath;
            } else if (fileType.compareTo("PPPHO") == 0) {
                v2_2Files [7] = filePath;
            } else if (fileType.compareTo("PPREL") == 0) {
                v2_2Files [8] = filePath;
            } else if (fileType.compareTo("PPAGP") == 0) {
                v2_2Files [9] = filePath;
            }
        }

        // process the 2.2 files, now in dependency order
        for (String filePath: v2_2Files) {
            if (Strings.isNullOrEmpty(filePath))
                continue;

            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);

            if (fileType.compareTo("LOREF") == 0) {
                // call into 2.2 location transform
                LOREF parser = new LOREF(serviceId, systemId, exchangeId, version, filePath, true);
                LOREFTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PRSNLREF") == 0) {
                //call into 2.2 personnel transform
                PRSNLREF parser = new PRSNLREF(serviceId, systemId, exchangeId, version, filePath, true);
                PRSNLREFTransformer.transform(version, parser, fhirResourceFiler, csvHelper);
                parser.close();
            } else if (fileType.compareTo("PPATI") == 0) {
                //call into 2.2 main person/patient transform
                PPATI parser = new PPATI(serviceId, systemId, exchangeId, version, filePath, true);
                PPATITransformer.transform(version, parser, fhirResourceFiler, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PPADD") == 0) {
                //call into 2.2 patient address transform
                PPADD parser = new PPADD(serviceId, systemId, exchangeId, version, filePath, true);
                PPADDTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PPALI") == 0) {
                //call into 2.2 patient alias transform
                PPALI parser = new PPALI(serviceId, systemId, exchangeId, version, filePath, true);
                PPALITransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PPINF") == 0) {
                //TODO: call into 2.2 patient info transform
                PPINF parser = new PPINF(serviceId, systemId, exchangeId, version, filePath, true);
                //PPINFTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PPNAM") == 0) {
                //call into 2.2 patient name transform
                PPNAM parser = new PPNAM(serviceId, systemId, exchangeId, version, filePath, true);
                PPNAMTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PPPHO") == 0) {
                //call into 2.2 patient phone transform
                PPPHO parser = new PPPHO(serviceId, systemId, exchangeId, version, filePath, true);
                PPPHOTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PPREL") == 0) {
                //call into 2.2 patient relation transform
                PPREL parser = new PPREL(serviceId, systemId, exchangeId, version, filePath, true);
                PPRELTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PPAGP") == 0) {
                //call into 2.2 patient GP transform
                PPAGP parser = new PPAGP(serviceId, systemId, exchangeId, version, filePath, true);
                PPAGPTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            }

            PatientResourceCache.filePatientResources(fhirResourceFiler);
        }
    }

    private static void transformClinicalParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 TransformError previousErrors) throws Exception {

        BartsCsvHelper csvHelper = new BartsCsvHelper();
        String [] v2_2Files = new String[VERSION_2_2_FILE_COUNT];

        // loop through file list.  If the file is v2.2, add to a separate list to process in order later
        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);
            //LOG.debug("currFile:" + currFile.getAbsolutePath() + " Type:" + fileType);

           if (fileType.compareTo("ENCNT") == 0) {
                v2_2Files [10] = filePath;
            } else if (fileType.compareTo("DIAGN") == 0) {
                v2_2Files [11] = filePath;
            } else if (fileType.compareTo("PROCE") == 0) {
                v2_2Files [12] = filePath;
            } else if (fileType.compareTo("CLEVE") == 0) {
                v2_2Files [13] = filePath;
            }

            // 2.1 files
           // Commented out for the time being...
            /*else if (fileType.compareTo("BULKPROBLEMS") == 0) {
                BulkProblem parser = new BulkProblem(serviceId, systemId, exchangeId, version, filePath, true);
                BulkProblemTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("BULKDIAGNOSES") == 0) {
                BulkDiagnosis parser = new BulkDiagnosis(serviceId, systemId, exchangeId, version, filePath, true);
                BulkDiagnosisTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("BULKPROCEDURES") == 0) {
                BulkProcedure parser = new BulkProcedure(serviceId, systemId, exchangeId, version, filePath, true);
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
            } else */
            if (fileType.compareTo("PROB") == 0) {
                            Problem parser = new Problem(version, filePath, true);
                            ProblemTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                            parser.close();
            }

            /*
            else if (fileType.compareTo("PROC") == 0) {
                                Procedure parser = new Procedure(version, filePath, true);
                                ProcedureTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                                parser.close();
            } else if (fileType.compareTo("DIAG") == 0) {
                                    Diagnosis parser = new Diagnosis(version, filePath, true);
                                    DiagnosisTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                                    parser.close();
            }
            */
        }

        // process the 2.2 files, now in dependency order
        for (String filePath: v2_2Files) {
            if (Strings.isNullOrEmpty(filePath))
                continue;

            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);

            if (fileType.compareTo("ENCNT") == 0) {
                //call into 2.2 encounter transform
                ENCNT parser = new ENCNT(serviceId, systemId, exchangeId, version, filePath, true);
                ENCNTTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("DIAGN") == 0) {
                //call into 2.2 diagnosis transform
                DIAGN parser = new DIAGN(serviceId, systemId, exchangeId, version, filePath, true);
                DIAGNTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("PROCE") == 0) {
                //call into 2.2 procedure transform
                PROCE parser = new PROCE(serviceId, systemId, exchangeId, version, filePath, true);
                PROCETransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.compareTo("CLEVE") == 0) {
                //call into 2.2 clinical events transform
                CLEVE parser = new CLEVE(serviceId, systemId, exchangeId, version, filePath, true);
                CLEVETransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
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

            // v2.2 files
        } else if (filenamePart1.equalsIgnoreCase("LOREF") ||
                filenamePart1.equalsIgnoreCase("PRSNLREF") ||
                filenamePart1.equalsIgnoreCase("PPATI") ||
                filenamePart1.equalsIgnoreCase("PPADD") ||
                filenamePart1.equalsIgnoreCase("PPALI") ||
                filenamePart1.equalsIgnoreCase("PPINF") ||
                filenamePart1.equalsIgnoreCase("PPNAM") ||
                filenamePart1.equalsIgnoreCase("PPPHO") ||
                filenamePart1.equalsIgnoreCase("PPREL") ||
                filenamePart1.equalsIgnoreCase("PPAGP") ||
                filenamePart1.equalsIgnoreCase("ENCNT") ||
                filenamePart1.equalsIgnoreCase("DIAGN") ||
                filenamePart1.equalsIgnoreCase("PROCE") ||
                filenamePart1.equalsIgnoreCase("ORDER") ||
                filenamePart1.equalsIgnoreCase("CLEVE")) {
            return filenamePart1.toUpperCase();
        } else {
            String filenamePart3 = parts[2];

            if (filenamePart1.compareToIgnoreCase("tailip") == 0) {
                return filenamePart1.toUpperCase();
            } else {
                if (filenamePart1.compareToIgnoreCase("ip") == 0) {
                    return "SUSIP";
                } else if (filenamePart1.compareToIgnoreCase("rnj") == 0) {
                    return filenamePart3.toUpperCase();
                } else {
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
