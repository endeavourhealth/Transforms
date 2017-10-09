package org.endeavourhealth.transform.barts;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.barts.schema.*;
import org.endeavourhealth.transform.barts.transforms.*;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public abstract class BartsCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd"; //EMIS spec says "dd/MM/yyyy", but test data is different
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;
    public static final String PRIMARY_ORG_ODS_CODE = "R1H";
    public static final String PRIMARY_ORG_HL7_OID = "2.16.840.1.113883.3.2540.1";
    public static final String BARTS_RESOURCE_ID_SCOPE = "B";

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String sharedStoragePath, int maxFilingThreads, String version) throws Exception {

        //for Barts CSV, the exchange body will be a list of files received
        //split by /n but trim each one, in case there's a sneaky /r in there
        String[] files = exchangeBody.split("\n");
        for (int i=0; i<files.length; i++) {
            String file = files[i].trim();
            files[i] = file;
        }

        LOG.info("Invoking Barts CSV transformer for {} files using {} threads and service {}", files.length, maxFilingThreads, serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        File orgDirectory = validateAndFindCommonDirectory(sharedStoragePath, files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds, maxFilingThreads);

        //Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        LOG.trace("Transforming Barts CSV content in {}", orgDirectory);
        transformParsers(orgDirectory, version, processor, previousErrors, maxFilingThreads);

        LOG.trace("Completed transform for service {} - waiting for resources to commit to DB", serviceId);
        processor.waitToFinish();
    }


    private static File validateAndFindCommonDirectory(String sharedStoragePath, String[] files) throws Exception {
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
    }



    private static void transformParsers(File dir, String version,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors,
                                         int maxFilingThreads) throws Exception {

        for (File currFile: dir.listFiles()) {
            String fName = currFile.getName();
            String fileType = identifyFileType(fName);
            //LOG.debug("currFile:" + currFile.getAbsolutePath() + " Type:" + fileType);

            if (fileType.compareTo("SUSOPA") == 0) {
                File tailFile = new File(currFile.getParent() + File.separator + "tailopa_DIS." + currFile.getName().split("_")[1].split("\\.")[1]);
                //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                Tails tailsParser = new Tails(version, tailFile, true);
                TailsPreTransformer.transform(version, tailsParser);

                SusOutpatient parser = new SusOutpatient(version, currFile, true);
                SusOutpatientTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else {
                if (fileType.compareTo("SUSIP") == 0) {
                    File tailFile = new File(currFile.getParent() + File.separator + "tailip_DIS." + currFile.getName().split("_")[2] + "_susrnj.dat");
                    //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                    Tails tailsParser = new Tails(version, tailFile, true);
                    TailsPreTransformer.transform(version, tailsParser);

                    SusInpatient parser = new SusInpatient(version, currFile, true);
                    SusInpatientTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                    parser.close();
                } else {
                    if (fileType.compareTo("SUSAEA") == 0) {
                        File tailFile = new File(currFile.getParent() + File.separator + "tailaea_DIS." + currFile.getName().split("_")[1].split("\\.")[1]);
                        //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                        Tails tailsParser = new Tails(version, tailFile, true);
                        TailsPreTransformer.transform(version, tailsParser);

                        SusEmergency parser = new SusEmergency(version, currFile, true);
                        SusEmergencyTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                        parser.close();
                    } else {
                        if (fileType.compareTo("PROB") == 0) {
                            Problem parser = new Problem(version, currFile, true);
                            ProblemTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                            parser.close();
                        } else {
                            if (fileType.compareTo("PROC") == 0) {
                                Procedure parser = new Procedure(version, currFile, true);
                                ProcedureTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                                parser.close();
                            } else {
                                if (fileType.compareTo("DIAG") == 0) {
                                    Diagnosis parser = new Diagnosis(version, currFile, true);
                                    DiagnosisTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                                    parser.close();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static String identifyFileType(String filename) {
        String[] parts = filename.split("_");
        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];
        if (filenamePart1.compareToIgnoreCase("susopa") == 0) {
            return "SUSOPA";
        } else {
            if (filenamePart1.compareToIgnoreCase("susaea") == 0) {
                return "SUSAEA";
            } else {
                if (filenamePart1.compareToIgnoreCase("tailopa") == 0) {
                    return filenamePart1.toUpperCase();
                } else {
                    if (filenamePart1.compareToIgnoreCase("tailaea") == 0) {
                        return filenamePart1.toUpperCase();
                    } else {
                        String filenamePart3 = parts[2];

                        if (filenamePart1.compareToIgnoreCase("tailip") == 0) {
                            return filenamePart1.toUpperCase();
                        } else {
                            String filenamePart4 = parts[3];

                            if (filenamePart1.compareToIgnoreCase("ip") == 0) {
                                return "SUSIP";
                            } else {
                                if (filenamePart1.compareToIgnoreCase("rnj") == 0) {
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
            }
        }
    }
}
