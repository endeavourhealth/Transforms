package org.endeavourhealth.transform.barts;

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
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class BartsCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT = "dd/MM/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
                                                .withHeader()
                                                .withDelimiter('|')
                                                .withEscape('^')
                                                //.withQuoteMode(QuoteMode.NONE)
                                                //.withQuote((char)null);
                                                .withQuoteMode(QuoteMode.MINIMAL) //older combined files created by the SFTP Reader may have some quoting
                                                .withQuote('\"');

    public static final String PRIMARY_ORG_ODS_CODE = "R1H";
    public static final String PRIMARY_ORG_HL7_OID = "2.16.840.1.113883.3.2540.1";
    public static final String BARTS_RESOURCE_ID_SCOPE = "B";

    public static final int VERSION_2_2_FILE_COUNT = 14;

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String version) throws Exception {

        String[] files = ExchangeHelper.parseExchangeBodyIntoFileList(exchangeBody);
        LOG.info("Invoking Barts CSV transformer for " + files.length + " files using and service " + serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);
        LOG.trace("Transforming Barts CSV content in " + orgDirectory);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);
        BartsCsvHelper csvHelper = new BartsCsvHelper(serviceId, systemId, exchangeId, PRIMARY_ORG_HL7_OID, version);

        /*transformAdminAndPatientParsers(serviceId, systemId, exchangeId, files, version, fhirResourceFiler, csvHelper, previousErrors);
        transformClinicalParsers(serviceId, systemId, exchangeId, files, version, fhirResourceFiler, csvHelper, previousErrors);*/

        Map<String, List<String>> fileMap = hashFilesByType(files);
        Map<String, List<ParserI>> parserMap = new HashMap<>();

        //admin transformers
        LOREFTransformer.transform(version, createParser(fileMap, parserMap, "LOREF", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        PRSNLREFTransformer.transform(version, createParser(fileMap, parserMap, "PRSNLREF", csvHelper), fhirResourceFiler, csvHelper);

        //patient transformers
        PPATITransformer.transform(version, createParser(fileMap, parserMap, "PPATI", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        PPADDTransformer.transform(version, createParser(fileMap, parserMap, "PPADD", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        PPALITransformer.transform(version, createParser(fileMap, parserMap, "PPALI", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        //PPINFTransformer.transform(version, createParser(fileMap, parserMap, "PPINF", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        PPNAMTransformer.transform(version, createParser(fileMap, parserMap, "PPNAM", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        PPPHOTransformer.transform(version, createParser(fileMap, parserMap, "PPPHO", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        PPRELTransformer.transform(version, createParser(fileMap, parserMap, "PPREL", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        PPAGPTransformer.transform(version, createParser(fileMap, parserMap, "PPAGP", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);

        //we're now good to save our patient resources
        PatientResourceCache.filePatientResources(fhirResourceFiler);

        //pre-transformers to cache clinical data used later on
        CLEVEPreTransformer.transform(version, createParser(fileMap, parserMap, "CLEVE", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);

        //clinical transformers
        ENCNTTransformer.transform(version, createParser(fileMap, parserMap, "ENCNT", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        DIAGNTransformer.transform(version, createParser(fileMap, parserMap, "DIAGN", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        PROCETransformer.transform(version, createParser(fileMap, parserMap, "PROCE", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        CLEVETransformer.transform(version, createParser(fileMap, parserMap, "CLEVE", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
        ProblemTransformer.transform(version, createParsers(fileMap, parserMap, "PROB", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);

        //if we've got any updates to existing resources that haven't been handled in an above transform, apply them now
        csvHelper.processRemainingClinicalEventParentChildLinks(fhirResourceFiler);

        LOG.trace("Completed transform for service " + serviceId + " - waiting for resources to commit to DB");
        fhirResourceFiler.waitToFinish();
    }

    /**
     * most files should only exist once, so use this fn to create the parser
     */
    private static ParserI createParser(Map<String, List<String>> fileMap, Map<String, List<ParserI>> parserMap, String type, BartsCsvHelper csvHelper) throws Exception {
        List<ParserI> list = createParsers(fileMap, parserMap, type, csvHelper);
        if (list.isEmpty()) {
            return null;

        } else if (list.size() > 1) {
            throw new TransformException("" + list.size() + " files found for type " + type);

        } else {
            return list.get(0);
        }
    }

    /**
     * lazily creates parsers for the given file type on any matching files
     */
    private static List<ParserI> createParsers(Map<String, List<String>> fileMap, Map<String, List<ParserI>> parserMap, String type, BartsCsvHelper csvHelper) throws Exception {
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

    private static ParserI createParser(String file, String type, BartsCsvHelper csvHelper) throws Exception {

        UUID serviceId = csvHelper.getServiceId();
        UUID systemId = csvHelper.getSystemId();
        UUID exchangeId = csvHelper.getExchangeId();
        String version = csvHelper.getVersion();

        if (type.equalsIgnoreCase("LOREF")) {
            return new LOREF(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PRSNLREF")) {
            return new PRSNLREF(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PPATI")) {
            return new PPATI(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PPADD")) {
            return new PPADD(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PPALI")) {
            return new PPALI(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PPINF")) {
            return new PPINF(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PPNAM")) {
            return new PPNAM(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PPPHO")) {
            return new PPPHO(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PPREL")) {
            return new PPREL(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PPAGP")) {
            return new PPAGP(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("ENCNT")) {
            return new ENCNT(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("DIAGN")) {
            return new DIAGN(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PROCE")) {
            return new PROCE(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("CLEVE")) {
            return new CLEVE(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("PROB")) {
            return new Problem(version, file, true);
        } else {
            throw new TransformException("Unknown file type [" + type + "]");
        }
    }

    private static Map<String, List<String>> hashFilesByType(String[] files) throws TransformException {
        Map<String, List<String>> ret = new HashMap<>();
        
        for (String file: files) {
            String fileName = FilenameUtils.getBaseName(file);
            String type = identifyFileType(fileName);

            //always force into upper case, just in case
            type = type.toUpperCase();

            List<String> list = ret.get(type);
            if (list == null) {
                list = new ArrayList<>();
                ret.put(type, list);
            }
            list.add(file);
        }

        return ret;
    }

    /*private static void transformAdminAndPatientParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version,
                                         FhirResourceFiler fhirResourceFiler,
                                        BartsCsvHelper csvHelper,
                                         TransformError previousErrors) throws Exception {

        String [] v2_2Files = new String[VERSION_2_2_FILE_COUNT];

        // loop through file list.  If the file is v2.2, add to a separate list to process in order later
        // Only processing the Admin and Patient files initially
        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);
            //LOG.debug("currFile:" + currFile.getAbsolutePath() + " Type:" + fileType);

            // 2.2 files - put into a separate ordered list for processing later
            if (fileType.equalsIgnoreCase("LOREF")) {
                v2_2Files [0] = filePath;
            } else if (fileType.equalsIgnoreCase("PRSNLREF")) {
                v2_2Files [1] = filePath;
            } else if (fileType.equalsIgnoreCase("PPATI")) {
                v2_2Files [2] = filePath;
            } else if (fileType.equalsIgnoreCase("PPADD")) {
                v2_2Files [3] = filePath;
            } else if (fileType.equalsIgnoreCase("PPALI")) {
                v2_2Files [4] = filePath;
            } else if (fileType.equalsIgnoreCase("PPINF")) {
                v2_2Files [5] = filePath;
            } else if (fileType.equalsIgnoreCase("PPNAM")) {
                v2_2Files [6] = filePath;
            } else if (fileType.equalsIgnoreCase("PPPHO")) {
                v2_2Files [7] = filePath;
            } else if (fileType.equalsIgnoreCase("PPREL")) {
                v2_2Files [8] = filePath;
            } else if (fileType.equalsIgnoreCase("PPAGP")) {
                v2_2Files [9] = filePath;
            }
        }

        // process the 2.2 files, now in dependency order
        for (String filePath: v2_2Files) {
            if (Strings.isNullOrEmpty(filePath))
                continue;

            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);

            if (fileType.equalsIgnoreCase("LOREF")) {
                // call into 2.2 location transform
                LOREF parser = new LOREF(serviceId, systemId, exchangeId, version, filePath);
                LOREFTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PRSNLREF")) {
                //call into 2.2 personnel transform
                PRSNLREF parser = new PRSNLREF(serviceId, systemId, exchangeId, version, filePath);
                PRSNLREFTransformer.transform(version, parser, fhirResourceFiler, csvHelper);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PPATI")) {
                //call into 2.2 main person/patient transform
                PPATI parser = new PPATI(serviceId, systemId, exchangeId, version, filePath);
                PPATITransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PPADD")) {
                //call into 2.2 patient address transform
                PPADD parser = new PPADD(serviceId, systemId, exchangeId, version, filePath);
                PPADDTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PPALI")) {
                //call into 2.2 patient alias transform
                PPALI parser = new PPALI(serviceId, systemId, exchangeId, version, filePath);
                PPALITransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PPINF")) {
                //TODO: call into 2.2 patient info transform
                PPINF parser = new PPINF(serviceId, systemId, exchangeId, version, filePath);
                //PPINFTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PPNAM")) {
                //call into 2.2 patient name transform
                PPNAM parser = new PPNAM(serviceId, systemId, exchangeId, version, filePath);
                PPNAMTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PPPHO")) {
                //call into 2.2 patient phone transform
                PPPHO parser = new PPPHO(serviceId, systemId, exchangeId, version, filePath);
                PPPHOTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PPREL")) {
                //call into 2.2 patient relation transform
                PPREL parser = new PPREL(serviceId, systemId, exchangeId, version, filePath);
                PPRELTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PPAGP")) {
                //call into 2.2 patient GP transform
                PPAGP parser = new PPAGP(serviceId, systemId, exchangeId, version, filePath);
                PPAGPTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            }
        }

        PatientResourceCache.filePatientResources(fhirResourceFiler);
    }

    private static void transformClinicalParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 BartsCsvHelper csvHelper,
                                                 TransformError previousErrors) throws Exception {


        String [] v2_2Files = new String[VERSION_2_2_FILE_COUNT];

        // loop through file list.  If the file is v2.2, add to a separate list to process in order later
        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);
            //LOG.debug("currFile:" + currFile.getAbsolutePath() + " Type:" + fileType);

           if (fileType.equalsIgnoreCase("ENCNT")) {
                v2_2Files [10] = filePath;
            } else if (fileType.equalsIgnoreCase("DIAGN")) {
                v2_2Files [11] = filePath;
            } else if (fileType.equalsIgnoreCase("PROCE")) {
                v2_2Files [12] = filePath;
            } else if (fileType.equalsIgnoreCase("CLEVE")) {
                v2_2Files [13] = filePath;
            }

            // 2.1 files
           // Commented out for the time being...
            *//*else if (fileType.equalsIgnoreCase("BULKPROBLEMS")) {
                BulkProblem parser = new BulkProblem(serviceId, systemId, exchangeId, version, filePath, true);
                BulkProblemTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("BULKDIAGNOSES")) {
                BulkDiagnosis parser = new BulkDiagnosis(serviceId, systemId, exchangeId, version, filePath, true);
                BulkDiagnosisTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("BULKPROCEDURES")) {
                BulkProcedure parser = new BulkProcedure(serviceId, systemId, exchangeId, version, filePath, true);
                BulkProcedureTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("SUSOPA")) {
                String tailFilePath = findTailFile(files, "tailopa_DIS." + fName.split("_")[1].split("\\.")[1]);
                //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                Tails tailsParser = new Tails(version, tailFilePath, true);
                TailsPreTransformer.transform(version, tailsParser);

                SusOutpatient parser = new SusOutpatient(version, filePath, true);
                SusOutpatientTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("SUSIP")) {
                    String tailFilePath = findTailFile(files, "tailip_DIS." + fName.split("_")[2] + "_susrnj.dat");
                    //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                    Tails tailsParser = new Tails(version, tailFilePath, true);
                    TailsPreTransformer.transform(version, tailsParser);

                    SusInpatient parser = new SusInpatient(version, filePath, true);
                    SusInpatientTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                    parser.close();
            } else if (fileType.equalsIgnoreCase("SUSAEA")) {
                        String tailFilePath = findTailFile(files, "tailaea_DIS." + fName.split("_")[1].split("\\.")[1]);
                        //LOG.debug("currFile:" + currFile.getAbsolutePath() + " TailFile:" + tailFile.getAbsolutePath());
                        Tails tailsParser = new Tails(version, tailFilePath, true);
                        TailsPreTransformer.transform(version, tailsParser);

                        SusEmergency parser = new SusEmergency(version, filePath, true);
                        SusEmergencyTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                        parser.close();
            } else *//*
            if (fileType.equalsIgnoreCase("PROB")) {
                            Problem parser = new Problem(version, filePath, true);
                            List<ParserI> parsers = new ArrayList<>();
                            parsers.add(parser);
                            ProblemTransformer.transform(version, parsers, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                            parser.close();
            }

            *//*
            else if (fileType.equalsIgnoreCase("PROC")) {
                                Procedure parser = new Procedure(version, filePath, true);
                                ProcedureTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                                parser.close();
            } else if (fileType.equalsIgnoreCase("DIAG")) {
                                    Diagnosis parser = new Diagnosis(version, filePath, true);
                                    DiagnosisTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                                    parser.close();
            }
            *//*
        }

        // process the 2.2 files, now in dependency order
        for (String filePath: v2_2Files) {
            if (Strings.isNullOrEmpty(filePath))
                continue;

            String fName = FilenameUtils.getName(filePath);
            String fileType = identifyFileType(fName);

            if (fileType.equalsIgnoreCase("ENCNT")) {
                //call into 2.2 encounter transform
                ENCNT parser = new ENCNT(serviceId, systemId, exchangeId, version, filePath);
                ENCNTTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("DIAGN")) {
                //call into 2.2 diagnosis transform
                DIAGN parser = new DIAGN(serviceId, systemId, exchangeId, version, filePath);
                DIAGNTransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("PROCE")) {
                //call into 2.2 procedure transform
                PROCE parser = new PROCE(serviceId, systemId, exchangeId, version, filePath);
                PROCETransformer.transform(version, parser, fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else if (fileType.equalsIgnoreCase("CLEVE")) {
                //call into 2.2 clinical events transform
                CLEVE parser = new CLEVE(serviceId, systemId, exchangeId, version, filePath);
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
    }*/

    private static String identifyFileType(String filename) throws TransformException {
        String[] parts = filename.split("_");
        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];

        if (filenamePart1.equalsIgnoreCase("PC")) {
            // Bulk
            if (filenamePart2.equalsIgnoreCase("PROBLEMS")) {
                return "BULKPROBLEMS";
            } else if (filenamePart2.equalsIgnoreCase("DIAGNOSES")) {
                return "BULKDIAGNOSES";
            } else if (filenamePart2.equalsIgnoreCase("PROCEDURES")) {
                return "BULKPROCEDURES";
            } else {
                //if we have an unknown file this should be raised as an error
                throw new TransformException("Unknown file type for " + filename);
                //return "UNKNOWN";
            }
        } else if (filenamePart1.equalsIgnoreCase("susopa")) {
            return "SUSOPA";
        } else if (filenamePart1.equalsIgnoreCase("susaea")) {
            return "SUSAEA";
        } else if (filenamePart1.equalsIgnoreCase("tailopa")) {
            return filenamePart1.toUpperCase();
        } else if (filenamePart1.equalsIgnoreCase("tailaea")) {
            return filenamePart1.toUpperCase();
        } else if (filenamePart1.equalsIgnoreCase("spfit")) {
            return "SPFIT";
        } else if (filenamePart1.equalsIgnoreCase("cc")) {
            return "CC";
        } else if (filenamePart1.equalsIgnoreCase("hdb")) {
            return "HDB";

            // v2.2 files
        } else if (filenamePart1.equalsIgnoreCase("PPATI")
            || filenamePart1.equalsIgnoreCase("PPREL")
            || filenamePart1.equalsIgnoreCase("CDSEV")
            || filenamePart1.equalsIgnoreCase("PPATH")
            || filenamePart1.equalsIgnoreCase("RTTPE")
            || filenamePart1.equalsIgnoreCase("AEATT")
            || filenamePart1.equalsIgnoreCase("AEINV")
            || filenamePart1.equalsIgnoreCase("AETRE")
            || filenamePart1.equalsIgnoreCase("OPREF")
            || filenamePart1.equalsIgnoreCase("OPATT")
            || filenamePart1.equalsIgnoreCase("EALEN")
            || filenamePart1.equalsIgnoreCase("EALSU")
            || filenamePart1.equalsIgnoreCase("EALOF")
            || filenamePart1.equalsIgnoreCase("HPSSP")
            || filenamePart1.equalsIgnoreCase("IPEPI")
            || filenamePart1.equalsIgnoreCase("IPWDS")
            || filenamePart1.equalsIgnoreCase("DELIV")
            || filenamePart1.equalsIgnoreCase("BIRTH")
            || filenamePart1.equalsIgnoreCase("SCHAC")
            || filenamePart1.equalsIgnoreCase("APPSL")
            || filenamePart1.equalsIgnoreCase("DIAGN")
            || filenamePart1.equalsIgnoreCase("PROCE")
            || filenamePart1.equalsIgnoreCase("ORDER")
            || filenamePart1.equalsIgnoreCase("DOCRP")
            || filenamePart1.equalsIgnoreCase("DOCREF")
            || filenamePart1.equalsIgnoreCase("CNTRQ")
            || filenamePart1.equalsIgnoreCase("LETRS")
            || filenamePart1.equalsIgnoreCase("LOREF")
            || filenamePart1.equalsIgnoreCase("ORGREF")
            || filenamePart1.equalsIgnoreCase("PRSNLREF")
            || filenamePart1.equalsIgnoreCase("CVREF")
            || filenamePart1.equalsIgnoreCase("NOMREF")
            || filenamePart1.equalsIgnoreCase("EALIP")
            || filenamePart1.equalsIgnoreCase("CLEVE")
            || filenamePart1.equalsIgnoreCase("ENCNT")
            || filenamePart1.equalsIgnoreCase("RESREF")
            || filenamePart1.equalsIgnoreCase("PPNAM")
            || filenamePart1.equalsIgnoreCase("PPADD")
            || filenamePart1.equalsIgnoreCase("PPPHO")
            || filenamePart1.equalsIgnoreCase("PPALI")
            || filenamePart1.equalsIgnoreCase("PPINF")
            || filenamePart1.equalsIgnoreCase("PPAGP")
            || filenamePart1.equalsIgnoreCase("SURCC")
            || filenamePart1.equalsIgnoreCase("SURCP")
            || filenamePart1.equalsIgnoreCase("SURCA")
            || filenamePart1.equalsIgnoreCase("SURCD")
            || filenamePart1.equalsIgnoreCase("PDRES")
            || filenamePart1.equalsIgnoreCase("PDREF")
            || filenamePart1.equalsIgnoreCase("ABREF")
            || filenamePart1.equalsIgnoreCase("CEPRS")
            || filenamePart1.equalsIgnoreCase("ORDDT")
            || filenamePart1.equalsIgnoreCase("STATREF")
            || filenamePart1.equalsIgnoreCase("STATA")
            || filenamePart1.equalsIgnoreCase("ENCINF")
            || filenamePart1.equalsIgnoreCase("SCHDETAIL")
            || filenamePart1.equalsIgnoreCase("SCHOFFER")
            || filenamePart1.equalsIgnoreCase("PPGPORG")) {

            return filenamePart1.toUpperCase();
        } else {
            String filenamePart3 = parts[2];

            if (filenamePart1.equalsIgnoreCase("tailip")) {
                return filenamePart1.toUpperCase();
            } else {
                if (filenamePart1.equalsIgnoreCase("ip")) {
                    return "SUSIP";
                } else if (filenamePart1.equalsIgnoreCase("rnj")) {
                    return filenamePart3.toUpperCase();
                } else {
                    if (filenamePart1.equalsIgnoreCase("GETL")) {
                        return filenamePart3.toUpperCase();
                    } else {
                        //if we have an unknown file this should be raised as an error
                        throw new TransformException("Unknown file type for " + filename);
                        //return "UNKNOWN";
                    }
                }
            }
        }
    }
}
