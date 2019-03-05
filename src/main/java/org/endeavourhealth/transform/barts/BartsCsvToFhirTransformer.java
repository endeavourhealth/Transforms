package org.endeavourhealth.transform.barts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.transforms.*;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class BartsCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvToFhirTransformer.class);

    //public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT = "dd/MM/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";

    public static final String CDS_DATE_FORMAT = "yyyyMMdd";
    public static final String CDS_TIME_FORMAT = "hhmmss";

    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
                                                .withHeader()
                                                .withDelimiter('|')
                                                .withEscape((Character)null)
                                                .withQuote((Character)null)
                                                .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mdde NONE, but validation in the library means we need to use this

    /*public static final CSVFormat CSV_FORMAT_NO_HEADER = CSVFormat.DEFAULT
            .withDelimiter('|')
            .withEscape((Character)null)
            .withQuote((Character)null)
            .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mdde NONE, but validation in the library means we need to use this*/

    //public static final String PRIMARY_ORG_ODS_CODE = "R1H";
    public static final String PRIMARY_ORG_ODS_CODE = "RNJ"; //internally in the data Barts use RNJ as their ODS code
    public static final String PRIMARY_ORG_HL7_OID = "2.16.840.1.113883.3.2540.1";
    public static final String BARTS_RESOURCE_ID_SCOPE = "B";

    //public static final int VERSION_2_2_FILE_COUNT = 14;

    public static void transform(String exchangeBody, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        LOG.info("Invoking Barts CSV transformer for " + files.size() + " files using and service " + fhirResourceFiler.getServiceId());

        //separate out the bulk cleve files
        /*List<ExchangePayloadFile> cleveBulks = new ArrayList<>();
        for (int i=files.size()-1; i>=0; i--) {
            ExchangePayloadFile f = files.get(i);
            if (f.getType().equals("CLEVE")) {
                String path = f.getPath();
                if (path.indexOf("999999") > -1) {
                    files.remove(i);
                    cleveBulks.add(f);
                }
            }
        }*/

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String exchangeDirectory = ExchangePayloadFile.validateFilesAreInSameDirectory(files);
        LOG.trace("Transforming Barts CSV content in " + exchangeDirectory);

        BartsCsvHelper csvHelper = new BartsCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId(), PRIMARY_ORG_HL7_OID, version);

        /*transformAdminAndPatientParsers(serviceId, systemId, exchangeId, files, version, fhirResourceFiler, csvHelper, previousErrors);
        transformClinicalParsers(serviceId, systemId, exchangeId, files, version, fhirResourceFiler, csvHelper, previousErrors);*/

        Map<String, List<ParserI>> parserMap = hashFilesByType(files, exchangeDirectory, csvHelper);

        try {
            //admin transformers
            ORGREFPreTransformer.transform(getParsers(parserMap, "ORGREF", false), fhirResourceFiler, csvHelper);
            ORGREFTransformer.transform(getParsers(parserMap, "ORGREF", true), fhirResourceFiler, csvHelper);
            CVREFTransformer.transform(getParsers(parserMap, "CVREF", true), fhirResourceFiler, csvHelper);
            NOMREFTransformer.transform(getParsers(parserMap, "NOMREF", true), fhirResourceFiler, csvHelper);
            LOREFTransformer.transform(getParsers(parserMap, "LOREF", true), fhirResourceFiler, csvHelper);
            csvHelper.getLocationCache().fileLocationResources(fhirResourceFiler);
            PRSNLREFTransformer.transform(getParsers(parserMap, "PRSNLREF", true), fhirResourceFiler, csvHelper);

            //patient PRE transformers - to cache stuff fast
            PPALIPreTransformer.transform(getParsers(parserMap, "PPALI", false), fhirResourceFiler, csvHelper); //this must be FIRST
            PPADDPreTransformer.transform(getParsers(parserMap, "PPADD", false), fhirResourceFiler, csvHelper);
            PPNAMPreTransformer.transform(getParsers(parserMap, "PPNAM", false), fhirResourceFiler, csvHelper);
            PPPHOPreTransformer.transform(getParsers(parserMap, "PPPHO", false), fhirResourceFiler, csvHelper);
            PPRELPreTransformer.transform(getParsers(parserMap, "PPREL", false), fhirResourceFiler, csvHelper);

            //patient transformers
            PPATITransformer.transform(getParsers(parserMap, "PPATI", true), fhirResourceFiler, csvHelper);
            PPALITransformer.transform(getParsers(parserMap, "PPALI", true), fhirResourceFiler, csvHelper);
            PPADDTransformer.transform(getParsers(parserMap, "PPADD", true), fhirResourceFiler, csvHelper);
            //PPINFTransformer.transform(getParsers(parserMap, "PPINF", csvHelper), fhirResourceFiler, csvHelper);
            PPNAMTransformer.transform(getParsers(parserMap, "PPNAM", true), fhirResourceFiler, csvHelper);
            PPPHOTransformer.transform(getParsers(parserMap, "PPPHO", true), fhirResourceFiler, csvHelper);
            PPRELTransformer.transform(getParsers(parserMap, "PPREL", true), fhirResourceFiler, csvHelper);
            PPAGPTransformer.transform(getParsers(parserMap, "PPAGP", true), fhirResourceFiler, csvHelper);

            //we're now good to save our patient resources
            csvHelper.getPatientCache().filePatientResources(fhirResourceFiler);

            //subsequent transforms may refer to Patient resources, so ensure they're all on the DB before continuing
            fhirResourceFiler.waitUntilEverythingIsSaved();

            //pre-transformers, must be done before encounter ones
            CLEVEPreTransformer.transform(getParsers(parserMap, "CLEVE", false), fhirResourceFiler, csvHelper);
            DIAGNPreTransformer.transform(getParsers(parserMap, "DIAGN", false), fhirResourceFiler, csvHelper);
            PROCEPreTransformer.transform(getParsers(parserMap, "PROCE", false), fhirResourceFiler, csvHelper);

            ENCNTPreTransformer.transform(getParsers(parserMap, "ENCNT", false), fhirResourceFiler, csvHelper);

            // Encounters - Doing ENCNT first to try and create as many Ecnounter->EoC links as possible in cache
            ENCNTTransformer.transform(getParsers(parserMap, "ENCNT", true), fhirResourceFiler, csvHelper);
            AEATTTransformer.transform(getParsers(parserMap, "AEATT", true), fhirResourceFiler, csvHelper);
            IPEPITransformer.transform(getParsers(parserMap, "IPEPI", true), fhirResourceFiler, csvHelper);
            IPWDSTransformer.transform(getParsers(parserMap, "IPWDS", true), fhirResourceFiler, csvHelper);
            OPATTTransformer.transform(getParsers(parserMap, "OPATT", true), fhirResourceFiler, csvHelper);
            //ENCINFTransformer.transform(getParsers(parserMap, "ENCINF", csvHelper), fhirResourceFiler, csvHelper);

            csvHelper.getEncounterCache().fileEncounterResources(fhirResourceFiler);
            csvHelper.getEpisodeOfCareCache().fileResources(fhirResourceFiler);

            //subsequent transforms may refer to Encounter resources, so ensure they're all on the DB before continuing
            fhirResourceFiler.waitUntilEverythingIsSaved();

            //PROCEDURES - the order is significant, going from less to more rich files
            SusInpatientTailPreTransformer.transform(getParsers(parserMap, "SusInpatientTail", true), fhirResourceFiler,csvHelper);
            SusOutpatientTailPreTransformer.transform(getParsers(parserMap, "SusOutpatientTail", true), fhirResourceFiler,csvHelper);
            SusInpatientPreTransformer.transform(getParsers(parserMap, "SusInpatient", true), fhirResourceFiler,csvHelper);
            SusOutpatientPreTransformer.transform(getParsers(parserMap, "SusOutpatient", true), fhirResourceFiler,csvHelper);

            ProcedurePreTransformer.transform(getParsers(parserMap, "Procedure", true), fhirResourceFiler, csvHelper);
            PROCETransformer.transform(getParsers(parserMap, "PROCE", true), fhirResourceFiler, csvHelper);

            fhirResourceFiler.waitUntilEverythingIsSaved();

            //DIAGNOSES
            /*
            DiagnosisTransformer.transform(version, getParsers(parserMap, "Diagnosis", csvHelper), fhirResourceFiler, csvHelper, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
            SusEmergencyTransformer.transformProcedures(getParsers(parserMap, "SusEmergency", csvHelper, false), fhirResourceFiler, csvHelper, fileMap);
            SusEmergencyCareDataSetTransformer.transformProcedures(getParsers(parserMap, "SusEmergencyCareDataSet", csvHelper, false), fhirResourceFiler, csvHelper, fileMap);
            DIAGNTransformer.transform(getParsers(parserMap, "DIAGN", csvHelper, true), fhirResourceFiler, csvHelper);
            save all diagnoses to the DB
            fhirResourceFiler.waitUntilEverythingIsSaved();
            */

            //other clinical transformers
            CLEVETransformer.transform(getParsers(parserMap, "CLEVE", true), fhirResourceFiler, csvHelper);
            ProblemTransformer.transform(getParsers(parserMap, "Problem", true), fhirResourceFiler, csvHelper);
            FamilyHistoryTransformer.transform(getParsers(parserMap, "FamilyHistory", true), fhirResourceFiler, csvHelper);

            //if we've got any updates to existing resources that haven't been handled in an above transform, apply them now
            csvHelper.processRemainingClinicalEventParentChildLinks(fhirResourceFiler);
            csvHelper.processRemainingNewConsultationRelationships(fhirResourceFiler);

            /*LOG.debug("Doing CLEVE bulks now");
            for (ExchangePayloadFile bulk: cleveBulks) {
                String path = bulk.getPath();
                LOG.debug("Doing " + path);

                //these are done and we don't need to reprocess them
                String name = FilenameUtils.getBaseName(path);
                if (name.endsWith("_55")
                        || name.endsWith("_54")
                        || name.endsWith("_53")
                        || name.endsWith("_52")
                        || name.endsWith("_51")
                        || name.endsWith("_50")
                        || name.endsWith("_49")
                        || name.endsWith("_48")
                        || name.endsWith("_47")
                        || name.endsWith("_46")
                        || name.endsWith("_45")
                        || name.endsWith("_44")
                        || name.endsWith("_43")
                        || name.endsWith("_42")
                        || name.endsWith("_41")
                        || name.endsWith("_40")
                        || name.endsWith("_39")
                        || name.endsWith("_38")
                        || name.endsWith("_37")
                        || name.endsWith("_36")
                        || name.endsWith("_35")
                        || name.endsWith("_34")
                        || name.endsWith("_33")
                        || name.endsWith("_32")
                        || name.endsWith("_31")
                        || name.endsWith("_30")
                        || name.endsWith("_29")
                        || name.endsWith("_28")
                        || name.endsWith("_27")
                        || name.endsWith("_26")
                        || name.endsWith("_25")
                        || name.endsWith("_24")
                        || name.endsWith("_23")
                        || name.endsWith("_22")
                        || name.endsWith("_21")
                        || name.endsWith("_20")
                        || name.endsWith("_19")
                        || name.endsWith("_18")
                        || name.endsWith("_17")
                        || name.endsWith("_16")
                        || name.endsWith("_15")
                        || name.endsWith("_14")


                        ) {
                    continue;
                }

                List<ParserI> parsers = new ArrayList<>();
                CLEVE parser = new CLEVE(serviceId, systemId, exchangeId, version, bulk.getPath());
                parsers.add(parser);

                CLEVEPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
                CLEVETransformer.transform(parsers, fhirResourceFiler, csvHelper);

                csvHelper.processRemainingClinicalEventParentChildLinks(fhirResourceFiler);
                csvHelper.processRemainingNewConsultationRelationships(fhirResourceFiler);
            }*/

        } finally {

            //and stop the thread pool
            csvHelper.stopThreadPool();
        }
    }
    /**
     * finds parsers for the given file type on any matching files
     */
    private static List<ParserI> getParsers(Map<String, List<ParserI>> parserMap, String type, boolean removeFromMap) throws Exception {

        List<ParserI> ret = null;

        if (removeFromMap) {
            //if removeFromMap is true, it means that this is the last time
            //we'll need the parsers, to remove from the map and allow them to be garbage collected when we're done
            ret = parserMap.remove(type);
            
        } else {
            ret = parserMap.get(type);
        }

        if (ret == null) {
            ret = new ArrayList<>();
        }

        return ret;
    }

    private static ParserI createParser(String filePath, String type, BartsCsvHelper csvHelper) throws Exception {

        UUID serviceId = csvHelper.getServiceId();
        UUID systemId = csvHelper.getSystemId();
        UUID exchangeId = csvHelper.getExchangeId();
        String version = csvHelper.getVersion();

        try {

            String clsName = "org.endeavourhealth.transform.barts.schema." + type;
            Class cls = Class.forName(clsName);

            //now construct an instance of the parser for the file we've found
            Constructor<AbstractCsvParser> constructor = cls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
            return constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);

        } catch (ClassNotFoundException cnfe) {
            throw new TransformException("No parser for file type [" + type + "]");
        }
    }



    private static Map<String, List<ParserI>> hashFilesByType(List<ExchangePayloadFile> files, String exchangeDirectory, BartsCsvHelper csvHelper) throws Exception {

        //the exchange directory name is the date of the batch, which we need to use
        //to work out whether to ignore deltas of files we later received bulks for
        String exchangeDirectoryName = FilenameUtils.getBaseName(exchangeDirectory);
        Date exchangeDate = new SimpleDateFormat("yyyy-MM-dd").parse(exchangeDirectoryName); //date of current exchange
        Date bulkDate = new SimpleDateFormat("yyyy-MM-dd").parse("2018-03-09"); //date the bulks were generated
        Date bulkProcessingDate = new SimpleDateFormat("yyyy-MM-dd").parse("2017-12-03"); //exchange date the bulks were processed with
        boolean ignoreDeltasProcessedOutOfOrder = exchangeDate.before(bulkDate)
                                            && exchangeDate.after(bulkProcessingDate);

        //NOTE: we've had bulks for other file types, but only need to ignore extracts where
        //the bulk was processed out of order e.g. the DIAGN bulk
        //file will be processed according to the date it was actually generated, but the below
        //files will be processed as though they were received in 2017 even though they came from Mar 2018
        HashSet<String> fileTypesBulked = new HashSet<>();
        /*fileTypesBulked.add("ENCNT");
        fileTypesBulked.add("ENCINF");
        fileTypesBulked.add("OPATT");
        fileTypesBulked.add("AEATT");
        fileTypesBulked.add("IPEPI");
        fileTypesBulked.add("IPWDS");*/
        fileTypesBulked.add("PPNAM");
        fileTypesBulked.add("PPPHO");
        fileTypesBulked.add("PPALI");
        fileTypesBulked.add("PPREL");
        fileTypesBulked.add("PPATI");

        Map<String, List<ParserI>> ret = new HashMap<>();
        
        for (ExchangePayloadFile fileObj: files) {

            String file = fileObj.getPath();
            String type = fileObj.getType();

            ParserI parser = createParser(file, type, csvHelper);

            /*String fileName = FilenameUtils.getBaseName(file);
            String type = identifyFileType(fileName);

            //always force into upper case, just in case
            type = type.toUpperCase();*/

            //we might want to ignore this file if it's before a known bulk
            if (ignoreDeltasProcessedOutOfOrder
                    && fileTypesBulked.contains(type)) {
                LOG.info("Skipping " + file + " as it's a delta from before the bulks");
                continue;
            }

            List<ParserI> list = ret.get(type);
            if (list == null) {
                list = new ArrayList<>();
                ret.put(type, list);
            }
            list.add(parser);
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

    /**
     * finds the tails file for the given name
     */
    public static ParserI findTailFile(Map<String, List<ParserI>> parserMap, String fileType, String mainFilePath) throws TransformException {

        List<ParserI> parsersForType = parserMap.get(fileType);
        if (parsersForType == null) {
            throw new TransformException("Failed to find any files of type " + fileType + " in file map");
        }

        //the tails file and mail file have different naming conventions but both contain the same numeric element
        String mainFileName = FilenameUtils.getName(mainFilePath);
        int numericElement = findNumericElement(mainFileName);

        for (ParserI parser: parsersForType) {
            String path = parser.getFilePath();
            String tailName = FilenameUtils.getName(path);
            int tailNumericElement = findNumericElement(tailName);
            if (tailNumericElement == numericElement) {
                return parser;
            }
        }

        throw new TransformException("Failed to find " + fileType + " file for " + mainFileName);
    }

    private static int findNumericElement(String str) {
        StringBuffer sb = new StringBuffer();

        for (int i=0; i<str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isDigit(c)) {
                sb.append(c);
            }
        }

        String s = sb.toString();
        return Integer.parseInt(s);
    }

    /*public static String identifyFileType(String filename) throws TransformException {

        String[] parts = filename.split("_");
        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];

        if (filenamePart1.equalsIgnoreCase("PC")) {
            // Bulk
            if (filenamePart2.equalsIgnoreCase("PROBLEMS")) {
                return "BulkProblem";
            } else if (filenamePart2.equalsIgnoreCase("DIAGNOSES")) {
                return "BulkDiagnosis";
            } else if (filenamePart2.equalsIgnoreCase("PROCEDURES")) {
                return "BulkProcedure";
            } else {
                //if we have an unknown file this should be raised as an error
                throw new TransformException("Unknown file type for " + filename);
                //return "UNKNOWN";
            }
        } else if (filenamePart1.equalsIgnoreCase("rnj")) {
            // Bulk
            String filenamePart3 = parts[2];
            if (filenamePart3.equalsIgnoreCase("prob")) {
                return "Problem";
            } else if (filenamePart3.equalsIgnoreCase("proc")) {
                return "Procedure";
            } else if (filenamePart3.equalsIgnoreCase("diag")) {
                return "Diagnosis";
            } else {
                //if we have an unknown file this should be raised as an error
                throw new TransformException("Unknown file type for " + filename);
                //return "UNKNOWN";
            }

        } else if (filenamePart1.equalsIgnoreCase("susopa")) {
            return "SusOutpatient";
        } else if (filenamePart1.equalsIgnoreCase("susaea")) {
            return "SusEmergency";
        } else if (filenamePart1.equalsIgnoreCase("ip")) {
            return "SusInpatient";
        } else if (filenamePart1.equalsIgnoreCase("tailip")) {
            return "SusOutpatientTail";
        } else if (filenamePart1.equalsIgnoreCase("tailopa")) {
            return "SusEmergencyTail";
        } else if (filenamePart1.equalsIgnoreCase("tailaea")) {
            return "SusInpatientTail";
        } else if (filenamePart1.equalsIgnoreCase("spfit")) {
            return "SPFIT";
        } else if (filenamePart1.equalsIgnoreCase("cc")) {
            return "CriticalCare";
        } else if (filenamePart1.equalsIgnoreCase("hdb")) {
            return "HomeDeliveryAndBirth";
        } else if (filenamePart1.equalsIgnoreCase("susecd")) {
            return "SusEmergencyCareDataSet";
        } else if (filenamePart1.equalsIgnoreCase("tailecd")) {
            return "SusEmergencyCareDataSetTail";

        } else if (filenamePart1.equalsIgnoreCase("fam")
                && filenamePart2.equalsIgnoreCase("hist")) {
            return "FamilyHistory";

        } else if (filenamePart1.equalsIgnoreCase("GETL")) {
            if (parts.length == 6) {
                String tok3 = parts[2];
                if (tok3.equalsIgnoreCase("PREG")) {
                    return "Pregnancy";

                } else if (tok3.equalsIgnoreCase("BIRTH")) {
                    return "Birth";

                } else {
                    throw new TransformException("Unexpected third element " + tok3 + " after GETL in filename [" + filename + "]");
                }

            } else if (parts.length == 7) {
                String tok2 = parts[1];
                if (tok2.equals("APPSL2")) {
                    return "APPSL2";

                } else {
                    throw new TransformException("Unexpected second element " + tok2 + " after GETL in filename [" + filename + "]");
                }

            } else {
                throw new TransformException("Unexpected number of elements after GETL in filename [" + filename + "]");
            }

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
            throw new TransformException("Unknown file type for " + filename);
        }
    }*/
}
