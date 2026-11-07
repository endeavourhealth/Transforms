package org.endeavourhealth.transform.nwlextracts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.nwlextracts.transforms.*;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class NwlExtractsCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(NwlExtractsCsvToFhirTransformer.class);

    public static final String DATE_FORMAT = "dd/MM/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";

    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
                                                .withHeader()
                                                .withDelimiter('|')
                                                .withEscape((Character)null)
                                                .withQuote((Character)null)
                                                .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mode NONE, but validation in the library means we need to use this

    public static final String IMPERIALS_RESOURCE_ID_SCOPE = "H";

    public static void transform(Exchange exchange, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        String exchangeBody = exchange.getBody();
        String sourceSystem= exchange.getHeader("SenderLocalIdentifier");

        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        UUID serviceId = fhirResourceFiler.getServiceId();
        Service service = DalProvider.factoryServiceDal().getById(serviceId);
        ExchangeHelper.filterFileTypes(files, service, fhirResourceFiler.getExchangeId());
        LOG.info("Invoking NwlExtracts CSV transformer for " + files.size() + " files for service " + service.getName() + " " + service.getId());

        //if filtering by file type, we may end up with exchanges containing no files
        if (files.isEmpty()) {
            LOG.info("No files, so returning out");
            return;
        }

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String exchangeDirectory = ExchangePayloadFile.validateFilesAreInSameDirectory(files);
        LOG.trace("Transforming NwlExtracts CSV content in " + exchangeDirectory);

        //config as to whether we only process the Encounters within CDS files
        if (TransformConfig.instance().isCernerCDSEncountersOnly()) {
            LOG.debug("**    CDS Encounters Only config applied    **");
        }

        NwlExtractsCsvHelper csvHelper = new NwlExtractsCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId(), sourceSystem, version);

        Map<String, List<ParserI>> parserMap = hashFilesByType(files, exchangeDirectory, csvHelper);

        try {
            //admin transformers
            LOG.trace("Starting admin transformers");
            ORGREFPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "ORGREF", false), fhirResourceFiler, csvHelper);
            ORGREFTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "ORGREF", false), fhirResourceFiler, csvHelper);
            CVREFTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "CVREF", true), fhirResourceFiler, csvHelper,sourceSystem);
            LOREFTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "LOREF", true), fhirResourceFiler, csvHelper,sourceSystem);
            csvHelper.getLocationCache().fileLocationResources(fhirResourceFiler);

            PRSNLREFPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PRSNLREF", false), fhirResourceFiler, csvHelper); //saves mappings in multiple threads
            PRSNLREFTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PRSNLREF", false), fhirResourceFiler, csvHelper); //updates/creates FHIR practitioners

            //patient PRE transformers - to save and cache stuff fast (mostly PPxxx ID to Person ID mappings)
            LOG.trace("Starting PPxxx pre-transformers");
            PPATIPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPATI", false), fhirResourceFiler, csvHelper,sourceSystem);
            csvHelper.waitUntilThreadPoolIsEmpty();

            PPADDPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPADD", false), fhirResourceFiler, csvHelper,sourceSystem);
            PPNAMPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPNAM", false), fhirResourceFiler, csvHelper,sourceSystem);
            PPPHOPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPPHO", false), fhirResourceFiler, csvHelper,sourceSystem);


            //let all the pre-transformers finish pre-loading the Patient resources
            csvHelper.waitUntilThreadPoolIsEmpty();

            //patient transformers
            LOG.trace("Starting PPxxx transformers");
            PPATITransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPATI", false), fhirResourceFiler, csvHelper,sourceSystem);
            PPADDTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPADD", false), fhirResourceFiler, csvHelper,sourceSystem);
            PPNAMTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPNAM", false), fhirResourceFiler, csvHelper,sourceSystem);
            PPPHOTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPPHO", true), fhirResourceFiler, csvHelper,sourceSystem);
            PPAGPTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPAGP", false), fhirResourceFiler, csvHelper,sourceSystem);

            //we're now good to save our patient resources
            csvHelper.getPatientCache().filePatientResources(fhirResourceFiler);

            //subsequent transforms may refer to Patient resources, so ensure they're all on the DB before continuing
            fhirResourceFiler.waitUntilEverythingIsSaved();

            //pre-transformers, must be done before encounter ones
            CLEVEPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "CLEVE", false), fhirResourceFiler, csvHelper);

            csvHelper.waitUntilThreadPoolIsEmpty();

           //Processing PPATI file again to transform the religion and language as an Observation attribute for the Patient, that is to replicate the HL7 ADT feed.
            ObservationTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "PPATI", false), fhirResourceFiler, csvHelper,sourceSystem);

            /*CLINICAL EVENTS - execute the staging procedures to target clinical events stored proc only if the CLEVE file is present
                this is the last time we reference the CLEVE file if present in the file list, so remove from the parser map*/
            if (!getParsers(parserMap, csvHelper, fhirResourceFiler, "CLEVE", true).isEmpty()) {

                csvHelper.processStagingForTargetClinicalEvents();

                //Clinical events data transformation on final clinical events target staging table
                ClinicalEventTargetTransformer.transform(fhirResourceFiler, csvHelper);
            }

            // Encounter Pre Transformer
            ENCNTPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "ENCNT", false), fhirResourceFiler, csvHelper);
            fhirResourceFiler.waitUntilEverythingIsSaved(); //the above may update existing resources, so let it fully finish

            // Encounter  Transformer
            ENCNTTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "ENCNT", false), fhirResourceFiler, csvHelper,sourceSystem);
            csvHelper.getEncounterCache().fileEncounterResources(fhirResourceFiler);
          //  csvHelper.getEpisodeOfCareCache().fileResources(fhirResourceFiler);

            //subsequent transforms may refer to Encounter resources, so ensure they're all on the DB before continuing
            fhirResourceFiler.waitUntilEverythingIsSaved();

            csvHelper.waitUntilThreadPoolIsEmpty();

            EpisodeOfCareTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "ENCNT", false), fhirResourceFiler, csvHelper,sourceSystem);

            //DIAGNOSES / PROBLEMS - NOTE:  Any Sus Diagnosis data will have been processed during the transforms above, i.e. Inpatient, Outpatient
            DIAGNPreTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "DIAGN", true), fhirResourceFiler, csvHelper);

            //CONDITIONS - execute the staging conditions to target procedures stored proc - ONLY if the CDS Encounters only config filter is NOT set
            csvHelper.waitUntilThreadPoolIsEmpty();
            if (!TransformConfig.instance().isCernerCDSEncountersOnly()) {

                //Condition data transform from staging to target
                csvHelper.processStagingForTargetConditions();

                //Condition data transformation on final condition target staging table
                ConditionTargetTransformer.transform(fhirResourceFiler, csvHelper);
            } 

            fhirResourceFiler.waitUntilEverythingIsSaved();

            //if we've got any updates to existing resources that haven't been handled in an above transform, apply them now
            csvHelper.processRemainingClinicalEventParentChildLinks(fhirResourceFiler);
            csvHelper.processRemainingNewConsultationRelationships(fhirResourceFiler);

        } finally {

            //and stop the thread pool
            csvHelper.stopThreadPool();
        }
    }
    /**
     * finds parsers for the given file type on any matching files
     */
    private static List<ParserI> getParsers(Map<String, List<ParserI>> parserMap, NwlExtractsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, String type, boolean removeFromMap) throws Exception {

        //not strictly to do with getting parsers, but before we start any new transform, we should
        //ensure that everything in the thread pool is done
        csvHelper.waitUntilThreadPoolIsEmpty();

        //and if we had any errors on the previous file, we should bomb out now
        fhirResourceFiler.failIfAnyErrors();

        List<ParserI> ret = null;

        if (removeFromMap) {
            /*if removeFromMap is true, it means that this is the last time we'll need the parsers, to remove from the map and allow them to be garbage collected when we're done*/
            ret = parserMap.remove(type);
            
        } else {
            ret = parserMap.get(type);
        }

        if (ret == null) {
            ret = new ArrayList<>();
        }

        return ret;
    }

    @SuppressWarnings("unchecked")
    private static ParserI createParser(String filePath, String type, NwlExtractsCsvHelper csvHelper) throws Exception {

        UUID serviceId = csvHelper.getServiceId();
        UUID systemId = csvHelper.getSystemId();
        UUID exchangeId = csvHelper.getExchangeId();
        String version = csvHelper.getVersion();

        try {

            String clsName = "org.endeavourhealth.transform.nwlextracts.schema." + type;
            Class cls = Class.forName(clsName);

            //now construct an instance of the parser for the file we've found
            Constructor<AbstractCsvParser> constructor = cls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
            return constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);

        } catch (ClassNotFoundException cnfe) {
            throw new TransformException("No parser for file type [" + type + "]");
        }
    }

    private static Map<String, List<ParserI>> hashFilesByType(List<ExchangePayloadFile> files, String exchangeDirectory, NwlExtractsCsvHelper csvHelper) throws Exception {

        /* The exchange directory name is the date of the batch, which we need to use
            to work out whether to ignore deltas of files we later received bulks for*/
        String exchangeDirectoryName = FilenameUtils.getBaseName(exchangeDirectory);
        Date exchangeDate = new SimpleDateFormat("yyyy-MM-dd").parse(exchangeDirectoryName); //date of current exchange
        Date bulkDate = new SimpleDateFormat("yyyy-MM-dd").parse("2016-03-09"); //date the bulks were generated
        Date bulkProcessingDate = new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-03"); //exchange date the bulks were processed with
        boolean ignoreDeltasProcessedOutOfOrder = exchangeDate.before(bulkDate)
                                            && exchangeDate.after(bulkProcessingDate);

        /* NOTE: we've had bulks for other file types, but only need to ignore extracts where the bulk was processed out of order
            e.g. the DIAGN bulk file will be processed according to the date it was actually generated, but the below
            files will be processed as though they were received in 2017 even though they came from Mar 2018*/
        HashSet<String> fileTypesBulked = new HashSet<>();

        fileTypesBulked.add("PPNAM");
        fileTypesBulked.add("PPPHO");
        fileTypesBulked.add("PPATI");

        Map<String, List<ParserI>> ret = new HashMap<>();
        
        for (ExchangePayloadFile fileObj: files) {

            String file = fileObj.getPath();
            String type = fileObj.getType();

            ParserI parser = createParser(file, type, csvHelper);

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

}
