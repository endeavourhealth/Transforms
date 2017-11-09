package org.endeavourhealth.transform.vision;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.ExchangeDalI;
import org.endeavourhealth.core.xml.TransformErrorUtility;
import org.endeavourhealth.core.xml.transformError.Error;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.schema.*;
import org.endeavourhealth.transform.vision.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.stream.Collectors;

public abstract class VisionCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(VisionCsvToFhirTransformer.class);

    public static final String VERSION_0_18 = "0.18";

    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd"; //EMIS spec says "dd/MM/yyyy", but test data is different
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180;  //. DEFAULT;

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String sharedStoragePath, int maxFilingThreads, String version) throws Exception {

        //the exchange body will be a list of files received
        //split by /n but trim each one, in case there's a sneaky /r in there
        String[] files = exchangeBody.split("\n");
        for (int i=0; i<files.length; i++) {
            String file = files[i].trim();
            files[i] = file;
        }
        //String[] files = exchangeBody.split(java.lang.System.lineSeparator());

        LOG.info("Invoking Vision CSV transformer for {} files using {} threads and service {}", files.length, maxFilingThreads, serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        File orgDirectory = validateAndFindCommonDirectory(sharedStoragePath, files);



        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds, maxFilingThreads);

        Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            validateAndOpenParsers(orgDirectory, version, true, allParsers);

            LOG.trace("Transforming Vision CSV content in {}", orgDirectory);
            transformParsers(version, allParsers, processor, previousErrors, maxFilingThreads);

        } finally {

            closeParsers(allParsers.values());
        }

        LOG.trace("Completed transform for service {} - waiting for resources to commit to DB", serviceId);
        processor.waitToFinish();
    }


    private static void closeParsers(Collection<AbstractCsvParser> parsers) {
        for (AbstractCsvParser parser : parsers) {
            try {
                parser.close();
            } catch (IOException ex) {
                //don't worry if this fails, as we're done anyway
            }
        }
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


    private static void validateAndOpenParsers(File dir, String version, boolean openParser, Map<Class, AbstractCsvParser> parsers) throws Exception {
        //admin - practice
        //admin - staff
        //patient - demographics
        //clinical - encounters
        //clinical - journal
        //clinical - referrals

        findFileAndOpenParser(Practice.class, dir, version, openParser, parsers);
        findFileAndOpenParser(Staff.class, dir, version, openParser, parsers);

        findFileAndOpenParser(Patient.class, dir, version, openParser, parsers);

        findFileAndOpenParser(Encounter.class, dir, version, openParser, parsers);
        findFileAndOpenParser(Journal.class, dir, version, openParser, parsers);
        findFileAndOpenParser(Referral.class, dir, version, openParser, parsers);


        //then validate there are no extra, unexpected files in the folder, which would imply new data
        //Set<File> sh = new HashSet<>(parsers);

        Set<File> expectedFiles = parsers
                .values()
                .stream()
                .map(T -> T.getFile())
                .collect(Collectors.toSet());

        for (File file: dir.listFiles()) {
            if (file.isFile()
                    && !expectedFiles.contains(file)
                    && !Files.getFileExtension(file.getAbsolutePath()).equalsIgnoreCase("zip")) {

                throw new FileFormatException(file, "Unexpected file " + file + " in Vision CSV extract");
            }
        }
    }

    public static void findFileAndOpenParser(Class parserCls, File dir, String version, boolean openParser, Map<Class, AbstractCsvParser> ret) throws Exception {

        String name = parserCls.getSimpleName();

        for (File f: dir.listFiles()) {
            String fName = f.getName();

            //we're only interested in CSV files
            String extension = Files.getFileExtension(fName);
            if (!extension.equalsIgnoreCase("csv")) {
                continue;
            }

            //Vision files are format:  FULL_GMS123_encounter_data_extract-2014-09-23_165206.csv
            String[] toks = fName.split("_");
            if (toks.length != 6) {
                continue;
            }

            //No file matching the class, i.e. Encounter
            if (!toks[2].equalsIgnoreCase(name)) {
                continue;
            }

            //now construct an instance of the parser for the file we've found
            Constructor<AbstractCsvParser> constructor = parserCls.getConstructor(String.class, File.class, Boolean.TYPE);
            AbstractCsvParser parser = constructor.newInstance(version, f, openParser);

            ret.put(parserCls, parser);
            return;
        }

        throw new FileNotFoundException("Failed to find CSV file for " + name + " in " + dir);
    }


    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors,
                                         int maxFilingThreads) throws Exception {

        VisionCsvHelper csvHelper = new VisionCsvHelper();

        //if this is the first extract for this organisation, we need to apply all the content of the admin resource cache
        ExchangeDalI exchangeDal = DalProvider.factoryExchangeDal();
        if (!exchangeDal.isServiceStarted(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId())) {
            LOG.trace("Applying admin resource cache for service {} and system {}", fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId());
            csvHelper.applyAdminResourceCache(fhirResourceFiler);
        }

        //these transforms don't create resources themselves, but cache data that the subsequent ones rely on
//        ClinicalCodeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper, maxFilingThreads);
//        DrugCodeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper, maxFilingThreads);
//        OrganisationLocationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        SessionUserTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        ProblemPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //TODO: get consultation, problem links and cache - PARSE Entire Journal file for links
        //JournalPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        DrugRecordPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        IssueRecordPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//        DiaryPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
//
//        //before getting onto the files that actually create FHIR resources, we need to
//        //work out what record numbers to process, if we're re-running a transform
//        boolean processingSpecificRecords = findRecordsToProcess(parsers, previousErrors);

        //run the transforms for non-patient resources
        PracticeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        StaffTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //note the order of these transforms is important, as consultations should be before obs etc.
        PatientTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        EncounterTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        ReferralTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        JournalTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

//        if (!processingSpecificRecords) {
//
//            //if we have any new Obs, Conditions, Medication etc. that reference pre-existing parent obs or problems,
//            //then we need to retrieve the existing resources and update them
//            csvHelper.processRemainingObservationParentChildLinks(fhirResourceFiler);
//
//            //process any new items linked to past consultations
//            csvHelper.processRemainingConsultationRelationships(fhirResourceFiler);
//
//            //if we have any new Obs etc. that refer to pre-existing problems, we need to update the existing FHIR Problem
//            csvHelper.processRemainingProblemRelationships(fhirResourceFiler);
//
//            //if we have any changes to the staff in pre-existing sessions, we need to update the existing FHIR Schedules
//            csvHelper.processRemainingSessionPractitioners(fhirResourceFiler);
//
//            //process any changes to ethnicity or marital status, without a change to the Patient
//            csvHelper.processRemainingEthnicitiesAndMartialStatuses(fhirResourceFiler);
//
//            //process any changes to Org-Location links without a change to the Location itself
//            csvHelper.processRemainingOrganisationLocationMappings(fhirResourceFiler);
//
//            //process any changes to Problems that didn't have an associated Observation change too
//            csvHelper.processRemainingProblems(fhirResourceFiler);
//
//            //update any MedicationStatements to set the last issue date on them
//            csvHelper.processRemainingMedicationIssueDates(fhirResourceFiler);
//        }
    }


    public static boolean findRecordsToProcess(Map<Class, AbstractCsvParser> allParsers, TransformError previousErrors) throws Exception {

        boolean processingSpecificRecords = false;

        for (AbstractCsvParser parser: allParsers.values()) {

            String fileName = parser.getFile().getName();

            Set<Long> recordNumbers = findRecordNumbersToProcess(fileName, previousErrors);
            parser.setRecordNumbersToProcess(recordNumbers);

            //if we have a non-null set, then we're processing specific records in some file
            if (recordNumbers != null) {
                processingSpecificRecords = true;
            }
        }

        return processingSpecificRecords;
    }

    private static Set<Long> findRecordNumbersToProcess(String fileName, TransformError previousErrors) {

        //if we're running for the first time, then return null to process the full file
        if (previousErrors == null) {
            return null;
        }

        //if we previously had a fatal exception, then we want to process the full file
        if (TransformErrorUtility.containsArgument(previousErrors, TransformErrorUtility.ARG_FATAL_ERROR)) {
            return null;
        }

        //if we previously aborted due to errors in a previous exchange, then we want to process it all
        if (TransformErrorUtility.containsArgument(previousErrors, TransformErrorUtility.ARG_WAITING)) {
            return null;
        }

        //if we make it to here, we only want to process specific record numbers in our file, or even none, if there were
        //no previous errors processing this specific file
        HashSet<Long> recordNumbers = new HashSet<>();

        for (Error error: previousErrors.getError()) {

            String errorFileName = TransformErrorUtility.findArgumentValue(error, TransformErrorUtility.ARG_EMIS_CSV_FILE);
            if (!Strings.isNullOrEmpty(errorFileName)
                && errorFileName.equals(fileName)) {

                String errorRecordNumber = TransformErrorUtility.findArgumentValue(error, TransformErrorUtility.ARG_EMIS_CSV_RECORD_NUMBER);
                recordNumbers.add(Long.valueOf(errorRecordNumber));
            }
        }

        return recordNumbers;
    }

}
