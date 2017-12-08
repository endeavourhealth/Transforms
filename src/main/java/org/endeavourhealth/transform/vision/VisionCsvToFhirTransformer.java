package org.endeavourhealth.transform.vision;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
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

    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmm";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;   //Vision files do not contain a header, so set on in each parsers constructor

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
        findFileAndOpenParser(Practice.class, dir, version, openParser, parsers);
        //admin - staff
        findFileAndOpenParser(Staff.class, dir, version, openParser, parsers);
        //patient - demographics
        findFileAndOpenParser(Patient.class, dir, version, openParser, parsers);
        //clinical - encounters
        findFileAndOpenParser(Encounter.class, dir, version, openParser, parsers);
        //clinical - referrals
        findFileAndOpenParser(Referral.class, dir, version, openParser, parsers);
        //clinical - journal (observations, medication, problems etc.)
        findFileAndOpenParser(Journal.class, dir, version, openParser, parsers);

        //then validate there are no extra, unexpected files in the folder, which would imply new data
        //Set<File> sh = new HashSet<>(parsers);

        Set<File> expectedFiles = parsers
                .values()
                .stream()
                .map(T -> T.getFile())
                .collect(Collectors.toSet());

        for (File file: dir.listFiles()) {
            if (file.isFile()
                    && !ignoreKnownFile(file)
                    && !expectedFiles.contains(file)
                    && !Files.getFileExtension(file.getAbsolutePath()).equalsIgnoreCase("csv")) {

                throw new FileFormatException(file, "Unexpected file " + file + " in Vision CSV extract");
            }
        }
    }

    // these files comes with the Vision extract but we currently do not transform, so ignore them in the unexpected file check
    public static boolean ignoreKnownFile (File file) {
        return file.getName().contains("active_user_data") || file.getName().contains("patient_check_sum_data");
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

            //Vision files are format:  FULL_33333_encounter_data_extract-2017-09-23-165206.csv
            String[] toks = fName.split("_");
            if (toks.length != 5) {
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

        //this transform does not create resources themselves, but cache data that the subsequent ones rely on
        JournalProblemPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        JournalPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //before getting onto the files that actually create FHIR resources, we need to
        //work out what record numbers to process, if we're re-running a transform
        //boolean processingSpecificRecords = findRecordsToProcess(parsers, previousErrors);

        //run the transforms for non-patient resources
        PracticeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        StaffTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //then for the patient resources - note the order of these transforms is important, as encounters should be before journal obs etc.
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

    public static String cleanUserId(String data) {
        if (!Strings.isNullOrEmpty(data)) {
            if (data.contains(":STAFF:")) {
                data = data.replace(":","").replace("STAFF","");
                LOG.info("Cleansed user Id: "+data);
                return data;
            }
            if (data.contains(":EXT_STAFF:")) {
                data = data.substring(0,data.indexOf(","));
                data = data.replace(":","").replace("EXT_STAFF","").replace(",", "");
                LOG.info("Cleansed user Id: "+data);
                return data;
            }
        }
        return data;
    }
}
