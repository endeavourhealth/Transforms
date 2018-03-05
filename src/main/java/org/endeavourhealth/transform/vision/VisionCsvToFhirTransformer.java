package org.endeavourhealth.transform.vision;

import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.xml.TransformErrorUtility;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.endeavourhealth.transform.vision.schema.*;
import org.endeavourhealth.transform.vision.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.stream.Collectors;

public abstract class VisionCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(VisionCsvToFhirTransformer.class);

    public static final String VERSION_0_18 = "0.18";

    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmm";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;   //Vision files do not contain a header, so set on in each parsers constructor

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String version) throws Exception {

        //the exchange body will be a list of files received
        String[] files = ExchangeHelper.parseExchangeBodyIntoFileList(exchangeBody);
        LOG.info("Invoking Vision CSV transformer for " + files.length + " files using service " + serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

        Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            validateAndOpenParsers(serviceId, systemId, exchangeId, files, version, allParsers);

            LOG.trace("Transforming Vision CSV content in {}", orgDirectory);
            transformParsers(version, allParsers, processor, previousErrors);

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

    private static void validateAndOpenParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version, Map<Class, AbstractCsvParser> parsers) throws Exception {
        //admin - practice
        findFileAndOpenParser(Practice.class, serviceId, systemId, exchangeId, files, version, parsers);
        //admin - staff
        findFileAndOpenParser(Staff.class, serviceId, systemId, exchangeId, files, version, parsers);
        //patient - demographics
        findFileAndOpenParser(Patient.class, serviceId, systemId, exchangeId, files, version, parsers);
        //clinical - encounters
        findFileAndOpenParser(Encounter.class, serviceId, systemId, exchangeId, files, version, parsers);
        //clinical - referrals
        findFileAndOpenParser(Referral.class, serviceId, systemId, exchangeId, files, version, parsers);
        //clinical - journal (observations, medication, problems etc.)
        findFileAndOpenParser(Journal.class, serviceId, systemId, exchangeId, files, version, parsers);

        //then validate there are no extra, unexpected files in the folder, which would imply new data
        //Set<File> sh = new HashSet<>(parsers);

        Set<String> expectedFiles = parsers
                .values()
                .stream()
                .map(T -> T.getFilePath())
                .collect(Collectors.toSet());

        for (String filePath: files) {
            if (!ignoreKnownFile(filePath)
                    && !expectedFiles.contains(filePath)
                    && !FilenameUtils.getExtension(filePath).equalsIgnoreCase("csv")) {

                throw new FileFormatException(filePath, "Unexpected file " + filePath + " in Vision CSV extract");
            }
        }
    }

    // these files comes with the Vision extract but we currently do not transform, so ignore them in the unexpected file check
    private static boolean ignoreKnownFile(String filePath) {
        String name = FilenameUtils.getName(filePath);
        return name.contains("active_user_data") || name.contains("patient_check_sum_data");
    }

    public static void findFileAndOpenParser(Class parserCls, UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version, Map<Class, AbstractCsvParser> ret) throws Exception {

        String name = parserCls.getSimpleName();

        for (String filePath: files) {
            String fName = FilenameUtils.getName(filePath);

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
            Constructor<AbstractCsvParser> constructor = parserCls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
            AbstractCsvParser parser = constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);

            ret.put(parserCls, parser);
            return;
        }

        throw new FileNotFoundException("Failed to find CSV file for " + name);
    }


    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors) throws Exception {

        VisionCsvHelper csvHelper = new VisionCsvHelper();

        //these transforms do not create resources themselves, but cache data that the subsequent ones rely on
        JournalProblemPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        JournalDrugPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        JournalPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        EncounterPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

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

            String filePath = parser.getFilePath();
            String fileName = FilenameUtils.getName(filePath);

            Set<Long> recordNumbers = TransformErrorUtility.findRecordNumbersToProcess(fileName, previousErrors);
            parser.setRecordNumbersToProcess(recordNumbers);

            //if we have a non-null set, then we're processing specific records in some file
            if (recordNumbers != null) {
                processingSpecificRecords = true;
            }
        }

        return processingSpecificRecords;
    }
}
