package org.endeavourhealth.transform.tpp;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SRCcgTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SROrganisationBranchTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SROrganisationTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SRTrustTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.SRAppointmentFlagsTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.SRAppointmentTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.SRRotaTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.SRVisitTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.clinical.*;
import org.endeavourhealth.transform.tpp.csv.transforms.codes.*;
import org.endeavourhealth.transform.tpp.csv.transforms.patient.*;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberProfilePreTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberProfileTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class TppCsvToFhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(TppCsvToFhirTransformer.class);

    public static final String DATE_FORMAT = "dd MMM yyyy";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final String TIME_FORMAT_NO_SEC = "HH:mm";   //the SRPatient file uses this time format

    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();
    public static final Charset ENCODING = Charset.forName("Cp1252");

    public static final String VERSION_TEST_PACK = "TestPack"; //file format we got from the demo environment mid-2017
    public static final String VERSION_TEST_PACK_2 = "TestPack2"; //file format we got since 2017-04-26
    public static final String VERSION_TEST_PACK_3 = "TestPack3"; //file format we got since 2017-04-27
    public static final String VERSION_87 = "87"; //file format first received from the pilot practice
    public static final String VERSION_88 = "88";
    public static final String VERSION_89 = "89"; //Basically 88 plus RemovedData as needed
    public static final String VERSION_90 = "90"; //Basically 89 plus 1 new column in ReferralOut
    public static final String VERSION_91 = "91"; //Basically 90 but no RemovedData
    public static final String VERSION_92 = "92"; //Basically 91 plus 1 new column in SRRota (DateStart), no RemovedData
    public static final String VERSION_93 = "93"; //Basically 92 plus RemovedData

    private static Set<String> cachedFileNamesToIgnore = null; //set of file names we know contain data but are deliberately ignoring



    public static void transform(String exchangeBody, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        String[] files = ExchangeHelper.parseExchangeBodyOldWay(exchangeBody);
        LOG.info("Invoking TPP CSV transformer for " + files.length + " files and service " + fhirResourceFiler.getServiceId());

        if (files.length == 0) {
            LOG.info("No files in exchange, so returning out");
            return;
        }

        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        Map<Class, AbstractCsvParser> parsers = new HashMap<>();

        //work out the version of the files by checking the headers (ignoring what was passed in)
        //version = determineVersion(files);
        Map<String, String> parserToVersionsMap = buildParserToVersionsMap(files);

        boolean processPatientData = shouldProcessPatientData(files);

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            createParsers(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId(), files,parserToVersionsMap, parsers);

            LOG.trace("Transforming TPP CSV content in " + orgDirectory);
            transformParsers(parsers, fhirResourceFiler, processPatientData);

        } finally {
            closeParsers(parsers.values());
        }
    }


    /**
     * works out if we want to process (i.e. transform and store) the patient data from this extract,
     * which we don't if this extract is from before we received a later re-bulk from emis
     */
    public static boolean shouldProcessPatientData(String[] files) throws Exception {

        //find the extract date from one of the CSV file names
        String firstFile = files[0];
        File f = new File(firstFile);

        //our ODS code is the same as the directory name
        File odsDir = f.getParentFile();
        String odsCode = odsDir.getName();

        //the extract date is the parent of the parent
        File splitDir = odsDir.getParentFile();
        File extractDir = splitDir.getParentFile();
        String extractDateStr = extractDir.getName();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm.ss");
        Date extractDate = sdf.parse(extractDateStr);

        Date startDate = findStartDate(odsCode);

        if (startDate == null
                || !extractDate.before(startDate)) {
            return true;

        } else {
            LOG.info("Not processing patient data for extract " + extractDate + " for " + odsCode + " as this is before their start date of " + startDate);
            return false;
        }
    }


    private static Date findStartDate(String odsCode) throws Exception {

        Map<String, String> map = new HashMap<>();

        map.put("F86638", "30/03/2018"); //Microfaculty
        map.put("B86071", "22/01/2019"); //Whitehall
        map.put("B86022", "22/01/2019"); //Oakwood

        String startDateStr = map.get(odsCode);
        if (Strings.isNullOrEmpty(startDateStr)) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        return sdf.parse(startDateStr);
    }



    /**
     * the TPP schema changes without notice, so rather than define the version in the SFTP reader,
     * and then need to back pedal when we find it's changed, dynamically work it out from the CSV headers
     * 16/08/2019 - changing from requiring one consistent version for all files in a batch to
     * accepting the latest version of compatible parsers for each file.
     */
//    public static String determineVersion(String[] files) throws Exception {
//
//        List<String> possibleVersions = new ArrayList<>();
//        Map<String, List<String>> breadcrumbs = new HashMap<String, List<String>>();
//
//        possibleVersions.add(VERSION_93);
//        possibleVersions.add(VERSION_92);
//        possibleVersions.add(VERSION_91);
//        possibleVersions.add(VERSION_90);
//        possibleVersions.add(VERSION_TEST_PACK_3);
//        possibleVersions.add(VERSION_TEST_PACK_2);
//        possibleVersions.add(VERSION_89);
//        possibleVersions.add(VERSION_88);
//        possibleVersions.add(VERSION_87);
//        possibleVersions.add(VERSION_TEST_PACK);
//
//        for (String filePath : files) {
//
//            try {
//                //create a parser for the file but with a null version, which will be fine since we never actually parse any data from it
//                AbstractCsvParser parser = createParserForFile(null, null, null, null, filePath);
//
//                //calling this will return the possible versions that apply to this parser
//                possibleVersions = parser.testForValidVersions(possibleVersions);
//                breadcrumbs.put(filePath, possibleVersions);
//                if (possibleVersions.isEmpty()) {
//                    break;
//                }
//
//            } catch (ClassNotFoundException ex) {
//                //we don't have parsers for every file, since there are a lot of secondary-care (etc.) files
//                //that we don't transform, but we also want to make sure that they're EMPTY unless we explicitly
//                //have decided to ignore a non-empty file
//                ensureFileIsEmpty(filePath);
//            } catch (IOException eio) {
//                LOG.error("", eio);
//            }
//        }
//
//        //if we end up with one or more possible versions that do apply, then
//        //return the first, since that'll be the most recent one
//        if (!possibleVersions.isEmpty()) {
//            return possibleVersions.get(0);
//        }
//        // We've run out of goes so print some breadcrumbs
//        LOG.info("Filename : possible versions");
//        for (String filePath : files) {
//            if (breadcrumbs.get(filePath) != null ) {
//                LOG.info(filePath + ":" + breadcrumbs.get(filePath).toString());
//            } else {
//                LOG.info(filePath + ": has NULL breadcrumbs reference");
//            }
//        }
//        throw new TransformException("Unable to determine version for TPP CSV");
//    }

    /**
     * the TPP schema changes without notice, so rather than define the version in the SFTP reader,
     * and then need to back pedal when we find it's changed, dynamically work it out from the CSV headers
     */
    public static Map<String, String> buildParserToVersionsMap(String[] files) throws Exception {

       List<String> possibleVersions = new ArrayList<>();
        Map<String, String> parserToVersionsMap = new HashMap<>();
        possibleVersions.add(VERSION_93);
        possibleVersions.add(VERSION_92);
        possibleVersions.add(VERSION_91);
        possibleVersions.add(VERSION_90);
        possibleVersions.add(VERSION_TEST_PACK_3);
        possibleVersions.add(VERSION_TEST_PACK_2);
        possibleVersions.add(VERSION_89);
        possibleVersions.add(VERSION_88);
        possibleVersions.add(VERSION_87);
        possibleVersions.add(VERSION_TEST_PACK);

        List<String> noVersions  = new ArrayList<>();

        for (String filePath : files) {
            try {
                List<String> compatibleVersions = new ArrayList<>();
                //create a parser for the file but with a null version, which will be fine since we never actually parse any data from it
                AbstractCsvParser parser = createParserForFile(null, null, null, null, filePath);
                //calling this will return the possible versions that apply to this parser
                compatibleVersions = parser.testForValidVersions(possibleVersions);
                if (filePath.contains("PatientRelation")) {
                    LOG.info("PatientRelation versions " + Arrays.toString(noVersions.toArray()));
                }
                if (compatibleVersions.isEmpty()) {
                    ensureFileIsEmpty(filePath);
                    noVersions.add(filePath); //Not dropping straight out as multiple files may have changed.
                    //throw new TransformException("Unable to determine version for TPP CSV file: " + filePath);
                } else  {
                    parserToVersionsMap.put(filePath, compatibleVersions.get(0));
                }
            } catch (ClassNotFoundException ex) {
                //we don't have parsers for every file, since there are a lot of secondary-care (etc.) files
                //that we don't transform, but we also want to make sure that they're EMPTY unless we explicitly
                //have decided to ignore a non-empty file
                ensureFileIsEmpty(filePath);
            } catch (IOException eio) {
                if (eio.getMessage().contains("startline 1")) {
                    //
                    LOG.info("Missing newline in file. Skipping : " +filePath);
                    parserToVersionsMap.put(filePath,"0");
                } else {
                    LOG.error("", eio);
                }
            }
        }
        if (noVersions.size()>0) {
            System.out.println(Arrays.toString(noVersions.toArray()));
            throw new TransformException("Unable to determine TPP CSV version for above file(s).");
        }

        for (Map.Entry<String, String> entry : parserToVersionsMap.entrySet()) {
            LOG.info("ParserMap" +entry.getKey() + "/" + entry.getValue());
        }
    return parserToVersionsMap;

    }


    public static void createParsers(UUID serviceId, UUID systemId, UUID exchangeId,  String[] files,Map<String, String> versions, Map<Class, AbstractCsvParser> parsers) throws Exception {

        for (String filePath : files) {
            LOG.info("Files: " + Arrays.toString(files));
            if (filePath == null) {continue;}
            try {
                String version = versions.get(filePath);
                if (version == null) {
                    LOG.info("Null version for " + filePath);
                    continue;
//                    for (Map.Entry<String, String> entry : versions.entrySet()) {
//                        LOG.info("ParserMap" + entry.getKey() + "/" + entry.getValue());
//                    }
                }
                if (version == ("0")) {
                    LOG.info("Skipping file with just headers, no data: " + filePath);
                    continue;
                }
                AbstractCsvParser parser = createParserForFile(serviceId, systemId, exchangeId, version, filePath);
                Class cls = parser.getClass();
                parsers.put(cls, parser);

            } catch (ClassNotFoundException ex) {
                //ignore any file the doesn't have a parser (we've already validated
                //that we're OK to ignore it)
            }
        }
    }

    private static void ensureFileIsEmpty(String filePath) throws Exception {

        //if we know it'll be non-empty but we know we want to ignore it, don't check the content
        String baseName = FilenameUtils.getBaseName(filePath);
        if (getFilesToIgnore().contains(baseName)) {
            return;
        }

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath);
        CSVParser parser = new CSVParser(reader, CSV_FORMAT);
        try {
            Iterator<CSVRecord> iterator = parser.iterator();
            if (iterator.hasNext()) {
                throw new TransformException("" + filePath + " isn't being transformed but isn't empty");
            }

        } finally {
            parser.close();
        }
    }

    private static Set<String> getFilesToIgnore() {
        if (cachedFileNamesToIgnore == null) {

            Set<String> set = new HashSet<>();

            //add any non-empty files we want to ignore here
            set.add("SRAddressBookEntry");

            //TODO - confirm that these files ARE OK to be ignored

            set.add("SRActivityEvent");
            set.add("SRAppointmentRoom");
            set.add("SRCarePlanDetail");
            set.add("SRCarePlanItem");
            set.add("SRCarePlanReview");
            set.add("SRCarePlanSkillset");
            set.add("SRCarePlanPerformance");
            set.add("SRCaseload");
            set.add("SRCaseloadHistory");
            set.add("SRCtv3ToVersion2");
            set.add("SRCtv3ToSnomed");
            set.add("SRGPPracticeHistory");
            set.add("SRLetter");
            set.add("SRMappingGroup");
            set.add("SROnlineServices");
            set.add("SROnlineUsers");
            set.add("SRPatientGroups");
            set.add("SRPatientInformation");
            set.add("SRQuestionnaire");
            set.add("SRAnsweredQuestionnaire");
            set.add("SRReferralContactEvent");
            set.add("SRReferralIn");
            set.add("SRReferralInStatusDetails");
            set.add("SRReferralInIntervention");
            set.add("SRReferralInReferralReason");
            set.add("SRRotaSlot");
            set.add("SRSchoolHistory");
            set.add("SRSmsConsent");
            set.add("SRStaff");
            set.add("SRStaffMemberProfileRole");
            set.add("SRSystmOnline");
            set.add("SRTreatmentCentrePreference");
            set.add("SRTemplate");
            set.add("SRCodeTemplateLink");
            set.add("SRMedia");
            set.add("SRImmunisationConsent");
            set.add("SRGoal");
            set.add("SRWaitingList");
            set.add("SRAttendee");
            set.add("SRCluster");
            set.add("SRHospitalAAndENumber");
            set.add("SRCHSStatusHistory");
            set.add("SRMentalHealthAssessment");
            set.add("SRCarePlanFrequency");
            set.add("SRPlaceHolderMedication");
            set.add("SRProblemSubstance");
            set.add("SR18WeekWait");
            set.add("SRCarePlanPerformanceCodeLink");
            set.add("SRExpense");
            set.add("SROverseasVisitorChargingCategory");
            set.add("SRAppointmentAttendees");
            set.add("SRSchedulingSuspension");
            set.add("SRQuestionnaireAmendment");
            set.add("SRHospitalAlertIndicator");
            set.add("SRReferralAllocation");
            set.add("SRContacts");
            set.add("SRReferralAllocationStaff");

            cachedFileNamesToIgnore = set;
        }
        return cachedFileNamesToIgnore;
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

    private static AbstractCsvParser createParserForFile(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {

        String fName = FilenameUtils.getBaseName(filePath);
        String domain = getDomainFromFileName(fName);
        String clsName = "org.endeavourhealth.transform.tpp.csv.schema." + domain + "." + fName;
        Class cls = Class.forName(clsName);

        //now construct an instance of the parser for the file we've found
        Constructor<AbstractCsvParser> constructor = cls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
        return constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);
    }

    private static String getDomainFromFileName(String fileName) throws Exception {

        if (fileName.equals("SRAddressBook")
                || fileName.equals("SROrganisation")
                || fileName.equals("SROrganisationBranch")
                || fileName.equals("SRTrust")
                || fileName.equals("SRCcg")) {
            return "admin";

        } else if (fileName.equals("SRAppointment")
                || fileName.equals("SRAppointmentFlags")
                || fileName.equals("SRAppointmentRoom")
                || fileName.equals("SRRota")
                || fileName.equals("SRRotaSlot")) {
            return "appointment";

        } else if (fileName.equals("SRChildAtRisk")
                || fileName.equals("SRCode")
                || fileName.equals("SRDrugSensitivity")
                || fileName.equals("SREvent")
                || fileName.equals("SREventLink")
                || fileName.equals("SRImmunisation")
                || fileName.equals("SRImmunisationConsent")
                || fileName.equals("SRImmunisationContent")
                || fileName.equals("SRPrimaryCareMedication")
                || fileName.equals("SRProblem")
                || fileName.equals("SRRecall")
                || fileName.equals("SRRecordStatus")
                || fileName.equals("SRRepeatTemplate")
                || fileName.equals("SRSpecialNotes")
                || fileName.equals("SRVisit")) {
            return "clinical";

        } else if (fileName.equals("SRConfiguredListOption")
                || fileName.equals("SRCtv3")
                || fileName.equals("SRCtv3Hierarchy")
                || fileName.equals("SRCtv3ToVersion2")
                || fileName.equals("SRMapping")
                || fileName.equals("SRMedicationReadCodeDetails")
                || fileName.equals("SRTemplate")) {
            return "codes";

        } else if (fileName.equals("SRMedia")
                || fileName.equals("SRLetter")) {
            return "documents";

        } else if (fileName.equals("SRPatient")
                || fileName.equals("SRPatientAddressHistory")
                || fileName.equals("SRPatientContactDetails")
                || fileName.equals("SRPatientRegistration")
                || fileName.equals("SRPatientRelationship")) {
            return "patient";

        } else if (fileName.equals("SRReferralOut")
                || fileName.equals("SRReferralOutStatusDetails")) {
            return "referral";

        } else if (fileName.equals("SRStaffMember")
                || fileName.equals("SRStaffMemberProfile")
                || fileName.equals("SRStaffSkillSet")
                || fileName.equals("SRStaffSpecialty")) {
            return "staff";

        } else {
            //use this exception type, since that's what's caught higher up
            throw new ClassNotFoundException("Unknown domain for file " + fileName);
        }
    }

    private static void transformParsers(Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler,
                                         boolean processPatientData) throws Exception {

        TppCsvHelper csvHelper = new TppCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                                                    fhirResourceFiler.getExchangeId());

        //reference data
        LOG.trace("Starting reference data transforms");
        SRCtv3Transformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRCtv3HierarchyTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRImmunisationContentTransformer.transform(parsers, fhirResourceFiler);
        SRMappingTransformer.transform(parsers, fhirResourceFiler);
        SRConfiguredListOptionTransformer.transform(parsers, fhirResourceFiler);
        SRMedicationReadCodeDetailsTransformer.transform(parsers, fhirResourceFiler);

        //organisational admin data
        LOG.trace("Starting admin transforms");
        SRCcgTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRTrustTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SROrganisationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SROrganisationBranchTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRRotaTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        fhirResourceFiler.waitUntilEverythingIsSaved();

        LOG.trace("Starting practitioners transforms");
        //these pre-transformers all cache data used by SRStaffMemberProfileTransformer
        SRStaffMemberProfilePreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //this must be done before the next two
        SREventPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRReferralOutPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);


        SRStaffMemberTransformer.transform(parsers, fhirResourceFiler, csvHelper); //this just caches staff member details
        SRStaffMemberProfileTransformer.transform(parsers, fhirResourceFiler, csvHelper); //this actually creates Practitioner resources
        csvHelper.getStaffMemberCache().processRemainingStaffMembers(csvHelper, fhirResourceFiler);

        //make sure all practitioners are saved to the DB before doing anything clinical
        fhirResourceFiler.waitUntilEverythingIsSaved();

        if (processPatientData) {

            LOG.trace("Starting patient demographics transforms");
            SRPatientAddressHistoryPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientContactDetailsPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientRelationshipPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRCodePreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRRecordStatusTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientAddressHistoryTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientContactDetailsTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientRelationshipTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            LOG.debug("Going to do registration transform");
            SRPatientRegistrationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            LOG.debug("Done registration transform");
            fhirResourceFiler.waitUntilEverythingIsSaved();

            csvHelper.getPatientResourceCache().fileResources(fhirResourceFiler);
            csvHelper.processRemainingRegistrationStatuses(fhirResourceFiler);

            fhirResourceFiler.waitUntilEverythingIsSaved();

            SRAppointmentFlagsTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRAppointmentTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.getAppointmentFlagCache().processRemainingFlags(csvHelper, fhirResourceFiler);
            SRVisitTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            fhirResourceFiler.waitUntilEverythingIsSaved();

            LOG.trace("Starting clinical transforms");
            SREventLinkTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRDrugSensitivityPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRImmunisationPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRRecallPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRRepeatTemplatePreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPrimaryCareMedicationPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            fhirResourceFiler.waitUntilEverythingIsSaved();

            SREventTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            SRRepeatTemplateTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPrimaryCareMedicationTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            SRReferralOutTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRReferralOutStatusDetailsTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.getReferralRequestResourceCache().fileReferralRequestResources(fhirResourceFiler);

            fhirResourceFiler.waitUntilEverythingIsSaved();

            SRProblemPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRProblemTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRCodeTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.getConditionResourceCache().fileConditionResources(fhirResourceFiler);

            SRDrugSensitivityTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            SRImmunisationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRChildAtRiskTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            SRSpecialNotesTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        }

        //close down the utility thread pool
        csvHelper.stopThreadPool();
    }

    private boolean isFileBulk(String[] infiles) {
        // We try to deduce if this is a bulk file (whole table) or a delta file of updates.
        // We look at the first 3 rows and if they're very low values then probably old stable
        // values so this is probably a bulk file.
        int probabilityBulk = 0;
        int probabilityThreshold = 3;
        int lowValue = 150;
        int count = 0;
        int max = 3;

        for (String filename : infiles) {
            try {
                CSVParser csvFileParser = CSVFormat.DEFAULT.parse(new FileReader(new File(filename)));
                for (CSVRecord csvRecord : csvFileParser) {
                    count++;
                    // Accessing Values by Column Index - 0 based.
                    if (Integer.parseInt(csvRecord.get(count)) < lowValue) {
                        probabilityBulk++;
                    }
                    if (count > max && probabilityBulk >= probabilityThreshold) {
                        return true;
                    } else {
                        return false;
                    }
                }
            } catch (Exception ex) {
                LOG.error("Check for bulk file exception. " + ex.getMessage());
            }
        }
        return false;
    }

    ;
}
