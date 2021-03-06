package org.endeavourhealth.transform.tpp;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SRCcgTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SROrganisationBranchTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SROrganisationTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SRTrustTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.*;
import org.endeavourhealth.transform.tpp.csv.transforms.clinical.*;
import org.endeavourhealth.transform.tpp.csv.transforms.codes.*;
import org.endeavourhealth.transform.tpp.csv.transforms.patient.*;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberProfileTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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

    private static Set<String> cachedFileNamesToIgnore = null; //set of file names we know contain data but will not be processing


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
        Map<String, String> fileVersionMap = calculateVersionForFiles(files, fhirResourceFiler);

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            createParsers(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId(), files, fileVersionMap, parsers);

            LOG.trace("Transforming TPP CSV content in " + orgDirectory);
            transformParsers(parsers, fhirResourceFiler);

        } finally {
            closeParsers(parsers.values());
        }
    }


    /**
     * works out if we want to process (i.e. transform and store) the patient data from this extract,
     * which we don't if this extract is from before we received a later re-bulk from emis
     */
    public static boolean shouldProcessPatientData(FhirResourceFiler fhirResourceFiler) throws Exception {

        Date extractDate = fhirResourceFiler.getDataDate();

        ServiceDalI serviceDal = DalProvider.factoryServiceDal();
        Service service = serviceDal.getById(fhirResourceFiler.getServiceId());
        String odsCode = service.getLocalId();
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
    public static Map<String, String> calculateVersionForFiles(String[] files,
                                                               HasServiceSystemAndExchangeIdI hasServiceSystemAndExchangeId) throws Exception {

        Map<String, String> ret = new HashMap<>();

        List<String> possibleVersions = new ArrayList<>();
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

        List<String> versionFailures = new ArrayList<>();

        for (String filePath : files) {
            LOG.trace("Calculating version for file " + filePath);

            //see if we have a suitable parser for the file
            AbstractCsvParser parser = null;
            try {
                //create a parser for the file but with a null version, which will be fine since we never actually parse any data from it
                parser = createParserForFile(null, null, null, null, filePath);

            } catch (ClassNotFoundException ex) {
                //we don't have parsers for every file, since there are a lot of secondary-care (etc.) files
                //that we don't transform, but we also want to make sure that they're EMPTY unless we explicitly
                //have decided to ignore a non-empty file
                ensureFileIsEmpty(filePath, hasServiceSystemAndExchangeId);
                continue;
            }
            LOG.trace("Parser created");

            //if we have a parser, then detect what version it is
            List<String> compatibleVersions = parser.testForValidVersions(possibleVersions);
            LOG.trace("Found " + compatibleVersions.size() + " possible versions");

            if (compatibleVersions.isEmpty()) {
                ensureFileIsEmpty(filePath, hasServiceSystemAndExchangeId);
                versionFailures.add(filePath); //Not dropping straight out as multiple files may have changed so let's find them all

            } else {
                ret.put(filePath, compatibleVersions.get(0));
            }
        }

        if (versionFailures.size() > 0) {
            String msg = "Failed to calculate version for files [" + String.join(", ", versionFailures) + "]";
            throw new TransformException(msg);
        }

        return ret;
    }


    public static void createParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, Map<String, String> versions, Map<Class, AbstractCsvParser> parsers) throws Exception {

        for (String filePath : files) {

            //why would we have nulls in this array?
            if (filePath == null) {
                continue;
            }

            try {
                String version = versions.get(filePath);
                if (version == null) {
                    //if this is a file we don't process, this may be null
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

    private static void ensureFileIsEmpty(String filePath, HasServiceSystemAndExchangeIdI hasServiceSystemAndExchangeId) throws Exception {

        //if we're sure we are OK to ignore this file, then don't check if it's empty
        String name = FilenameUtils.getBaseName(filePath);
        if (getFilesToIgnore().contains(name)) {
            return;
        }

        //S3 complains if we open a stream and kill it before we get to the end, so avoid this by
        //explicitly reading only the first 5KB of the file and testing that
        //InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath);
        String firstChars = FileHelper.readFirstCharactersFromSharedStorage(filePath, 5 * 1024);
        StringReader reader = new StringReader(firstChars);

        CSVParser parser = new CSVParser(reader, CSV_FORMAT);
        try {
            Iterator<CSVRecord> iterator = parser.iterator();
            if (iterator.hasNext()) {
                //sick of hitting this exception and having to fix the code, so just log and proceed
                String baseName = FilenameUtils.getBaseName(filePath);
                TransformWarnings.log(LOG, hasServiceSystemAndExchangeId, "TPP file {} ignored but not empty", baseName);
                //throw new TransformException("" + filePath + " isn't being transformed but isn't empty");
            }

        } finally {
            parser.close();
        }
    }

    private static Set<String> getFilesToIgnore() {
        if (cachedFileNamesToIgnore == null) {

            Set<String> set = new HashSet<>();

            //add any non-empty files we are aware we're ignoring here
            //set.add("SRCtv3ToSnomed"); //added transformer for this
            set.add("SRMappingGroup");
            set.add("SRQuestionnaire");
            set.add("SRStaffMemberProfileRole");
            set.add("SRCodeTemplateLink");
            set.add("SRGPPracticeHistory");
            set.add("SRImmunisationConsent");
            set.add("SRLetter");
            set.add("SRMedia");
            set.add("SROnlineServices");
            set.add("SRSmsConsent");
            set.add("SRSystmOnline");
            set.add("SRAddressBookEntry");
            set.add("SRGoal");
            set.add("SROnlineUsers");
            set.add("SRCarePlanPerformanceCodeLink");
            set.add("SRActivityEvent");
            set.add("SRAnsweredQuestionnaire");
            set.add("SRCarePlanDetail");
            set.add("SRCarePlanItem");
            set.add("SRCaseload");
            set.add("SRCaseloadHistory");
            set.add("SRExpense");
            set.add("SRHospitalAAndENumber");
            set.add("SROverseasVisitorChargingCategory");
            set.add("SRPatientGroups");
            set.add("SRProblemSubstance");
            set.add("SRReferralIn");
            set.add("SRReferralInIntervention");
            set.add("SRReferralInReferralReason");
            set.add("SRReferralInStatusDetails");
            set.add("SRWaitingList");
            set.add("SRSchoolHistory");
            set.add("SRTeam");
            set.add("SRTeamMember");
            set.add("SRTreatmentCentrePreference");
            set.add("SRCPA");
            set.add("SRReferralAllocation");
            set.add("SRReferralAllocationStaff");
            set.add("SR18WeekWait");
            set.add("SRCHSStatusHistory");
            set.add("SRCarePlanReview");
            set.add("SRAppointmentAttendees");
            set.add("SRAttendee");
            set.add("SRSchedulingSuspension");
            set.add("SRCarePlanPerformance");
            set.add("SRCarePlanFrequency");
            set.add("SRVariableDoseCDMedication");
            set.add("SRContacts");
            set.add("SRQuestionnaireAmendment");
            set.add("SRMentalHealthAssessment");
            set.add("SRPlaceHolderMedication");
            set.add("SRRiskReview");
            set.add("SRCluster");
            set.add("SRSection");
            set.add("SRCarePlanSkillset");
            set.add("SRManifest");
            set.add("SRSectionedBy");
            set.add("SRSectionAppeal");
            set.add("SRSectionRecall");
            set.add("SRReferralContactEvent");
            set.add("SRPatientRelationship");
            set.add("SRMHConsent");
            set.add("SRMentalHealthCarePlan");
            set.add("SRSecondaryCareMedication");
            set.add("SRDrugAdministration");
            set.add("SRAppointmentVisitOutcomes");
            set.add("SRPatientContactProperty");
            set.add("SRRefusedOffer");
            set.add("SRRtt");
            set.add("SRRttPause");
            set.add("SRRttStatus");
            set.add("SRStaffActivity");
            set.add("SRClinicalCode");
            set.add("SRTask");
            set.add("SRTaskUpdate");
            set.add("SRTTOMedication");
            set.add("SROnAdmissionMedication");
            set.add("SRSectionRightsExplained");

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

    @SuppressWarnings("unchecked")
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
                || fileName.equals("SRPersonAtRisk")
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
                || fileName.equals("SRTemplate")
                || fileName.equals("SRCtv3ToSnomed")) {
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
                                         FhirResourceFiler fhirResourceFiler) throws Exception {

        TppCsvHelper csvHelper = new TppCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId());
        boolean processPatientData = shouldProcessPatientData(fhirResourceFiler);

        //massive hack to allow the clinical observations to be processed faster - audit skipping it so we can come back later
//        if (true) {
//            auditSkippingAdminData(fhirResourceFiler);
//            processAdminData = false;
//        }

        //hack to skip the SRCode file if it's a re-bulk. Audit in a table so we can come back.
        //boolean processSRCode = !shouldSkipSRCode(fhirResourceFiler);


        //reference data
        LOG.info("Starting reference data transforms");
        SRCtv3Transformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRCtv3HierarchyTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRImmunisationContentTransformer.transform(parsers, fhirResourceFiler);
        SRMappingTransformer.transform(parsers, fhirResourceFiler);
        SRConfiguredListOptionTransformer.transform(parsers, fhirResourceFiler);
        SRMedicationReadCodeDetailsTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRCtv3ToSnomedTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        //organisational admin data
        LOG.info("Starting admin transforms");
        SRCcgTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRTrustTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SROrganisationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SROrganisationBranchTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        fhirResourceFiler.waitUntilEverythingIsSaved();

        LOG.info("Starting practitioners transforms");
        SRStaffMemberProfileTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRStaffMemberTransformer.transform(parsers, fhirResourceFiler, csvHelper); //must be after the above

        LOG.info("Starting pre-transforms to cache patient data");
        SREventPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches existing links to consultations
        SRCodePreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches ethnicities, marital status and new links to consultation
        SRImmunisationPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches new links to consultation
        SRReferralOutPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches new links to consultation
        SRDrugSensitivityPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches new links to consultation
        SRPrimaryCareMedicationPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches new links to consultation
        SRRecallPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches new links to consultation
        SRRepeatTemplatePreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches new links to consultation

        SRRotaPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //retrieves existing rota data off the DB and caches
        SRAppointmentPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches staff and start for rotas
        SRRotaTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        csvHelper.getRotaDateAndStaffCache().processRemainingRotaDetails(fhirResourceFiler, csvHelper); //make updates to rotas NOT in SRRota

        if (processPatientData) {

            LOG.info("Starting patient demographics transforms");
            SRPatientAddressHistoryPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientContactDetailsPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientRelationshipPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.waitUntilThreadPoolIsEmpty();

            SRPatientTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPatientRegistrationPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //set care provider
            SRPatientAddressHistoryTransformer.transform(parsers, fhirResourceFiler, csvHelper); //add addresses
            SRPatientContactDetailsTransformer.transform(parsers, fhirResourceFiler, csvHelper); //add phone numbers
            SRPatientRelationshipTransformer.transform(parsers, fhirResourceFiler, csvHelper); //add relationships
            csvHelper.getPatientResourceCache().fileResources(fhirResourceFiler);
            fhirResourceFiler.waitUntilEverythingIsSaved(); //just to make sure that all the above is done before we continue
            csvHelper.processRemainingEthnicitiesAndMartialStatuses(fhirResourceFiler);

            //create episodes of care
            SRRecordStatusTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches statuses
            SRPatientRegistrationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.getRecordStatusHelper().processRemainingRegistrationStatuses(fhirResourceFiler); //handle any record statuses without registrations
            fhirResourceFiler.waitUntilEverythingIsSaved();

            //appointments and visits
            SRAppointmentFlagsTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRAppointmentTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.getAppointmentFlagCache().processRemainingFlags(csvHelper, fhirResourceFiler);
            SRVisitTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            fhirResourceFiler.waitUntilEverythingIsSaved();

            LOG.info("Starting clinical transforms");
            SREventLinkTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            fhirResourceFiler.waitUntilEverythingIsSaved();

            SREventTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            SRRepeatTemplateTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRPrimaryCareMedicationTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            //referrals
            SRReferralOutStatusDetailsTransformer.transform(parsers, fhirResourceFiler, csvHelper); //caches status records
            SRReferralOutTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.getReferralStatusCache().processRemainingReferralStatuses(fhirResourceFiler); //handle any record statuses without registrations
            fhirResourceFiler.waitUntilEverythingIsSaved();

            //SRCode and problems
            SRProblemPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //saves some mappings using multiple threads
            SRProblemTransformer.transform(parsers, fhirResourceFiler, csvHelper); //
            SRCodeTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.getConditionResourceCache().processRemainingProblems(fhirResourceFiler);

            SRDrugSensitivityTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRImmunisationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRChildAtRiskTransformer.transform(parsers, fhirResourceFiler, csvHelper); //no longer being received as of 2019-11-21 (but keep because we may reprocess old data)
            SRPersonAtRiskTransformer.transform(parsers, fhirResourceFiler, csvHelper); //added to replace the above
            SRSpecialNotesTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SRRecallTransformer.transform(parsers, fhirResourceFiler, csvHelper); //was missing SD-159
        }

        //only after processing all the above files do we know what staff members we're actually interested in transforming
        csvHelper.getStaffMemberCache().processChangedStaffMembers(csvHelper, fhirResourceFiler);
        fhirResourceFiler.waitUntilEverythingIsSaved();

        //close down the utility thread pool
        csvHelper.stopThreadPool();
    }


    /*private static void auditSkippingAdminData(HasServiceSystemAndExchangeIdI fhirFiler) throws Exception {

        LOG.info("Skipping admin data for exchange " + fhirFiler.getExchangeId());
        AuditWriter.writeExchangeEvent(fhirFiler.getExchangeId(), "Skipped admin data");

        //write to audit table so we can find out
        Connection connection = ConnectionManager.getAuditConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO skipped_admin_data VALUES (?, ?, ?, ?)";
            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setString(col++, fhirFiler.getServiceId().toString());
            ps.setString(col++, fhirFiler.getSystemId().toString());
            ps.setString(col++, fhirFiler.getExchangeId().toString());
            ps.setTimestamp(col++, new java.sql.Timestamp(new Date().getTime()));

            ps.executeUpdate();
            connection.commit();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }*/


    /*private static boolean shouldSkipSRCode(FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID serviceId = fhirResourceFiler.getServiceId();
        UUID systemId = fhirResourceFiler.getSystemId();

        ServiceDalI serviceDal = DalProvider.factoryServiceDal();
        Service service = serviceDal.getById(serviceId);

        //only if in NORMAL mode do we look at skipping it
        List<ServiceInterfaceEndpoint> endpoints = service.getEndpointsList();
        for (ServiceInterfaceEndpoint endpoint: endpoints) {
            if (endpoint.getSystemUuid().equals(systemId)) {
                String mode = endpoint.getEndpoint();
                if (!mode.equals(ServiceInterfaceEndpoint.STATUS_NORMAL)) {
                    return false;
                }
            }
        }

        UUID exchangeId = fhirResourceFiler.getExchangeId();
        ExchangeDalI exchangeDal = DalProvider.factoryExchangeDal();
        Exchange exchange = exchangeDal.getExchange(exchangeId);
        String bodyJson = exchange.getBody();
        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(bodyJson);
        for (ExchangePayloadFile file: files) {
            if (file.getType().equals("Code")) {
                Long size = file.getSize();
                if (size != null
                        && size.longValue() > 150 * 1024 * 1024) {
                    auditSkippingSRCode(fhirResourceFiler);
                    return true;
                }
            }
        }

        return false;
    }

    private static void auditSkippingSRCode(HasServiceSystemAndExchangeIdI fhirFiler) throws Exception {

        LOG.warn("Skipping SRCode for exchange " + fhirFiler.getExchangeId());
        AuditWriter.writeExchangeEvent(fhirFiler.getExchangeId(), "Skipped SRCode");

        String msg = "Skipped large SRCode file for service " + fhirFiler.getServiceId();
        SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, msg);

        //write to audit table so we can find out
        Connection connection = ConnectionManager.getAuditConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO tpp_skipped_srcode (service_id, system_id, exchange_id, dt_skipped) VALUES (?, ?, ?, ?)";
            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setString(col++, fhirFiler.getServiceId().toString());
            ps.setString(col++, fhirFiler.getSystemId().toString());
            ps.setString(col++, fhirFiler.getExchangeId().toString());
            ps.setTimestamp(col++, new java.sql.Timestamp(new Date().getTime()));

            ps.executeUpdate();
            connection.commit();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }*/

}
