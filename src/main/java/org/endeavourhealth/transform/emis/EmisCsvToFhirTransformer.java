package org.endeavourhealth.transform.emis;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.transforms.admin.*;
import org.endeavourhealth.transform.emis.csv.transforms.agreements.SharingOrganisationTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.appointment.SessionTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.appointment.SessionUserTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.appointment.SlotTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.careRecord.*;
import org.endeavourhealth.transform.emis.csv.transforms.coding.ClinicalCodeTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.coding.DrugCodeTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.DrugRecordPreTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.DrugRecordTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.IssueRecordPreTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.IssueRecordTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class EmisCsvToFhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCsvToFhirTransformer.class);

    public static final String VERSION_5_4 = "5.4"; //version being received live from Emis as of Dec 2016
    public static final String VERSION_5_3 = "5.3"; //version being received live from Emis as of Nov 2016
    public static final String VERSION_5_1 = "5.1"; //version received in official emis test pack
    public static final String VERSION_5_0 = "5.0"; //assumed version received prior to emis test pack (not sure of actual version number)

    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd"; //EMIS spec says "dd/MM/yyyy", but test data is different
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();   //EMIS csv files always contain a header

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds) throws Exception {

        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        LOG.info("Invoking EMIS CSV transformer for " + files.size() + " files and service " + serviceId);

        //we ignore the version already set in the exchange header, as Emis change versions without any notification,
        //so we dynamically work out the version when we load the first set of files
        String version = determineVersion(files);

        ExchangePayloadFile.validateFilesAreInSameDirectory(files);
        boolean processPatientData = shouldProcessPatientData(serviceId, files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

        Map<Class, AbstractCsvParser> parsers = new HashMap<>();

        try {
            createParsers(serviceId, systemId, exchangeId, files, version, parsers);
            transformParsers(version, parsers, processor, processPatientData);

        } finally {
            closeParsers(parsers.values());
        }

        LOG.debug("Completed transform for service " + serviceId + " - waiting for resources to commit to DB");
        processor.waitToFinish();
    }

    /**
     * works out if we want to process (i.e. transform and store) the patient data from this extract,
     * which we don't if this extract is from before we received a later re-bulk from emis
     */
    public static boolean shouldProcessPatientData(UUID serviceId, List<ExchangePayloadFile> files) throws Exception {

        ServiceDalI serviceDal = DalProvider.factoryServiceDal();
        Service service = serviceDal.getById(serviceId);
        String odsCode = service.getLocalId();
        Date startDate = findStartDate(odsCode);

        //find the extract date from one of the CSV file names
        ExchangePayloadFile firstFileObj = files.get(0);
        Date extractDate = findExtractDate(firstFileObj.getPath());

        if (startDate == null
                || !extractDate.before(startDate)) {
            LOG.trace("Processing patient data for extract " + extractDate + " for " + service.getName() + " " + odsCode + " as this is on or after their start date of " + startDate);
            return true;

        } else {
            LOG.info("Not processing patient data for extract " + extractDate + " for " + service.getName() + " " + odsCode + " as this is before their start date of " + startDate);
            return false;
        }
    }

    private static Date findStartDate(String odsCode) throws Exception {

        Map<String, String> map = new HashMap<>();

        //this list of ODS codes and dates is based off the live Emis extracts, giving the most recent bulk date for each organisation
        //only practices where the extract started before the move to AWS and where the extract was disabled and re-bulked need to be in here.
        //Practices disabled and re-bulked since the move to AWS are handled differently.
        map.put("F84640", "20/07/2017");
        map.put("F84711", "15/10/2017");
        map.put("F84658", "09/08/2017");
        map.put("F84621", "05/05/2017");
        map.put("F84051", "31/01/2017");
        map.put("Y00092", "04/11/2017");
        map.put("F84033", "13/10/2017");
        map.put("F84021", "15/10/2017");
        map.put("F84601", "01/02/2017");
        map.put("F84004", "31/10/2018");
        map.put("F84003", "13/10/2017");
        map.put("F84641", "13/05/2017");
        map.put("F84088", "12/08/2017");
        map.put("F86011", "11/07/2017");
        map.put("F84666", "15/10/2017");
        map.put("F84657", "15/03/2017");
        map.put("F84041", "19/04/2017");
        map.put("Y04273", "20/04/2017");
        map.put("F84741", "27/04/2017");
        map.put("F84022", "17/10/2017");
        map.put("F84105", "20/04/2017");
        map.put("Y03023", "09/05/2017");
        map.put("F84017", "31/10/2018");
        map.put("F86679", "20/04/2017");
        map.put("F84046", "15/10/2017");
        map.put("F84635", "05/02/2017");
        map.put("F84083", "06/07/2017");
        map.put("F84696", "15/10/2017");
        map.put("F84030", "01/02/2017");
        map.put("F84724", "17/10/2017");
        map.put("F84718", "17/10/2017");
        map.put("F84025", "19/10/2017");
        map.put("F84706", "15/10/2017");
        map.put("F84031", "15/10/2017");
        map.put("F86030", "04/07/2017");
        map.put("Y00403", "21/09/2017");
        map.put("F86639", "10/06/2017");
        map.put("F84642", "15/10/2017");
        map.put("16456", "28/09/2017");
        map.put("F84698", "15/10/2017");
        map.put("Y03049", "13/10/2017");
        map.put("F84631", "11/01/2018");
        map.put("F84747", "01/02/2017");
        map.put("F84081", "19/02/2018");
        map.put("F84012", "19/10/2017");
        map.put("H85035", "04/02/2018");
        map.put("F86701", "08/06/2017");
        map.put("F84702", "15/10/2017");
        map.put("F84093", "15/10/2017");
        map.put("F86626", "08/06/2017");
        map.put("F84018", "15/10/2017");
        map.put("F84673", "15/10/2017");
        map.put("F84686", "13/11/2017");
        map.put("F86627", "11/05/2017");
        map.put("F84731", "15/10/2017");
        map.put("F84749", "27/09/2017");
        map.put("F86044", "05/05/2017");
        map.put("F86705", "19/10/2017");
        map.put("F84117", "07/06/2017");
        map.put("F84010", "19/10/2017");
        map.put("F84670", "21/02/2017");
        map.put("F84052", "20/07/2017");
        map.put("F84668", "15/10/2017");
        map.put("F84118", "01/02/2017");
        map.put("F84055", "15/10/2017");
        map.put("F84124", "20/04/2017");
        map.put("F84677", "15/10/2017");
        map.put("F84097", "26/04/2017");
        map.put("F84006", "15/10/2017");
        map.put("RWKGY", "01/06/2018");
        map.put("F84009", "26/08/2017");
        map.put("F84122", "31/01/2017");
        map.put("F84035", "15/10/2017");
        map.put("F84087", "15/10/2017");
        map.put("F84123", "15/10/2017");
        map.put("F84692", "02/12/2017");
        map.put("29605", "28/09/2017");
        map.put("F84047", "15/10/2017");
        map.put("F84072", "13/10/2017");
        map.put("F86644", "13/10/2017");
        map.put("F86058", "24/05/2017");
        map.put("F84719", "19/10/2017");
        map.put("F82660", "26/11/2018");
        map.put("F84647", "15/10/2017");
        map.put("F84074", "13/10/2017");
        map.put("F84636", "15/10/2017");
        map.put("F84121", "26/08/2017");
        map.put("F84656", "14/02/2017");
        map.put("F84036", "08/06/2017");
        map.put("F86006", "13/10/2017");
        map.put("F84669", "07/02/2018");
        map.put("F84044", "19/10/2017");
        map.put("F84716", "17/10/2017");
        map.put("F82002", "17/10/2018");
        map.put("F84054", "15/10/2017");
        map.put("F86712", "17/10/2017");
        map.put("F84039", "15/10/2017");
        map.put("F86621", "10/06/2017");
        map.put("Y02957", "20/04/2017");
        map.put("F84043", "21/08/2017");
        map.put("F84034", "15/10/2017");
        map.put("G85076", "13/10/2017");
        map.put("F84096", "28/01/2018");
        map.put("F84730", "13/10/2017");
        map.put("F84735", "30/09/2017");
        map.put("F84682", "01/02/2017");
        map.put("F84014", "09/10/2017");
        map.put("F84632", "13/10/2017");
        map.put("F84079", "15/10/2017");
        map.put("Y02928", "13/10/2017");
        map.put("F84080", "13/10/2017");
        map.put("F84086", "07/02/2018");
        map.put("G85715", "20/04/2017");
        map.put("F84114", "15/10/2017");
        map.put("F84733", "15/10/2017");
        map.put("F84013", "19/02/2018");
        map.put("G85119", "27/04/2017");
        map.put("Y00212", "01/02/2017");
        map.put("Y01962", "03/02/2018");
        map.put("F86666", "28/04/2017");
        map.put("F84676", "15/10/2017");
        map.put("F84660", "05/07/2017");
        map.put("F84038", "11/01/2018");
        map.put("F84015", "19/10/2017");
        map.put("F86689", "02/11/2017");
        map.put("F84739", "30/12/2017");
        map.put("F86038", "05/11/2017");
        map.put("G85053", "05/05/2017");
        map.put("F84016", "15/10/2017");
        map.put("Y02973", "20/04/2017");
        map.put("F84740", "15/08/2017");
        map.put("F84742", "10/05/2017");
        map.put("F84008", "13/10/2017");
        map.put("F83048", "13/10/2017");
        map.put("F84070", "15/10/2017");
        map.put("F84050", "07/02/2018");
        map.put("F84624", "20/04/2017");
        map.put("F84729", "19/11/2017");
        map.put("F84720", "20/10/2017");
        map.put("F84619", "27/04/2017");
        map.put("F84119", "13/10/2017");
        map.put("F84062", "15/10/2017");
        map.put("F84092", "19/10/2017");
        map.put("F84111", "03/05/2017");
        map.put("F84714", "15/10/2017");
        map.put("F86005", "08/07/2017");
        map.put("F84681", "13/10/2017");
        map.put("F82007", "26/11/2018");
        map.put("F84672", "15/10/2017");
        map.put("F84685", "19/10/2017");
        map.put("F82014", "15/12/2018");
        map.put("F82607", "26/12/2018");

        String startDateStr = map.get(odsCode);
        if (Strings.isNullOrEmpty(startDateStr)) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        return sdf.parse(startDateStr);
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

    /**
     * the Emis schema changes without notice, so rather than define the version in the SFTP reader,
     * we simply look at the files to work out what version it really is
     */
    public static String determineVersion(List<ExchangePayloadFile> files) throws Exception {

        List<String> possibleVersions = new ArrayList<>();
        possibleVersions.add(VERSION_5_4);
        possibleVersions.add(VERSION_5_3);
        possibleVersions.add(VERSION_5_1);
        possibleVersions.add(VERSION_5_0);

        for (ExchangePayloadFile fileObj : files) {

            //create a parser for the file but with a null version, which will be fine since we never actually parse any data from it
            AbstractCsvParser parser = createParserForFile(null, null, null, null, fileObj);

            //calling this will return the possible versions that apply to this parser
            possibleVersions = parser.testForValidVersions(possibleVersions);
            if (possibleVersions.isEmpty()) {
                break;
            }
        }

        //if we end up with one or more possible versions that do apply, then
        //return the first, since that'll be the most recent one
        if (!possibleVersions.isEmpty()) {
            return possibleVersions.get(0);
        }

        throw new TransformException("Unable to determine version for EMIS CSV");
    }
    /*public static String determineVersion(File dir) throws Exception {

        String[] versions = new String[]{VERSION_5_0, VERSION_5_1, VERSION_5_3, VERSION_5_4};
        Exception lastException = null;

        for (String version: versions) {

            Map<Class, AbstractCsvParser> parsers = new HashMap<>();
            try {
                validateAndOpenParsers(dir, version, true, parsers);

                //if we make it here, this version is the right one
                return version;

            } catch (Exception ex) {
                //ignore any exceptions, as they just mean the version is wrong, so try the next one
                lastException = ex;

            } finally {
                //make sure to close any parsers that we opened
                closeParsers(parsers.values());
            }
        }

        throw new TransformException("Unable to determine version for EMIS CSV", lastException);
    }*/


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


    public static void createParsers(UUID serviceId, UUID systemId, UUID exchangeId, List<ExchangePayloadFile> files, String version, Map<Class, AbstractCsvParser> parsers) throws Exception {

        for (ExchangePayloadFile fileObj : files) {

            AbstractCsvParser parser = createParserForFile(serviceId, systemId, exchangeId, version, fileObj);
            Class cls = parser.getClass();
            parsers.put(cls, parser);
        }
    }

    private static AbstractCsvParser createParserForFile(UUID serviceId, UUID systemId, UUID exchangeId, String version, ExchangePayloadFile fileObj) throws Exception {

        String fileType = fileObj.getType();
        String filePath = fileObj.getPath();

        String[] toks = fileType.split("_");

        String domain = toks[0];
        String name = toks[1];

        //need to camel case the domain
        String first = domain.substring(0, 1);
        String last = domain.substring(1);
        domain = first.toLowerCase() + last;

        String clsName = "org.endeavourhealth.transform.emis.csv.schema." + domain + "." + name;
        Class cls = Class.forName(clsName);

        //now construct an instance of the parser for the file we've found
        Constructor<AbstractCsvParser> constructor = cls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
        return constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);
    }

    public static String findDataSharingAgreementGuid(Map<Class, AbstractCsvParser> parsers) throws Exception {

        //we need a file name to work out the data sharing agreement ID, so just the first file we can find
        String firstFilePath = parsers
                .values()
                .iterator()
                .next()
                .getFilePath();

        String name = FilenameUtils.getBaseName(firstFilePath); //file name without extension
        String[] toks = name.split("_");
        if (toks.length != 5) {
            throw new TransformException("Failed to extract data sharing agreement GUID from filename " + firstFilePath);
        }
        return toks[4];
    }

    private static Date findExtractDate(String filePath) throws Exception {
        String name = FilenameUtils.getBaseName(filePath);
        String[] toks = name.split("_");
        if (toks.length != 5) {
            throw new TransformException("Failed to find extract date in filename " + filePath);
        }
        String dateStr = toks[3];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.parse(dateStr);
    }

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler,
                                         boolean processPatientData) throws Exception {

        String sharingAgreementGuid = findDataSharingAgreementGuid(parsers);

        EmisCsvHelper csvHelper = new EmisCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), sharingAgreementGuid, processPatientData);

        //if this is the first extract for this organisation, we need to apply all the content of the admin resource cache
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        if (!resourceDal.dataExists(fhirResourceFiler.getServiceId())) {
            LOG.trace("Applying admin resource cache for service {} and system {}", fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId());

            csvHelper.applyAdminResourceCache(fhirResourceFiler);
            AuditWriter.writeExchangeEvent(fhirResourceFiler.getExchangeId(), "Applied Emis Admin Resource Cache");
        }

        //check the sharing agreement to see if it's been disabled
        SharingOrganisationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //these transforms don't create resources themselves, but cache data that the subsequent ones rely on
        ClinicalCodeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        DrugCodeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        LOG.trace("Starting orgs, locations and user transforms");
        OrganisationLocationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        LocationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        OrganisationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        UserInRoleTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        csvHelper.processRemainingOrganisationLocationMappings(fhirResourceFiler); //process any changes to Org-Location links without a change to the Location itself

        //appointments
        LOG.trace("Starting appointments transforms");
        SessionUserTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        SessionTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        if (processPatientData) {
            SlotTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        }
        //if we have any changes to the staff in pre-existing sessions, we need to update the existing FHIR Schedules
        //Confirmed on Live data - we NEVER get an update to a session_user WITHOUT also an update to the session
        //csvHelper.processRemainingSessionPractitioners(fhirResourceFiler);
        csvHelper.clearCachedSessionPractitioners(); //clear this down as it's a huge memory sink

        //if this extract is one of the ones from BEFORE we got a subsequent re-bulk, we don't want to process
        //the patient data in the extract, as we know we'll be getting a later extract saying to delete it and then
        //another extract to replace it
        if (processPatientData) {

            LOG.trace("Starting patient pre-transforms");
            PatientPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ProblemPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ObservationPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            DrugRecordPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            IssueRecordPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            DiaryPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ConsultationPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

            //note the order of these transforms is important, as consultations should be before obs etc.
            LOG.trace("Starting patient transforms");
            PatientTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ConsultationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            IssueRecordTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            DrugRecordTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

            DiaryTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ObservationReferralTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ProblemTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ObservationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

            //if we have any new Obs, Conditions, Medication etc. that reference pre-existing parent obs or problems,
            //then we need to retrieve the existing resources and update them
            csvHelper.processRemainingObservationParentChildLinks(fhirResourceFiler);

            //process any new items linked to past consultations
            csvHelper.processRemainingNewConsultationRelationships(fhirResourceFiler);

            //process any changes to ethnicity or marital status, without a change to the Patient
            csvHelper.processRemainingEthnicitiesAndMartialStatuses(fhirResourceFiler);

            //process any changes to Problems that didn't have an associated Observation change too
            csvHelper.processRemainingProblems(fhirResourceFiler);

            //if we have any new Obs etc. that refer to pre-existing problems, we need to update the existing FHIR Problem
            csvHelper.processRemainingProblemRelationships(fhirResourceFiler);

            //update any MedicationStatements to set the last issue date on them
            csvHelper.processRemainingMedicationIssueDates(fhirResourceFiler);
        }

        //close down the utility thread pool
        csvHelper.stopThreadPool();
    }


    /*public static boolean findRecordsToProcess(Map<Class, AbstractCsvParser> allParsers, TransformError previousErrors) throws Exception {

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
    }*/


}
