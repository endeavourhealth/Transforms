package org.endeavourhealth.transform.emis;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpStatus;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisAdminCacheDalI;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisPatientFiler;
import org.endeavourhealth.transform.emis.csv.transforms.admin.*;
import org.endeavourhealth.transform.emis.csv.transforms.agreements.SharingOrganisationTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.appointment.*;
import org.endeavourhealth.transform.emis.csv.transforms.careRecord.*;
import org.endeavourhealth.transform.emis.csv.transforms.coding.ClinicalCodeTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.coding.DrugCodeTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class EmisCsvToFhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCsvToFhirTransformer.class);

    public static final String VERSION_5_4 = "5.4"; //version being received live from Emis as of Dec 2016
    public static final String VERSION_5_3 = "5.3"; //version being received live from Emis as of Nov 2016
    public static final String VERSION_5_1 = "5.1"; //version received in official emis test pack
    public static final String VERSION_5_0 = "5.0"; //assumed version received prior to emis test pack (not sure of actual version number)

    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();   //EMIS csv files always contain a header

    public static void transform(Exchange exchange, FhirResourceFiler processor, String version) throws Exception {

        //get service
        UUID serviceId = processor.getServiceId();
        Service service = DalProvider.factoryServiceDal().getById(serviceId);

        //get files to process
        String exchangeBody = exchange.getBody();
        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        ExchangeHelper.filterFileTypes(files, service, processor.getExchangeId());
        LOG.info("Invoking EMIS CSV transformer for " + files.size() + " files and service " + service.getName() + " " + service.getId());

        if (files.isEmpty()) {
            LOG.info("No files, so returning out");
            return;
        }

        //see if we're filtering on any specific patients
        EmisPatientFiler patientFilter = EmisPatientFiler.factory(exchange);

        //we ignore the version already set in the exchange header, as Emis change versions without any notification,
        //so we dynamically work out the version when we load the first set of files
        version = determineVersion(files);

        ExchangePayloadFile.validateFilesAreInSameDirectory(files);
        Map<Class, AbstractCsvParser> parsers = new HashMap<>();

        try {
            createParsers(processor.getServiceId(), processor.getSystemId(), processor.getExchangeId(), files, version, parsers);
            transformParsers(version, parsers, processor, patientFilter);

        } finally {
            closeParsers(parsers.values());
        }
    }

    private static Set<String> findFilteringPatientGuids(Exchange exchange) throws Exception {

        List<String> patientGuids = exchange.getHeaderAsStringList(HeaderKeys.EmisPatientGuids);
        if (patientGuids == null) {
            return null;
        }

        return new HashSet<>(patientGuids);
    }

    /**
     * works out if we want to process (i.e. transform and store) the patient data from this extract,
     * which we don't if this extract is from before we received a later re-bulk from emis
     */
    public static boolean shouldProcessPatientData(FhirResourceFiler fhirResourceFiler) throws Exception {

        ServiceDalI serviceDal = DalProvider.factoryServiceDal();
        Service service = serviceDal.getById(fhirResourceFiler.getServiceId());
        String odsCode = service.getLocalId();
        Date startDate = findStartDate(odsCode);

        Date extractDate = fhirResourceFiler.getDataDate();

        if (startDate == null
                || !extractDate.before(startDate)) {
            //LOG.trace("Processing patient data for extract " + extractDate + " for " + service.getName() + " " + odsCode + " as this is on or after their start date of " + startDate);
            return true;

        } else {
            LOG.info("Not processing patient data for extract " + extractDate + " for " + service.getName() + " " + odsCode + " as this is before their start date of " + startDate);
            return false;
        }
    }

    private static Date findStartDate(String odsCode) throws Exception {

        //the start date for each service is in the Emis config record. Note that not all orgs have
        //a start date - this is only needed for ones where there were lots of re-bulks or they
        //we disabled for a long period of time, and this lets us ignore all the bad data and only
        //start processing from when we know it was right.
        String startDateStr = TransformConfig.instance().getEmisStartDate(odsCode);
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


    public static void createParsers(UUID serviceId, UUID systemId, UUID exchangeId, List<ExchangePayloadFile> files, String version, Map<Class, AbstractCsvParser> parsers) throws Exception {

        for (ExchangePayloadFile fileObj : files) {

            AbstractCsvParser parser = createParserForFile(serviceId, systemId, exchangeId, version, fileObj);
            Class cls = parser.getClass();
            parsers.put(cls, parser);
        }
    }

    @SuppressWarnings("unchecked")
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

    /*private static Date findExtractDate(String filePath) throws Exception {
        String name = FilenameUtils.getBaseName(filePath);
        String[] toks = name.split("_");
        if (toks.length != 5) {
            throw new TransformException("Failed to find extract date in filename " + filePath);
        }
        String dateStr = toks[3];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.parse(dateStr);
    }*/

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler,
                                         EmisPatientFiler patientFilter) throws Exception {

        String sharingAgreementGuid = findDataSharingAgreementGuid(parsers);

        EmisCsvHelper csvHelper = new EmisCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), sharingAgreementGuid, parsers);

        boolean processPatientData = shouldProcessPatientData(fhirResourceFiler);
        csvHelper.setProcessPatientData(processPatientData);
        csvHelper.setPatientFilter(patientFilter);

        /*ExchangeDalI exchangeDal = DalProvider.factoryExchangeDal();
        UUID firstExchangeId = exchangeDal.getFirstExchangeId(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId());
        if (firstExchangeId.equals(fhirResourceFiler.getExchangeId())) {
            LOG.trace("Applying admin resource cache for service {} and system {}", fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId());

            EmisAdminCacheFiler adminHelper = new EmisAdminCacheFiler(csvHelper.getDataSharingAgreementGuid());
            adminHelper.applyAdminResourceCache(fhirResourceFiler);
            AuditWriter.writeExchangeEvent(fhirResourceFiler.getExchangeId(), "Applied Emis Admin Resource Cache");
        }*/

        //check the sharing agreement to see if it's been disabled
        SharingOrganisationTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        //these transforms don't create resources themselves, but cache data that the subsequent ones rely on
        ClinicalCodeTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        DrugCodeTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        csvHelper.queueExchangesForFoundMissingCodes(); //if we've found some missing codes, re-queue exchanges

        boolean processAdminData = true;

        //massive hack to allow the clinical observations to be processed faster - audit skipping it so we can come back later
        /*if (TransformConfig.instance().isEmisSkipAdminData()) {
            auditSkippingAdminData(fhirResourceFiler);
            processAdminData = false;
        }*/

        if (processAdminData) {

            //create our starting admin resources if required
            csvHelper.getAdminHelper().applyAdminResourceCacheIfRequired(fhirResourceFiler, csvHelper);

            LOG.trace("Starting orgs, locations and user transforms");
            OrganisationLocationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            LocationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            OrganisationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            ObservationPreTransformer2.transform(parsers, fhirResourceFiler, csvHelper); //finds user IDs that are referenced
            IssueRecordPreTransformer2.transform(parsers, fhirResourceFiler, csvHelper); //finds user IDs that are referenced
            SessionUserPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //SD-283 - finds user IDs that are referenced

            UserInRoleTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            csvHelper.getAdminHelper().processAdminChanges(fhirResourceFiler, csvHelper);

            //appointments
            LOG.trace("Starting appointments transforms");
            SessionUserTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SessionTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        }

        //if this extract is one of the ones from BEFORE we got a subsequent re-bulk, we don't want to process
        //the patient data in the extract, as we know we'll be getting a later extract saying to delete it and then
        //another extract to replace it
        if (processPatientData) {

            LOG.trace("Starting patient pre-transforms");
            PatientPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //generate patient UUIDs and cache reg status data
            ProblemPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            ObservationPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            IssueRecordPreTransformer.transform(parsers, fhirResourceFiler, csvHelper); //must be done before DrugRecord pre-transformer
            DrugRecordPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            DiaryPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            ConsultationPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SlotPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            //note the order of these transforms is important, as consultations should be before obs etc.
            LOG.trace("Starting patient transforms");
            PatientTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            SlotTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            csvHelper.clearCachedSessionPractitioners(); //clear this down as it's a huge memory sink

            ConsultationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            IssueRecordTransformer.transform(parsers, fhirResourceFiler, csvHelper); //must be before DrugRecord
            DrugRecordTransformer.transform(parsers, fhirResourceFiler, csvHelper);

            DiaryTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            ObservationReferralTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            ProblemTransformer.transform(parsers, fhirResourceFiler, csvHelper);
            ObservationTransformer.transform(parsers, fhirResourceFiler, csvHelper);

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

    /*private static void auditSkippingAdminData(HasServiceSystemAndExchangeIdI fhirFiler) throws Exception {

        LOG.info("Skipping admin data for exchange " + fhirFiler.getExchangeId());
        AuditWriter.writeExchangeEvent(fhirFiler.getExchangeId(), "Skipped admin data");

        //write to audit table so we can find out
        Connection connection = ConnectionManager.getAuditConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO skipped_admin_data (service_id, system_id, exchange_id, dt_skipped) VALUES (?, ?, ?, ?)";
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
