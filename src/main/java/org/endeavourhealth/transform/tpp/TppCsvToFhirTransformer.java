package org.endeavourhealth.transform.tpp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.cache.*;
import org.endeavourhealth.transform.tpp.csv.transforms.Patient.SRPatientTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SRCcgTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SROrganisationBranchTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SROrganisationTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.admin.SRTrustTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.SRAppointmentFlagsTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.SRAppointmentTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.SRRotaTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.clinical.*;
import org.endeavourhealth.transform.tpp.csv.transforms.codes.*;
import org.endeavourhealth.transform.tpp.csv.transforms.referral.SRReferralOutStatusDetailsTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.referral.SRReferralOutTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberProfileTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.*;

public abstract class TppCsvToFhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(TppCsvToFhirTransformer.class);

    //public static final String DATE_FORMAT = "dd MMM yyyy";
    public static final String DATE_FORMAT = "mm/dd/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss a";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();

    public static final String VERSION_87 = "87";
    public static final String VERSION_88 = "88";

    private static Set<String> cachedFileNamesToIgnore = null; //set of file names we know contain data but are deliberately ignoring

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors) throws Exception {

        String[] files = ExchangeHelper.parseExchangeBodyIntoFileList(exchangeBody);
        LOG.info("Invoking TPP CSV transformer for " + files.length + " files and service " + serviceId);

        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);
        Map<Class, AbstractCsvParser> parsers = new HashMap<>();

        //work out the version of the files by checking the headers
        String version = determineVersion(files);

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            createParsers(serviceId, systemId, exchangeId, version, files, parsers);

            LOG.trace("Transforming TPP CSV content in " + orgDirectory);
            transformParsers(parsers, fhirResourceFiler);

        } finally {
            closeParsers(parsers.values());
        }

        LOG.trace("Completed transform for TPP service " + serviceId + " - waiting for resources to commit to DB");
        fhirResourceFiler.waitToFinish();


    }


    /**
     * the TPP schema changes without notice, so rather than define the version in the SFTP reader,
     * and then need to back pedal when we find it's changed, dynamically work it out from the CSV headers
     */
    public static String determineVersion(String[] files) throws Exception {

        List<String> possibleVersions = new ArrayList<>();
        possibleVersions.add(VERSION_88);
        possibleVersions.add(VERSION_87);

        for (String filePath: files) {

            try {
                //create a parser for the file but with a null version, which will be fine since we never actually parse any data from it
                AbstractCsvParser parser = createParserForFile(null, null, null, null, filePath);

                //calling this will return the possible versions that apply to this parser
                possibleVersions = parser.testForValidVersions(possibleVersions);
                if (possibleVersions.isEmpty()) {
                    break;
                }

            } catch (ClassNotFoundException ex) {
                //we don't have parsers for every file, since there are a lot of secondary-care (etc.) files
                //that we don't transform, but we also want to make sure that they're EMPTY unless we explicitly
                //have decided to ignore a non-empty file
                ensureFileIsEmpty(filePath);
            }
        }

        //if we end up with one or more possible versions that do apply, then
        //return the first, since that'll be the most recent one
        if (!possibleVersions.isEmpty()) {
            return possibleVersions.get(0);
        }

        throw new TransformException("Unable to determine version for TPP CSV");
    }

    private static void createParsers(UUID serviceId, UUID systemId, UUID exchangeId, String version, String[] files, Map<Class, AbstractCsvParser> parsers) throws Exception {

        for (String filePath: files) {

            try {
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
            set.add("SRGPPracticeHistory");
            set.add("SRMappingGroup");
            set.add("SRPatientInformation");
            set.add("SRQuestionnaire");


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

        } else if (fileName.equals("SRStaff")
                || fileName.equals("SRStaffMember")
                || fileName.equals("SRStaffMemberProfile")
                || fileName.equals("SRStaffMemberProfileRole")
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

        String sharingAgreementGuid = "";  //findDataSharingAgreementGuid(parsers);

        TppCsvHelper csvHelper = new TppCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), sharingAgreementGuid, true);

        LOG.trace("Starting pre-transforms to cache data");
        // Consultations (Events)
        SREventPreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        // Codes
        SRCodePreTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        // EventLink
        SREventLinkTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        // Medical Record Status
        SRRecordStatusTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        LOG.trace("Starting admin transforms");
        // Code lookups
        SRMappingTransformer.transform(parsers, fhirResourceFiler);
        SRConfiguredListOptionTransformer.transform(parsers, fhirResourceFiler);
        SRMedicationReadCodeDetailsTransformer.transform(parsers, fhirResourceFiler);
        SRCtv3HierarchyTransformer.transform(parsers, fhirResourceFiler);
        SRCtv3Transformer.transform(parsers, fhirResourceFiler);
        // Staff
        SRStaffMemberTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRStaffMemberProfileTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        PractitionerResourceCache.filePractitionerResources(fhirResourceFiler);
        // Appointment sessions (Rotas)
        SRRotaTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        // Organisations
        SRCcgTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRTrustTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SROrganisationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SROrganisationBranchTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        LOG.trace("Starting patient transforms");
        SRPatientTransformer.transform(parsers, fhirResourceFiler,csvHelper);
        PatientResourceCache.filePatientResources(fhirResourceFiler);

        LOG.trace("Starting appointment transforms");
        SRAppointmentTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRAppointmentFlagsTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SlotResourceCache.fileSlotResources(fhirResourceFiler);
        AppointmentResourceCache.fileAppointmentResources(fhirResourceFiler);

        LOG.trace("Starting clinical transforms");
        SREventTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRVisitTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        // medication (repeats first, then acutes and issues/orders)
        SRRepeatTemplateTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRPrimaryCareMedicationTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        // referrals
        SRReferralOutTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRReferralOutStatusDetailsTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        ReferralRequestResourceCache.fileReferralRequestResources(fhirResourceFiler);

        // problems and codes (observations)
        SRProblemTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRCodeTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        ConditionResourceCache.fileConditionResources(fhirResourceFiler);

        // drug allergies
        SRDrugSensitivityTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        // Immunisations (content first, then immunisations)
        SRImmunisationContentTransformer.transform(parsers, fhirResourceFiler);
        SRImmunisationTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRImmunisationConsentTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        // Media (documents - this is just a reference to documents that we are not getting so ignoring for now
        SRMediaTransformer.transform(parsers, fhirResourceFiler, csvHelper);

        // Child at risk
        SRChildAtRiskTransformer.transform(parsers, fhirResourceFiler, csvHelper);

    }
}
