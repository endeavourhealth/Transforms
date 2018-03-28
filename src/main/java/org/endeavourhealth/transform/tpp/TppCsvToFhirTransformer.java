package org.endeavourhealth.transform.tpp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.cache.PatientResourceCache;
import org.endeavourhealth.transform.tpp.cache.PractitionerResourceCache;
import org.endeavourhealth.transform.tpp.cache.ReferralRequestResourceCache;
import org.endeavourhealth.transform.tpp.csv.transforms.Patient.SRPatientTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.referral.SRReferralOutStatusDetailsTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.referral.SRReferralOutTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberProfileTransformer;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.SRStaffMemberTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

public abstract class TppCsvToFhirTransformer {

       //

    private static final Logger LOG = LoggerFactory.getLogger(TppCsvToFhirTransformer.class);


    public static final String DATE_FORMAT = "dd MMM yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();   //EMIS csv files always contain a header

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors) throws Exception {

        String[] files = ExchangeHelper.parseExchangeBodyIntoFileList(exchangeBody);

        LOG.info("Invoking TPP CSV transformer for " + files.length + " files and service " + serviceId);

        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        FhirResourceFiler fhirResourceFiler = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

        Map<Class, AbstractCsvParser> parsers = new HashMap<>();
        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            createParsers(serviceId, systemId, exchangeId, files, parsers);

            LOG.trace("Transforming TPP CSV content in " + orgDirectory);
            transformParsers(parsers, fhirResourceFiler);

        } finally {
            closeParsers(parsers.values());
        }

        LOG.trace("Completed transform for TPP service " + serviceId + " - waiting for resources to commit to DB");
        fhirResourceFiler.waitToFinish();

    }

    private static void createParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, Map<Class, AbstractCsvParser> parsers) throws Exception {

        for (String filePath: files) {

            AbstractCsvParser parser = createParserForFile(serviceId, systemId, exchangeId, filePath);
            Class cls = parser.getClass();
            parsers.put(cls, parser);
        }
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

    private static AbstractCsvParser createParserForFile(UUID serviceId, UUID systemId, UUID exchangeId, String filePath) throws Exception {

        String fName = FilenameUtils.getName(filePath);
        String domain = getDomainFromFileName(fName);
        String clsName = "org.endeavourhealth.transform.tpp.csv.schema." + domain + "." + fName;
        Class cls = Class.forName(clsName);

        //now construct an instance of the parser for the file we've found
        Constructor<AbstractCsvParser> constructor = cls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
        return constructor.newInstance(serviceId, systemId, exchangeId, filePath);
    }

    private static String getDomainFromFileName (String fileName) {

        if (fileName.startsWith("SRStaff")) {
            return "staff";
        } else if (fileName.startsWith("SRReferral")) {
            return "referral";
        } else if (fileName.startsWith("SRPatient")) {
            return "patient";
        } else if (fileName.startsWith("SRMedia")
                || fileName.startsWith("SRLetter")) {
            return "documents";
        } else if (fileName.startsWith("SRAppointment")
                || fileName.startsWith("SRRota")) {
            return "appointment";
        } else if (fileName.startsWith("SRAddressBook")
                || fileName.startsWith("SROrganisation")
                || fileName.startsWith("SRConfigured")) {
            return "admin";
        } else if (fileName.startsWith("SRCode")
                || fileName.startsWith("SRCtv3")
                || fileName.startsWith("SRTemplate")) {
            return "codes";
        } else
            return "clinical";
    }

    private static void transformParsers(Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler) throws Exception {

        String sharingAgreementGuid = "";  //findDataSharingAgreementGuid(parsers);

        TppCsvHelper csvHelper = new TppCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), sharingAgreementGuid, true);

        LOG.trace("Starting pre-transforms to cache data");

        LOG.trace("Starting admin transforms");
        // Staff
        SRStaffMemberTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRStaffMemberProfileTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        PractitionerResourceCache.filePractitionerResources(fhirResourceFiler);

        LOG.trace("Starting patient transforms");
        SRPatientTransformer.transform(parsers, fhirResourceFiler,csvHelper);
        PatientResourceCache.filePatientResources(fhirResourceFiler);

        LOG.trace("Starting clinical transforms");
        SRReferralOutTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        SRReferralOutStatusDetailsTransformer.transform(parsers, fhirResourceFiler, csvHelper);
        ReferralRequestResourceCache.fileReferralRequestResources(fhirResourceFiler);
    }
}
