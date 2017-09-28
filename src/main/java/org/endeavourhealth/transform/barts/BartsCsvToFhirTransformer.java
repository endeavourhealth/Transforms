package org.endeavourhealth.transform.barts;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.data.audit.AuditRepository;
import org.endeavourhealth.core.xml.TransformErrorUtility;
import org.endeavourhealth.core.xml.transformError.Error;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.barts.schema.Problem;
import org.endeavourhealth.transform.barts.schema.Sus;
import org.endeavourhealth.transform.barts.schema.Tails;
import org.endeavourhealth.transform.barts.transforms.ProblemTransformer;
import org.endeavourhealth.transform.barts.transforms.SusTransformer;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public abstract class BartsCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvToFhirTransformer.class);

    public static final String VERSION_1_0 = "1.0"; //initial version
    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd"; //EMIS spec says "dd/MM/yyyy", but test data is different
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;
    public static final String PRIMARY_ORG_ODS_CODE = "R1H";
    public static final String PRIMARY_ORG_HL7_OID = "2.16.840.1.113883.3.2540.1";

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors,
                                 String sharedStoragePath, int maxFilingThreads, String version) throws Exception {

        //for Barts CSV, the exchange body will be a list of files received
        //split by /n but trim each one, in case there's a sneaky /r in there
        String[] files = exchangeBody.split("\n");
        for (int i=0; i<files.length; i++) {
            String file = files[i].trim();
            files[i] = file;
        }

        LOG.info("Invoking Barts CSV transformer for {} files using {} threads and service {}", files.length, maxFilingThreads, serviceId);

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        File orgDirectory = validateAndFindCommonDirectory(sharedStoragePath, files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds, maxFilingThreads);

        //Map<Class, AbstractCsvParser> allParsers = new HashMap<>();

        LOG.trace("Transforming Barts CSV content in {}", orgDirectory);
        transformParsers(orgDirectory, version, processor, previousErrors, maxFilingThreads);

        LOG.trace("Completed transform for service {} - waiting for resources to commit to DB", serviceId);
        processor.waitToFinish();
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


    private static String findDataSharingAgreementGuid(Map<Class, AbstractFixedParser> parsers) throws Exception {

        //we need a file name to work out the data sharing agreement ID, so just the first file we can find
        File f = parsers
                .values()
                .iterator()
                .next()
                .getFile();

        return findDataSharingAgreementGuid(f);
    }

    public static String findDataSharingAgreementGuid(File f) throws Exception {
        String name = Files.getNameWithoutExtension(f.getName());
        String[] toks = name.split("_");
        if (toks.length != 5) {
            throw new TransformException("Failed to extract data sharing agreement GUID from filename " + f.getName());
        }
        return toks[4];
    }

    private static void transformParsers(File dir, String version,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors,
                                         int maxFilingThreads) throws Exception {

        //EmisCsvHelper csvHelper = new EmisCsvHelper(findDataSharingAgreementGuid(parsers));

        //if this is the first extract for this organisation, we need to apply all the content of the admin resource cache
        /*
        if (!new AuditRepository().isServiceStarted(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId())) {
            LOG.trace("Applying admin resource cache for service {} and system {}", fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId());
            csvHelper.applyAdminResourceCache(fhirResourceFiler);
        }
        */

        //these transforms don't create resources themselves, but cache data that the subsequent ones rely on
        for (File f: dir.listFiles()) {
            String fName = f.getName();
            String fileType = identifyFileType(fName);

            if ((fileType.compareTo("TAILOPA") == 0) || (fileType.compareTo("TAILIP") == 0) || (fileType.compareTo("TAILAEA") == 0)) {
                AbstractFixedParser parser = new Tails(version, f, true);
                // TODO
                //TailsPreTransformer.transform(version, parser, fhirResourceFiler, csvHelper);
            } else {
                if (fileType.compareTo("PREG") == 0) {
                    AbstractFixedParser parser = new Tails(version, f, true);
                    // TODO
                    //PregPreTransformer.transform(version, parser, fhirResourceFiler, csvHelper);
                }
            }
        }

        //before getting onto the files that actually create FHIR resources, we need to
        //work out what record numbers to process, if we're re-running a transform
        //boolean processingSpecificRecords = findRecordsToProcess(parsers, previousErrors);

        //run the transforms for non-patient resources
        // TODO - verify by i dont think there are any for Barts
        //LocationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //note the order of these transforms is important, as consultations should be before obs etc.
        for (File f2: dir.listFiles()) {
            String fName = f2.getName();
            String fileType = identifyFileType(fName);

            if ((fileType.compareTo("SUSOPA") == 0) || (fileType.compareTo("SUSIP") == 0) || (fileType.compareTo("SUSAEA") == 0)) {
                Sus parser = new Sus(version, f2, true);
                SusTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                parser.close();
            } else {
                if (fileType.compareTo("PROB") == 0) {
                    Problem parser = new Problem (version, f2, true);
                    ProblemTransformer.transform(version, parser, fhirResourceFiler, null, PRIMARY_ORG_ODS_CODE, PRIMARY_ORG_HL7_OID);
                    parser.close();
                }
            }
        }
    }

    private static String identifyFileType(String filename) {
        String[] parts = filename.split("_");
        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];
        if (filenamePart1.compareToIgnoreCase("susopa") == 0) {
            return "SUSOPA";
        } else {
            if (filenamePart1.compareToIgnoreCase("susaea") == 0) {
                return "SUSAEA";
            } else {
                if (filenamePart1.compareToIgnoreCase("tailopa") == 0) {
                    return filenamePart1.toUpperCase();
                } else {
                    if (filenamePart1.compareToIgnoreCase("tailaea") == 0) {
                        return filenamePart1.toUpperCase();
                    } else {
                        String filenamePart3 = parts[2];

                        if (filenamePart1.compareToIgnoreCase("tailip") == 0) {
                            return filenamePart1.toUpperCase();
                        } else {
                            String filenamePart4 = parts[3];

                            if (filenamePart1.compareToIgnoreCase("ip") == 0) {
                                return "SUSIP";
                            } else {
                                if (filenamePart1.compareToIgnoreCase("rnj") == 0) {
                                    return filenamePart3.toUpperCase();
                                } else {
                                    //String filenamePart5 = parts[4];
                                    String filenamePart6 = parts[5];

                                    if (filenamePart1.compareToIgnoreCase("GETL") == 0) {
                                        return filenamePart3.toUpperCase();
                                    } else {
                                        return "UNKNOWN";
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
