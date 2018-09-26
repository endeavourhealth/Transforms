package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

import static org.endeavourhealth.transform.vision.transforms.JournalTransformer.*;

public class JournalPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(JournalPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file is a critical error
        AbstractCsvParser parser = parsers.get(Journal.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processLine((Journal) parser, csvHelper, fhirResourceFiler, version);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }
    }


    private static void processLine(Journal parser, VisionCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            return;
        }

        ResourceType resourceType = getTargetResourceType(parser);
        CsvCell observationID = parser.getObservationID();
        CsvCell patientID = parser.getPatientID();
        String readCode = parser.getReadCode().getString();
        if (!Strings.isNullOrEmpty(readCode)) {
            readCode = readCode.substring(0,5);
        }

        //if it is not a problem itself, cache the Observation or Medication linked problem to be filed with the condition resource
        if (resourceType != ResourceType.Condition) {
            //extract actual problem links
            String problemLinkIDs = extractProblemLinkIDs(parser.getLinks().getString(), patientID.getString(), csvHelper);
            if (!Strings.isNullOrEmpty(problemLinkIDs)) {
                String[] linkIDs = problemLinkIDs.split("[|]");
                for (String problemID : linkIDs) {
                    //store the problem/observation relationship in the helper
                    csvHelper.cacheProblemRelationship(problemID,
                            patientID.getString(),
                            observationID.getString(),
                            resourceType,
                            parser.getLinks());
                }
            }
        }

        //linked consultation encounter record
        String consultationID = extractEncounterLinkID (parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            csvHelper.cacheNewConsultationChildRelationship(consultationID,
                    patientID.getString(),
                    observationID.getString(),
                    resourceType,
                    parser.getLinks());
        }

        //medication issue record - set linked drug record first and last issue dates
        if (resourceType == ResourceType.MedicationOrder) {
            String drugRecordID = extractDrugRecordLinkID (parser.getLinks().getString(), patientID.getString(), csvHelper);
            if (!Strings.isNullOrEmpty(drugRecordID)) {
                Date effectiveDate = parser.getEffectiveDateTime().getDate();
                String effectiveDatePrecision = "YMD";
                DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
                csvHelper.cacheDrugRecordDate(drugRecordID, patientID, dateTime);
            }
        }

        //try to get Ethnicity from Journal
        if (readCode.startsWith("9S") || readCode.startsWith("9i")) {
            Date effectiveDate = parser.getEffectiveDateTime().getDate();
            String effectiveDatePrecision = "YMD";
            DateTimeType fhirDate = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            EthnicCategory ethnicCategory = findEthnicityCode(readCode);
            if (ethnicCategory != null) {

                LOG.debug("PatientId: "+patientID.getString()+", Date: "+fhirDate.asStringValue()+", Ethnicity: "+ethnicCategory.getDescription());
                csvHelper.cacheEthnicity(patientID, fhirDate, ethnicCategory);
            } else {
                LOG.debug("Unmapped ethnic code: "+readCode+" for PatientID: "+patientID.getString());
            }
        }

        //try to get Marital status from Journal
        if (readCode.startsWith("133")) {
            Date effectiveDate = parser.getEffectiveDateTime().getDate();
            String effectiveDatePrecision = "YMD";
            DateTimeType fhirDate = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            MaritalStatus maritalStatus = findMaritalStatus(readCode);
            if (maritalStatus != null) {
                csvHelper.cacheMaritalStatus(patientID, fhirDate, maritalStatus);
            }
        }
    }

    private static MaritalStatus findMaritalStatus(String readCode) {
        if (Strings.isNullOrEmpty(readCode)) {
            return null;
        }
        if (readCode.equals("1331.")) {
            //single
            return MaritalStatus.NEVER_MARRIED;
        } else if (readCode.equals("1332.")
            || readCode.equals("133S.")) {
            return MaritalStatus.MARRIED;
        } else if (readCode.equals("1334.")
            || readCode.equals("133T.")) {
            return MaritalStatus.DIVORCED;
        } else if (readCode.equals("1335.")
            || readCode.equals("133C.")
            || readCode.equals("133V.")) {
            return MaritalStatus.WIDOWED;
        } else if (readCode.equals("1333.")) {
            return MaritalStatus.LEGALLY_SEPARATED;
        } else if (readCode.equals("1336.")
            || readCode.equals("133e.")
            || readCode.equals("133G.")
            || readCode.equals("133H.")) {
            return MaritalStatus.DOMESTIC_PARTNER;
        }

        return null;
    }

    //Vision use Ethnic groups and 2001 census categories so map from Read code
    private static EthnicCategory findEthnicityCode(String readCode) {
        if (Strings.isNullOrEmpty(readCode)) {
            return null;
        }
        if (readCode.startsWith("9S10") || readCode.startsWith("9i00")) {
            return EthnicCategory.WHITE_BRITISH;
        } else if (readCode.startsWith("9S11") || readCode.startsWith("9i10")) {
            return EthnicCategory.WHITE_IRISH;
        } else if (readCode.startsWith("9S12") || readCode.startsWith("9i2")) {
            return EthnicCategory.OTHER_WHITE;
        } else if (readCode.startsWith("9SB5") || readCode.startsWith("9i3")) {
            return EthnicCategory.MIXED_CARIBBEAN;
        } else if (readCode.startsWith("9SB6") || readCode.startsWith("9i4")) {
            return EthnicCategory.MIXED_AFRICAN;
        } else if (readCode.startsWith("9SB2") || readCode.startsWith("9i5")) {
            return EthnicCategory.MIXED_ASIAN;
        } else if (readCode.startsWith("9SB.") || readCode.startsWith("9i6"))  {
            return EthnicCategory.OTHER_MIXED;
        } else if (readCode.startsWith("9S6") || readCode.startsWith("9i7")) {
            return EthnicCategory.ASIAN_INDIAN;
        } else if (readCode.startsWith("9S7") || readCode.startsWith("9i8")) {
            return EthnicCategory.ASIAN_PAKISTANI;
        } else if (readCode.startsWith("9S8") || readCode.startsWith("9i9")) {
            return EthnicCategory.ASIAN_BANGLADESHI;
        } else if (readCode.startsWith("9SH") || readCode.startsWith("9iA")) {
            return EthnicCategory.OTHER_ASIAN;
        } else if (readCode.startsWith("9S2") || readCode.startsWith("9iB")) {
            return EthnicCategory.BLACK_CARIBBEAN;
        } else if (readCode.startsWith("9S3") || readCode.startsWith("9iC")) {
            return EthnicCategory.BLACK_AFRICAN;
        } else if (readCode.startsWith("9S4") || readCode.startsWith("9iD")) {
            return EthnicCategory.OTHER_BLACK;
        } else if (readCode.startsWith("9S9") || readCode.startsWith("9iE")) {
            return EthnicCategory.CHINESE;
        } else if (readCode.startsWith("9SJ") || readCode.startsWith("9iF")) {
            return EthnicCategory.OTHER;
        } else if (readCode.startsWith("9SE") || readCode.startsWith("9iG")) {
            return EthnicCategory.NOT_STATED;
        } else {
            return null;
        }
    }

}


