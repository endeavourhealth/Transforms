package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Date;
import java.util.Map;

import static org.endeavourhealth.transform.vision.transforms.JournalTransformer.extractEncounterLinkID;
import static org.endeavourhealth.transform.vision.transforms.JournalTransformer.getTargetResourceType;

public class JournalPreTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file is a critical error
        AbstractCsvParser parser = parsers.get(Journal.class);
        while (parser.nextRecord()) {

            try {
                processLine((Journal)parser, csvHelper, fhirResourceFiler, version);
            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }


    private static void processLine(Journal parser, VisionCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        if (parser.getAction().equalsIgnoreCase("D")) {
            return;
        }

        ResourceType resourceType = getTargetResourceType(parser, csvHelper);

        //get the Journal linked items
        String[] links = null;
        if (!Strings.isNullOrEmpty(parser.getLinks())) {
            links = parser.getLinks().split("|");
        }
        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();
        String readCode = parser.getReadCode();

        //TODO:// Are parent observation supported in Vision?

        //cache the fact an observation is a problem
        if (resourceType == ResourceType.Condition) {
            csvHelper.cacheProblemObservationGuid(patientID, observationID, readCode);
        } else {
            //if it is not a problem, cache the observation linked problem
            if (links != null) {
                String problemID = links[0];    //TODO: how determine the link is a problem, i.e. this is a medication resource with a linked problem?
                if (!Strings.isNullOrEmpty(problemID)) {
                    //if this record is linked to a problem, store this relationship in the helper
                    csvHelper.cacheProblemRelationship(problemID,
                            patientID,
                            observationID,
                            resourceType);
                }
            }
        }


        //linked consultation encounter record
        if (links != null) {
            String consultationID = extractEncounterLinkID (links);
            if (!Strings.isNullOrEmpty(consultationID)) {
                csvHelper.cacheConsultationRelationship(consultationID,
                        patientID,
                        observationID,
                        resourceType);
            }
        }

        //medication issue record - set linked drug record first and last issue dates
        if (resourceType == ResourceType.MedicationOrder) {
            if (links != null) {
                String drugRecordID = links[0];   //TODO:  if it is a medication issue, how determine linked drug statement?
                Date effectiveDate = parser.getEffectiveDateTime();
                String effectiveDatePrecision = "YMD";
                DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
                csvHelper.cacheDrugRecordDate(drugRecordID, patientID, dateTime);
            }
        }

        //try to get Ethicity from Journal
        if (readCode.startsWith("9i")) {
            Date effectiveDate = parser.getEffectiveDateTime();
            String effectiveDatePrecision = "YMD";
            DateTimeType fhirDate = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            EthnicCategory ethnicCategory = findEthnicityCode(readCode);
            if (ethnicCategory != null) {
                csvHelper.cacheEthnicity(patientID, fhirDate, ethnicCategory);
            }
        }

        //try to get Marital status from Journal
        if (readCode.startsWith("133")) {
            Date effectiveDate = parser.getEffectiveDateTime();
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

    private static EthnicCategory findEthnicityCode(String readCode) {
        if (Strings.isNullOrEmpty(readCode)) {
            return null;
        }
        if (readCode.startsWith("9i0")) {
            return EthnicCategory.WHITE_BRITISH;
        } else if (readCode.startsWith("9i1")) {
            return EthnicCategory.WHITE_IRISH;
        } else if (readCode.startsWith("9i2")) {
            return EthnicCategory.OTHER_WHITE;
        } else if (readCode.startsWith("9i3")) {
            return EthnicCategory.MIXED_CARIBBEAN;
        } else if (readCode.startsWith("9i4")) {
            return EthnicCategory.MIXED_AFRICAN;
        } else if (readCode.startsWith("9i5")) {
            return EthnicCategory.MIXED_ASIAN;
        } else if (readCode.startsWith("9i6")) {
            return EthnicCategory.OTHER_MIXED;
        } else if (readCode.startsWith("9i7")) {
            return EthnicCategory.ASIAN_INDIAN;
        } else if (readCode.startsWith("9i8")) {
            return EthnicCategory.ASIAN_PAKISTANI;
        } else if (readCode.startsWith("9i9")) {
            return EthnicCategory.ASIAN_BANGLADESHI;
        } else if (readCode.startsWith("9iA")) {
            return EthnicCategory.OTHER_ASIAN;
        } else if (readCode.startsWith("9iB")) {
            return EthnicCategory.BLACK_CARIBBEAN;
        } else if (readCode.startsWith("9iC")) {
            return EthnicCategory.BLACK_AFRICAN;
        } else if (readCode.startsWith("9iD")) {
            return EthnicCategory.OTHER_BLACK;
        } else if (readCode.startsWith("9iE")) {
            return EthnicCategory.CHINESE;
        } else if (readCode.startsWith("9iF")) {
            return EthnicCategory.OTHER;
        } else if (readCode.startsWith("9iG")) {
            return EthnicCategory.NOT_STATED;
        } else {
            return null;
        }
    }

}


