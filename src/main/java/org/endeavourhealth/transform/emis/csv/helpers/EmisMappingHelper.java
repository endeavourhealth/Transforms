package org.endeavourhealth.transform.emis.csv.helpers;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.fhir.schema.*;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisClinicalCode;
import org.endeavourhealth.transform.common.ResourceParser;

import java.util.Map;

public class EmisMappingHelper {

    private static Map<String, String> maritalStatusMap;
    private static Map<String, String> ethnicityMap;
    private static Map<String, String> referralPriorityMap;
    private static Map<String, String> referralModeMap;
    private static Map<String, String> referralTypeMap;
    private static Map<String, String> organisationTypeMap;
    private static Map<String, String> patientTypeMap;
    private static Map<String, String> drugRecordPrescriptionTypeMap;
    private static Map<String, String> problemSeverityMap;
    private static Map<String, String> problemRelationshipMap;

    public static MaritalStatus findMaritalStatus(EmisClinicalCode codeMapping) throws Exception {
        String read2Code = codeMapping.getAdjustedCode(); //use the adjusted code as it's padded to five chars
        if (Strings.isNullOrEmpty(read2Code)) {
            return null;
        }

        if (maritalStatusMap == null) {
            maritalStatusMap = ResourceParser.readCsvResourceIntoMap("EmisMaritalStatusMap.csv", "SourceCode", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = maritalStatusMap.get(read2Code);
        if (code == null) {
            throw new RuntimeException("Unknown marital status code " + read2Code);

        } else if (!Strings.isNullOrEmpty(code)) {
            return MaritalStatus.fromCode(code);

        } else {
            return null;
        }
    }


    public static EthnicCategory findEthnicityCode(EmisClinicalCode codeMapping) throws Exception {
        String read2Code = codeMapping.getAdjustedCode(); //use the adjusted code as it's padded to five chars
        if (Strings.isNullOrEmpty(read2Code)) {
            return null;
        }

        if (ethnicityMap == null) {
            ethnicityMap = ResourceParser.readCsvResourceIntoMap("EmisEthnicityMap.csv", "SourceCode", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = ethnicityMap.get(read2Code);
        if (code == null) {
            throw new RuntimeException("Unknown ethnicity code " + read2Code);

        } else if (!Strings.isNullOrEmpty(code)) {
            return EthnicCategory.fromCode(code);

        } else {
            return null;
        }
    }

    public static ReferralPriority findReferralPriority(String urgency) throws Exception {

        if (referralPriorityMap == null) {
            referralPriorityMap = ResourceParser.readCsvResourceIntoMap("EmisReferralPriorityMap.csv", "SourceUrgency", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = referralPriorityMap.get(urgency);
        if (code == null) {
            throw new RuntimeException("Unknown referral urgency " + urgency);

        } else if (!Strings.isNullOrEmpty(code)) {
            return ReferralPriority.fromCode(code);

        } else {
            return null;
        }
    }

    public static ReferralRequestSendMode findReferralMode(String mode) throws Exception {

        if (referralModeMap == null) {
            referralModeMap = ResourceParser.readCsvResourceIntoMap("EmisReferalModeMap.csv", "SourceMode", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = referralModeMap.get(mode);
        if (code == null) {
            throw new RuntimeException("Unknown referral mode " + mode);

        } else if (!Strings.isNullOrEmpty(code)) {
            return ReferralRequestSendMode.fromCode(code);

        } else {
            return null;
        }
    }

    public static ReferralType findReferralType(String type) throws Exception {

        if (referralTypeMap == null) {
            referralTypeMap = ResourceParser.readCsvResourceIntoMap("EmisReferralServiceTypeMap.csv", "SourceType", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = referralTypeMap.get(type);
        if (code == null) {
            throw new RuntimeException("Unknown referral type " + type);

        } else if (!Strings.isNullOrEmpty(code)) {
            return ReferralType.fromCode(code);

        } else {
            return null;
        }
    }

    public static OrganisationType findOrganisationType(String type) throws Exception {

        if (organisationTypeMap == null) {
            organisationTypeMap = ResourceParser.readCsvResourceIntoMap("EmisOrganisationTypeMap.csv", "SourceType", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = organisationTypeMap.get(type);
        if (code == null) {
            throw new RuntimeException("Unknown organisation type [" + type + "]");

        } else if (!Strings.isNullOrEmpty(code)) {
            return OrganisationType.fromCode(code);

        } else {
            return null;
        }
    }

    public static RegistrationType findRegistrationType(String type) throws Exception {

        if (patientTypeMap == null) {
            patientTypeMap = ResourceParser.readCsvResourceIntoMap("EmisPatientTypeMap.csv", "SourceType", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = patientTypeMap.get(type);
        if (code == null) {
            throw new RuntimeException("Unknown patient type " + type);

        } else if (!Strings.isNullOrEmpty(code)) {
            return RegistrationType.fromCode(code);

        } else {
            return null;
        }
    }

    public static MedicationAuthorisationType findMedicationAuthorisationType(String type) throws Exception {

        if (drugRecordPrescriptionTypeMap == null) {
            drugRecordPrescriptionTypeMap = ResourceParser.readCsvResourceIntoMap("EmisDrugRecordPrescriptionTypeMap.csv", "SourceType", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = drugRecordPrescriptionTypeMap.get(type);
        if (code == null) {
            throw new RuntimeException("Unknown drug record type " + type);

        } else if (!Strings.isNullOrEmpty(code)) {
            return MedicationAuthorisationType.fromCode(code);

        } else {
            return null;
        }
    }

    public static ProblemSignificance findProblemSignificance(String type) throws Exception {

        if (problemSeverityMap == null) {
            problemSeverityMap = ResourceParser.readCsvResourceIntoMap("EmisProblemSeverityMap.csv", "SourceSeverity", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = problemSeverityMap.get(type);
        if (code == null) {
            throw new RuntimeException("Unknown problem significance " + type);

        } else if (!Strings.isNullOrEmpty(code)) {
            return ProblemSignificance.fromCode(code);

        } else {
            return null;
        }
    }

    public static ProblemRelationshipType findProblemRelationship(String type) throws Exception {

        if (problemRelationshipMap == null) {
            problemRelationshipMap = ResourceParser.readCsvResourceIntoMap("EmisProblemRelationshipTypeMap.csv", "SourceType", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = problemRelationshipMap.get(type);
        if (code == null) {
            throw new RuntimeException("Unknown problem relationship " + type);

        } else if (!Strings.isNullOrEmpty(code)) {
            return ProblemRelationshipType.fromCode(code);

        } else {
            return null;
        }
    }

}
