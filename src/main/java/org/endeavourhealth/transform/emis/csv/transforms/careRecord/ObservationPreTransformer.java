package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.BpComponent;
import org.endeavourhealth.transform.emis.csv.helpers.CodeAndDate;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Observation;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class ObservationPreTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        AbstractCsvParser parser = parsers.get(Observation.class);
        while (parser.nextRecord()) {

            try {
                processLine((Observation)parser, csvHelper, fhirResourceFiler, version);
            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }


    private static void processLine(Observation parser, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            return;
        }

        //the test pack has non-deleted rows with missing CodeIds, so skip these rows
        if ((version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1))
            && parser.getCodeId().isEmpty()) {
            return;
        }

        ResourceType resourceType = ObservationTransformer.getTargetResourceType(parser, csvHelper);

        CsvCell parentGuid = parser.getParentObservationGuid();
        if (!parentGuid.isEmpty()) {

            CsvCell observationGuid = parser.getObservationGuid();
            CsvCell patientGuid = parser.getPatientGuid();

            //if the observation links to a parent observation, store this relationship in the
            //helper class, so when processing later, we can set the Has Member reference in the FHIR observation
            csvHelper.cacheObservationParentRelationship(parentGuid, patientGuid, observationGuid, resourceType);

            //if the observation is a BP reading, then cache in the helper
            CsvCell unit = parser.getNumericUnit();
            CsvCell value = parser.getValue();
            if (!unit.isEmpty()
                    && !value.isEmpty()) {

                //BP readings uniquely use mmHg for the units, so detect them using that
                String unitStr = unit.getString();
                if (unitStr.equalsIgnoreCase("mmHg")
                        || unitStr.equalsIgnoreCase("mm Hg")) {

                    CsvCell codeId = parser.getCodeId();

                    BpComponent bpComponent = new BpComponent(codeId, value, unit);
                    csvHelper.cacheBpComponent(parentGuid, patientGuid, bpComponent);
                }
            }
        }

        //if this record is linked to a problem, store this relationship in the helper
        CsvCell problemGuid = parser.getProblemGuid();
        if (!problemGuid.isEmpty()) {

            CsvCell observationGuid = parser.getObservationGuid();
            CsvCell patientGuid = parser.getPatientGuid();

            csvHelper.cacheProblemRelationship(problemGuid,
                    patientGuid,
                    observationGuid,
                    resourceType);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            CsvCell observationGuid = parser.getObservationGuid();
            CsvCell patientGuid = parser.getPatientGuid();

            csvHelper.cacheNewConsultationChildRelationship(consultationGuid,
                    patientGuid,
                    observationGuid,
                    resourceType);
        }

        CsvCell codeId = parser.getCodeId();
        ClinicalCodeType codeType = csvHelper.findClinicalCodeType(codeId);
        if (codeType == ClinicalCodeType.Ethnicity) {

            CsvCell effectiveDate = parser.getEffectiveDate();
            CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
            DateTimeType fhirDate = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            EmisCsvCodeMap codeMapping = csvHelper.findClinicalCode(codeId);
            EthnicCategory ethnicCategory = findEthnicityCode(codeMapping);
            if (ethnicCategory != null) {
                CsvCell patientGuid = parser.getPatientGuid();
                CodeAndDate codeAndDate = new CodeAndDate(codeMapping, fhirDate, codeId);
                csvHelper.cacheEthnicity(patientGuid, codeAndDate);
            }

        } else if (codeType == ClinicalCodeType.Marital_Status) {

            CsvCell effectiveDate = parser.getEffectiveDate();
            CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
            DateTimeType fhirDate = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            EmisCsvCodeMap codeMapping = csvHelper.findClinicalCode(codeId);
            MaritalStatus maritalStatus = findMaritalStatus(codeMapping);
            if (maritalStatus != null) {
                CsvCell patientGuid = parser.getPatientGuid();
                CodeAndDate codeAndDate = new CodeAndDate(codeMapping, fhirDate, codeId);
                csvHelper.cacheMaritalStatus(patientGuid, codeAndDate);
            }
        }

        //if we've previously found that our observation is a problem (via the problem pre-transformer)
        //then cache the read code of the observation
        CsvCell patientGuid = parser.getPatientGuid();
        CsvCell observationGuid = parser.getObservationGuid();
        if (csvHelper.isProblemObservationGuid(patientGuid, observationGuid)) {
            EmisCsvCodeMap codeMapping = csvHelper.findClinicalCode(codeId);
            String readCode = codeMapping.getReadCode();
            csvHelper.cacheProblemObservationGuid(patientGuid, observationGuid, readCode);
        }
    }

    private static MaritalStatus findMaritalStatus(EmisCsvCodeMap codeMapping) {
        String code = findRead2Code(codeMapping);
        if (code == null) {
            return null;
        }

        if (code.equals("1331.")) {
            //single

        } else if (code.equals("1332.")
            || code.equals("EMISNQHO15")
            || code.equals("EMISNQHO16")
            || code.equals("133S.")) {
            return MaritalStatus.MARRIED;

        } else if (code.equals("1334.")
            || code.equals("133T.")) {
            return MaritalStatus.DIVORCED;

        } else if (code.equals("1335.")
            || code.equals("133C.")
            || code.equals("133V.")) {
            return MaritalStatus.WIDOWED;

        } else if (code.equals("1333.")) {
            return MaritalStatus.LEGALLY_SEPARATED;

        } else if (code.equals("1336.")
            || code.equals("133e.")
                || code.equals("133G.")
                || code.equals("133H.")
                || code.equals("EMISNQCO47")) {
            return MaritalStatus.DOMESTIC_PARTNER;

        }

        return null;
    }

    private static EthnicCategory findEthnicityCode(EmisCsvCodeMap codeMapping) {
        String code = findRead2Code(codeMapping);
        if (code == null) {
            return null;
        }

        if (code.startsWith("9i0")) {
            return EthnicCategory.WHITE_BRITISH;
        } else if (code.startsWith("9i1")) {
            return EthnicCategory.WHITE_IRISH;
        } else if (code.startsWith("9i2")) {
            return EthnicCategory.OTHER_WHITE;
        } else if (code.startsWith("9i3")) {
            return EthnicCategory.MIXED_CARIBBEAN;
        } else if (code.startsWith("9i4")) {
            return EthnicCategory.MIXED_AFRICAN;
        } else if (code.startsWith("9i5")) {
            return EthnicCategory.MIXED_ASIAN;
        } else if (code.startsWith("9i6")) {
            return EthnicCategory.OTHER_MIXED;
        } else if (code.startsWith("9i7")) {
            return EthnicCategory.ASIAN_INDIAN;
        } else if (code.startsWith("9i8")) {
            return EthnicCategory.ASIAN_PAKISTANI;
        } else if (code.startsWith("9i9")) {
            return EthnicCategory.ASIAN_BANGLADESHI;
        } else if (code.startsWith("9iA")) {
            return EthnicCategory.OTHER_ASIAN;
        } else if (code.startsWith("9iB")) {
            return EthnicCategory.BLACK_CARIBBEAN;
        } else if (code.startsWith("9iC")) {
            return EthnicCategory.BLACK_AFRICAN;
        } else if (code.startsWith("9iD")) {
            return EthnicCategory.OTHER_BLACK;
        } else if (code.startsWith("9iE")) {
            return EthnicCategory.CHINESE;
        } else if (code.startsWith("9iF")) {
            return EthnicCategory.OTHER;
        } else if (code.startsWith("9iG")) {
            return EthnicCategory.NOT_STATED;
        } else {
            return null;
        }
    }

    private static String findRead2Code(EmisCsvCodeMap codeMapping) {
        return codeMapping.getReadCode();
    }
}


