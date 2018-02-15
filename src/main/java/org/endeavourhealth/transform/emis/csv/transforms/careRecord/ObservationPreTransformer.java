package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.*;
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
            if (EmisCodeHelper.isEthnicity(codeMapping)) {
                CsvCell patientGuid = parser.getPatientGuid();
                CodeAndDate codeAndDate = new CodeAndDate(codeMapping, fhirDate, codeId);
                csvHelper.cacheEthnicity(patientGuid, codeAndDate);
            }

        } else if (codeType == ClinicalCodeType.Marital_Status) {

            CsvCell effectiveDate = parser.getEffectiveDate();
            CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
            DateTimeType fhirDate = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            EmisCsvCodeMap codeMapping = csvHelper.findClinicalCode(codeId);
            if (EmisCodeHelper.isMaritalStatus(codeMapping)) {
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

}


