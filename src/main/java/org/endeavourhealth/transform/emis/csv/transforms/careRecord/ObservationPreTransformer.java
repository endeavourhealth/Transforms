package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.exceptions.EmisCodeNotFoundException;
import org.endeavourhealth.transform.emis.csv.helpers.*;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Observation;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ObservationPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ObservationPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Observation parser = (Observation) parsers.get(Observation.class);
        while (parser != null && parser.nextRecord()) {
            try {
                processLine(parser, csvHelper, fhirResourceFiler);

            } catch (EmisCodeNotFoundException ex) {
                csvHelper.logMissingCode(ex, parser.getPatientGuid(), parser.getCodeId(), parser);

            } catch (Exception ex) {
                //because this is a pre-transformer to cache data, throw any exception so we don't continue
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }

        //we've cached the resource type of EVERY observation, but only need those with a parent-child link,
        //so call this to remove the unnecessary ones and free up some memory
        csvHelper.pruneUnnecessaryParentObservationResourceTypes();
    }

    private static void processLine(Observation parser, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            return;
        }

        //the test pack has non-deleted rows with missing CodeIds, so skip these rows
        if ((parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_1))
                && parser.getCodeId().isEmpty()) {
            return;
        }


        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        ResourceType resourceType = ObservationTransformer.getTargetResourceType(parser, csvHelper);

        csvHelper.cacheParentObservationResourceType(patientGuid, observationGuid, resourceType);

        CsvCell parentGuid = parser.getParentObservationGuid();
        if (!parentGuid.isEmpty()) {

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

            csvHelper.cacheProblemRelationship(problemGuid,
                    patientGuid,
                    observationGuid,
                    resourceType);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {

            csvHelper.cacheNewConsultationChildRelationship(consultationGuid,
                    patientGuid,
                    observationGuid,
                    resourceType);
        }

        CsvCell codeId = parser.getCodeId();
        ClinicalCodeType codeType = EmisCodeHelper.findClinicalCodeType(codeId);
        if (codeType == ClinicalCodeType.Ethnicity) {

            CsvCell effectiveDate = parser.getEffectiveDate();
            CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
            DateTimeType fhirDate = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            EmisCsvCodeMap codeMapping = EmisCodeHelper.findClinicalCode(codeId);
            CodeAndDate codeAndDate = new CodeAndDate(codeMapping, fhirDate, codeId);
            csvHelper.cacheEthnicity(patientGuid, codeAndDate);

        } else if (codeType == ClinicalCodeType.Marital_Status) {

            CsvCell effectiveDate = parser.getEffectiveDate();
            CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
            DateTimeType fhirDate = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            EmisCsvCodeMap codeMapping = EmisCodeHelper.findClinicalCode(codeId);
            CodeAndDate codeAndDate = new CodeAndDate(codeMapping, fhirDate, codeId);
            csvHelper.cacheMaritalStatus(patientGuid, codeAndDate);
        }

        //if we've previously found that our observation is a problem (via the problem pre-transformer)
        //then cache the read code of the observation
        if (csvHelper.isProblemObservationGuid(patientGuid, observationGuid)) {
            EmisCsvCodeMap codeMapping = EmisCodeHelper.findClinicalCode(codeId);
            String readCode = codeMapping.getAdjustedCode(); //use the adjusted code as it's padded to five chars
            csvHelper.cacheProblemObservationGuid(patientGuid, observationGuid, readCode);
        }
    }

}


