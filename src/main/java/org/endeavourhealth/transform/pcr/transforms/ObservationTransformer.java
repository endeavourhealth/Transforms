package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.ObservationValue;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.TransformException;
import java.math.BigDecimal;
import java.util.Date;

public class ObservationTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                                     Resource resource,
                                     AbstractPcrCsvWriter csvWriter,
                                     PcrTransformParams params) throws Exception {

        Observation fhir = (Observation) resource;

        long id;
        Long owningOrganisationId;
        Long patientId;

        Long encounterId = null;
        Long effectivePractitionerId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;
        Long snomedConceptId = null;

        BigDecimal resultValue = null;
        String resultValueUnits = null;
        Date resultDate = null;
        String resultText = null;
        Long resultSnomedConceptId = null;
        String originalCode = null;
        String originalTerm = null;
        Integer originalCodeScheme = null;
        //TODO find original code scheme
        Integer originalSystem = null;

        Long conceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Long enteredByPractitionerId = null;
        Long careActivityId = null;
        Long careActivityHeadingConceptId = null;
        Long statusConceptId = null;
        boolean confidential = false;
        Long episodicityConceptId = null;
        Long freeTextId = null;
        Long dataEntryPromptId = null;
        Long significanceConceptId = null;
        boolean isConsent = false;
        Long resultConceptId = null;
        Long operatorConceptId = null;
        Long referenceRangeId = null;

        id = pcrId.longValue();
        owningOrganisationId = params.getPcrOrganisationId().longValue();
        patientId = params.getPcrPatientId();

        if (fhir.hasEncounter()) {

            Reference encounterReference = fhir.getEncounter();
            encounterId = findPcrId(params, encounterReference);

            careActivityId = encounterId;            //TODO: check this is correct
        }

        if (fhir.hasPerformer()) {

            for (Reference reference : fhir.getPerformer()) {
                ResourceType resourceType = ReferenceHelper.getResourceType(reference);
                if (resourceType == ResourceType.Practitioner) {
                    effectivePractitionerId = transformOnDemandAndMapId(reference, params);
                }
            }
        }

        if (fhir.hasEffectiveDateTimeType()) {

            DateTimeType dt = fhir.getEffectiveDateTimeType();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        if ((fhir.hasCode()) && (fhir.getCode()!=null)) {
            ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
            if (codes != null) {

                snomedConceptId = codes.getSnomedConceptId();
                conceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                //TODO IMClient.getConceptId(CodeScheme.SNOMED.getValue(), snomedConceptId.toString());

                originalCode = codes.getOriginalCode();
                originalTerm = codes.getOriginalTerm();
                String sys = codes.getSystem();
                originalSystem = (int) FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                //TODO IMClient.getConceptId(sys).intValue();
            }
        } else {
            LOG.warn("Fhir Observation record has no fhir code" + id);
            return;
        }

        if (fhir.hasValue()) {

            Type value = fhir.getValue();
            if (value instanceof Quantity) {
                Quantity quantity = (Quantity) value;
                resultValue = quantity.getValue();
                resultValueUnits = quantity.getUnit();

                Quantity.QuantityComparator comparator = quantity.getComparator();
                operatorConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                //TODO IMClient.getOrCreateConceptId("Quantity.QuantityComparator." + comparator.toCode());

            } else if (value instanceof DateTimeType) {
                DateTimeType dateTimeType = (DateTimeType) value;
                resultDate = dateTimeType.getValue();

            } else if (value instanceof StringType) {
                StringType stringType = (StringType) value;
                resultText = stringType.getValue();

            } else if (value instanceof CodeableConcept) {
                CodeableConcept resultCodeableConcept = (CodeableConcept) value;
                resultSnomedConceptId = CodeableConceptHelper.findSnomedConceptId(resultCodeableConcept);
                resultConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                //TODO IMClient.getConceptId(CodeScheme.SNOMED.getValue(), resultSnomedConceptId.toString());

            } else {
                throw new TransformException("Unsupported value type " + value.getClass() + " for " + fhir.getResourceType() + " " + fhir.getId());
            }
        }

        //recorded/entered date
        Extension enteredDateExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
        if (enteredDateExtension != null) {

            DateTimeType enteredDateTimeType = (DateTimeType) enteredDateExtension.getValue();
            enteredDate = enteredDateTimeType.getValue();
        }

        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {

            Reference enteredByPractitionerReference = (Reference) enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params);
        }

        //TODO: where get heading from?
        careActivityHeadingConceptId = -1L;


        //observation status
        if (fhir.hasStatus()) {
            statusConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //TODO IMClient.getOrCreateConceptId("Observation." +fhir.getStatus().toCode());
        }

        //confidential?
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {

            BooleanType b = (BooleanType) confidentialExtension.getValue();
            confidential = b.getValue();
        }

        Extension episodicityExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_EPISODICITY);
        if (episodicityExtension != null) {

            StringType episodicityType = (StringType) episodicityExtension.getValue();
            episodicityConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //TODO  = IMClient.getConceptId("FhirExtensionUri.PROBLEM_EPISODICITY");
            //TODO do we know how extension uri is mapped?
        }

        Extension significanceExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_SIGNIFICANCE);
        if (significanceExtension != null) {

            CodeableConcept codeableConcept = (CodeableConcept) significanceExtension.getValue();
            ProblemSignificance fhirSignificance = ProblemSignificance.fromCodeableConcept(codeableConcept);

            significanceConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //TODO IMClient.getConceptId(CodeScheme.SNOMED.getValue(),fhirSignificance.getCode());
            //TODO not sure how we model these codeschemes yet
        }

        //referenceRangeId = ??  //TODO: map to IM concept (not set in FHIR)

        org.endeavourhealth.transform.pcr.outputModels.Observation observationModel
                = (org.endeavourhealth.transform.pcr.outputModels.Observation) csvWriter;
        observationModel.writeUpsert(
                id,
                patientId,
                conceptId,
                effectiveDate,
                effectiveDatePrecisionId,
                effectivePractitionerId,
                enteredByPractitionerId,
                careActivityId,
                careActivityHeadingConceptId,
                owningOrganisationId,
                confidential,
                originalCode,
                originalTerm,
                originalCodeScheme,
                originalSystem,
                episodicityConceptId,
                freeTextId,
                dataEntryPromptId,
                significanceConceptId,
                isConsent);


        //if the observation has a value then file that data
        if (fhir.hasValue()) {
            LOG.debug("Observation id " + id + " has value ");
            String filename = observationModel.getFileName();
            String idFileName = filename.replace("observation","observation_value");
            ObservationValue observationValueModel = new ObservationValue(idFileName,FhirToPcrCsvTransformer.CSV_FORMAT,
                    FhirToPcrCsvTransformer.DATE_FORMAT ,FhirToPcrCsvTransformer.TIME_FORMAT);
            observationValueModel.writeUpsert(
                    patientId,
                    id,
                    operatorConceptId,
                    enteredByPractitionerId,
                    resultValue,
                    resultValueUnits,
                    resultDate,
                    resultText,
                    resultConceptId,
                    referenceRangeId
            );
        } else {
            LOG.debug("Observation id " + id + " has no value assigned.");
        }
        //TODO is this where we get allergy data from?

        //TODO - handle free text and linking
    }
}



