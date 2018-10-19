package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class AllergyIntoleranceTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AllergyIntoleranceTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        AllergyIntolerance fhir = (AllergyIntolerance)resource;

        long id;
        long owningOrganisationId;
        Integer patientId;
        Long encounterId = null;
        Integer effectivePractitionerId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;
        Long snomedConceptId = null;

        id = enterpriseId.longValue();
        owningOrganisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().intValue();
        //personId = params.getEnterprisePersonId().longValue();

        Integer conceptId = null;
        Integer substanceConceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Integer enteredByPractitionerId = null;
        Long careActivityId = null;
        Integer careActivityHeadingConceptId = null;
        Integer statusConceptId = null;
        boolean confidential = false;
        boolean isConsent = false;

        //TODO: - not currently supported in existing FHIR transforms
        Integer manifestationConceptId = null;
        Long manifestationFreeTextId = null;

        Extension encounterExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ASSOCIATED_ENCOUNTER);
        if (encounterExtension != null) {

            Reference encounterReference = (Reference)encounterExtension.getValue();
            encounterId = findEnterpriseId(params, encounterReference);

            careActivityId = encounterId;            //TODO: check this is correct
        }

        //note that the "recorder" field is actually used to store the named clinician,
        //and the standard "recorded by" extension is used to store who physically entered it into the source software
        if (fhir.hasRecorder()) {
            Reference practitionerReference = fhir.getRecorder();
            effectivePractitionerId = transformOnDemandAndMapId(practitionerReference, params).intValue();
        }

        //recorded/entered date
        Extension enteredDateExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
        if (enteredDateExtension != null) {

            DateTimeType enteredDateTimeType = (DateTimeType)enteredDateExtension.getValue();
            enteredDate = enteredDateTimeType.getValue();
        }

        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {

            Reference enteredByPractitionerReference = (Reference)enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params).intValue();
        }

        if (fhir.hasOnset()) {
            DateTimeType dt = fhir.getOnsetElement();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());

        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getSubstance());
        if (codes == null) {

            snomedConceptId = codes.getSnomedConceptId();

            //conceptId = ??  //TODO: map to IM conceptId

            //substanceConceptId = ??  why two?, check in FHIR
        }

        //confidential
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {

            BooleanType b = (BooleanType) confidentialExtension.getValue();
            confidential = b.getValue();
        }

        org.endeavourhealth.transform.pcr.outputModels.AllergyIntolerance model
                = (org.endeavourhealth.transform.pcr.outputModels.AllergyIntolerance)csvWriter;
        model.writeUpsert(
                id,
                patientId,
                conceptId,
                effectiveDate,
                effectiveDatePrecisionId,
                effectivePractitionerId,
                insertDate,
                enteredDate,
                enteredByPractitionerId,
                careActivityId,
                careActivityHeadingConceptId,
                owningOrganisationId,
                statusConceptId,
                confidential,
                substanceConceptId,
                manifestationConceptId,
                manifestationFreeTextId,
                isConsent);
    }
}


