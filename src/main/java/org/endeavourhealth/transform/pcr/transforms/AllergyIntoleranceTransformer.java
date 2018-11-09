package org.endeavourhealth.transform.pcr.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.subscriberTransform.PcrIdDalI;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.im.models.CodeScheme;
import org.endeavourhealth.transform.pcr.FhirToPcrHelper;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class AllergyIntoleranceTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AllergyIntoleranceTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
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

        id = pcrId.longValue();
        owningOrganisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().intValue();
        String codeSystem = null;
        Long conceptId = null;
        Long substanceConceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Integer enteredByPractitionerId = null;
        Long careActivityId = null;
        Long careActivityHeadingConceptId = null;
        Long statusConceptId = null;
        boolean confidential = false;
        boolean isConsent = false;

        //TODO: - manifestation not currently supported in existing FHIR transforms
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
        if (codes != null) {

            snomedConceptId = codes.getSnomedConceptId();
            substanceConceptId = IMClient.getConceptId(CodeScheme.SNOMED.getValue(), snomedConceptId.toString());
            codeSystem = codes.getSystem();
            conceptId = substanceConceptId;  //TODO: why two?, check in FHIR as only substance set
        } else return;

        //confidential
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {

            BooleanType b = (BooleanType) confidentialExtension.getValue();
            confidential = b.getValue();
        }
        StringBuilder manifestText  = new StringBuilder();
        if (fhir.hasReaction()) {
           List<AllergyIntolerance.AllergyIntoleranceReactionComponent> reactions = fhir.getReaction();
           for (AllergyIntolerance.AllergyIntoleranceReactionComponent reaction : reactions) {
               List<CodeableConcept> manifestCodes = reaction.getManifestation();
               manifestText.append(reaction.getDescription());

           }
            if (StringUtils.isNotEmpty(manifestText)) {
                PcrIdDalI pcrIdDal = DalProvider.factoryPcrIdDal(params.getConfigName());
                manifestationFreeTextId  = pcrIdDal.createPcrFreeTextId(resource.getId(),ResourceType.AllergyIntolerance.toString());
                FhirToPcrHelper.freeTextWriter(manifestationFreeTextId, patientId, manifestText.toString(),csvWriter);
            }
        }


        //allergy status
        if (fhir.hasStatus()) {
            AllergyIntolerance.AllergyIntoleranceStatus status = fhir.getStatus();
            statusConceptId = IMClient.getOrCreateConceptId("AllergyIntoleranceStatus." + status.toCode());
        }

        org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise.AllergyIntolerance model
                = (org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise.AllergyIntolerance)csvWriter;
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
                codeSystem,
                confidential,
                substanceConceptId,
                manifestationConceptId,
                manifestationFreeTextId,
                isConsent);
    }
}


