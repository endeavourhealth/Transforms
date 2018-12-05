package org.endeavourhealth.transform.pcr.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.subscriberTransform.PcrIdDalI;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.pcr.FhirToPcrHelper;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.FreeText;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;
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
        Long patientId;
        Long encounterId = null;
        Long effectivePractitionerId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;
        Long snomedConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;

        id = pcrId.longValue();
        owningOrganisationId = params.getPcrOrganisationId().longValue();
        patientId = params.getPcrPatientId();
      //  String codeSystem = null;
        Long conceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        Long substanceConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
             Long enteredByPractitionerId = null;
        Long careActivityId = null;
        Long careActivityHeadingConceptId = -1L;
        Long statusConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        boolean isConfidential = false;
        boolean isConsent = false;
        String originalCode=null;
        String originalTerm=null;
        Integer originalCodeScheme=null;
        Integer originalSystem=null;

        //TODO: - manifestation not currently supported in existing FHIR transforms
        Long manifestationConceptId = null;
        Long manifestationFreeTextId = null;

        Extension encounterExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ASSOCIATED_ENCOUNTER);
        if (encounterExtension != null) {

            Reference encounterReference = (Reference)encounterExtension.getValue();
            encounterId = findPcrId(params, encounterReference);

            careActivityId = encounterId;            //TODO: check this is correct
        }

        //note that the "recorder" field is actually used to store the named clinician,
        //and the standard "recorded by" extension is used to store who physically entered it into the source software
        if (fhir.hasRecorder()) {

            Reference practitionerReference = fhir.getRecorder();
            effectivePractitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        //recorded/entered date
//        Extension enteredDateExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
//        if (enteredDateExtension != null) {
//
//            DateTimeType enteredDateTimeType = (DateTimeType)enteredDateExtension.getValue();
//            enteredDate = enteredDateTimeType.getValue();
//        }

        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {

            Reference enteredByPractitionerReference = (Reference)enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params);
        }

        if (fhir.hasOnset()) {

            DateTimeType dt = fhir.getOnsetElement();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getSubstance());
        if (codes != null) {

            snomedConceptId = codes.getSnomedConceptId();
            substanceConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //IMClient.getConceptId(CodeScheme.SNOMED.getValue(), snomedConceptId.toString());
           // substanceConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //codeSystem = codes.getSystem();
            if (codes.getOriginalTerm()!=null) {
                originalTerm= codes.getOriginalTerm();
            }
            originalCode = codes.getOriginalCode();
           // originalCodeScheme =  toIntExact(CodeScheme.SNOMED.getValue());
            if (codes.getSystem()!=null) {
                originalCodeScheme = FhirToPcrHelper.getCodingScheme(codes.getSystem());
            }
            //originalSystem =
            conceptId = substanceConceptId;  //TODO: why two?, check in FHIR as only substance set
        } else return;

        //confidential
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {

            BooleanType b = (BooleanType) confidentialExtension.getValue();
            isConfidential = b.getValue();
        }
        OutputContainer data = params.getOutputContainer();
        StringBuilder manifestText  = new StringBuilder();
        if (fhir.hasReaction()) {
           List<AllergyIntolerance.AllergyIntoleranceReactionComponent> reactions = fhir.getReaction();
           for (AllergyIntolerance.AllergyIntoleranceReactionComponent reaction : reactions) {
               List<CodeableConcept> manifestCodes = reaction.getManifestation();
               manifestText.append(reaction.getDescription());

           }
            if (StringUtils.isNotEmpty(manifestText)) {
                PcrIdDalI pcrIdDal = DalProvider.factoryPcrIdDal(params.getConfigName());
                manifestationFreeTextId  = pcrIdDal.findOrCreatePcrFreeTextId(resource.getId(),ResourceType.AllergyIntolerance.toString());



                FreeText textWriter =data.getFreeText();
                textWriter.writeUpsert(manifestationFreeTextId,patientId,enteredByPractitionerId,manifestText.toString());
                }
        }
        if (fhir.hasNote()) {
            if (fhir.getNote().hasText()) {
                FreeText textWriter =data.getFreeText();
                textWriter.writeUpsert(null,patientId,enteredByPractitionerId,fhir.getNote().getText());
            }
        }


        //allergy status
        if (fhir.hasStatus()) {
            AllergyIntolerance.AllergyIntoleranceStatus status = fhir.getStatus();
            statusConceptId = IMClient.getOrCreateConceptId("Allergy.Status" + status.toCode());
            //statusConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        }

        org.endeavourhealth.transform.pcr.outputModels.Allergy model
                = (org.endeavourhealth.transform.pcr.outputModels.Allergy)csvWriter;
        model.writeUpsert(
                id,
                patientId,
                conceptId,
                effectiveDate,
                effectiveDatePrecisionId,
                effectivePractitionerId,
                enteredByPractitionerId ,
                careActivityId,
                careActivityHeadingConceptId,
                owningOrganisationId,
                statusConceptId,
                isConfidential,
                originalCode,
                originalTerm,
                originalCodeScheme,
                originalSystem,
                substanceConceptId,
                manifestationConceptId,
                manifestationFreeTextId,
               isConsent);
    }
}


