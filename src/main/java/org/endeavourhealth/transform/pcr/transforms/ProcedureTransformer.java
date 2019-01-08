package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.pcr.FhirToPcrHelper;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class ProcedureTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);
    private final static String PATIENT = "Patient";

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                                     Resource resource,
                                     AbstractPcrCsvWriter csvWriter,
                                     PcrTransformParams params) throws Exception {

        Long id = pcrId;
        Long patientId;
        Long conceptId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecision = null;
        Long effectivePractitionerId = null;
        Long enteredByPractitionerId = null;
        Date endDate = null;
        //TODO usualPractitionerId is already stored on the patient. Why here? Remove?
        Long usualPractitionerId = null;
        Long careActivityId = FhirToPcrCsvTransformer.CARE_ACTIVITY_PLACE_HOLDER;
        Long careActivityHeadingConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;

        Long owningOrganisationId = null;
        Long statusConceptId = null;
        Boolean isConfidential = null;
        String originalCode = null;
        String originalTerm = null;
        Integer originalCodeScheme = null;
        Integer originalSystem = null;
        Long outcomeConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        ;
        Boolean isConsent = null;

        Procedure fhir = (Procedure) resource;

        patientId = params.getPcrPatientId();
        if (fhir.hasPerformedDateTimeType()) {
            DateTimeType dt = fhir.getPerformedDateTimeType();
            effectiveDate = dt.getValue();
            effectiveDatePrecision = convertDatePrecision(dt.getPrecision());
        }
        if (fhir.hasEncounter() && fhir.getEncounter() != null && !fhir.getEncounter().isEmpty()) {
            Reference reference = fhir.getEncounter();
            careActivityId = transformOnDemandAndMapId(reference, params);

        }

        if (fhir.hasPerformer()) {
            List<Procedure.ProcedurePerformerComponent> performers = fhir.getPerformer();
            for (Procedure.ProcedurePerformerComponent perf : performers) {
                if (perf.hasActor()) {
                    Reference reference = perf.getActor();
                    ResourceType resourceType = ReferenceHelper.getResourceType(reference);
                    if (resourceType.equals(ResourceType.Practitioner)) {
                        if (effectivePractitionerId == null) {
                            effectivePractitionerId = transformOnDemandAndMapId(perf.getActor(), params);
                        } else {

                        }
                        //break;
                    } else if (resourceType.equals(ResourceType.Organization)) {
                        owningOrganisationId = transformOnDemandAndMapId(perf.getActor(), params);
                    }
                    //TODO Means we only record one performer. Need to think about 1-many. Maybe procedure to practitioner map?
                    //This limit is already enforced in at least one incoming transformer.
                    //We can do this once we know how careActivity and careEpisodes map
//                    writeExtraPractitioner(careEpisodeId,
//                            effectivePractitionerId,
//                            patientId,
//                            enteredByPractitionerId,
//                            csvWriter);
                }
            }
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        if (codes != null) {

            //snomedConceptId = codes.getSnomedConceptId();
            outcomeConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            if (codes.getOriginalTerm() != null) {
                originalTerm = codes.getOriginalTerm();
            }
            originalCode = codes.getOriginalCode();
            // originalCodeScheme =  toIntExact(CodeScheme.SNOMED.getValue());
            if (codes.getSystem() != null) {
                originalCodeScheme = FhirToPcrHelper.getCodingScheme(codes.getSystem());
            }
            //originalSystem =
            conceptId = outcomeConceptId;
        } else return;

        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {

            Reference enteredByPractitionerReference = (Reference) enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params);
        }

        if (fhir.hasPerformedPeriod() && fhir.getPerformedPeriod() != null && fhir.getPerformedPeriod().hasEnd()) {
            endDate = fhir.getPerformedPeriod().getEnd();
        }


        //confidential
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {
            BooleanType b = (BooleanType) confidentialExtension.getValue();
            isConfidential = b.getValue();
        }
        org.endeavourhealth.transform.pcr.outputModels.Procedure model =
                (org.endeavourhealth.transform.pcr.outputModels.Procedure) csvWriter;
        model.writeUpsert(id,
                patientId,
                conceptId,
                effectiveDate,
                effectiveDatePrecision,
                effectivePractitionerId,
                enteredByPractitionerId,
                endDate,
//                usualPractitionerId,
                //TODO Sort care activity
                careActivityId,
                careActivityHeadingConceptId,
                owningOrganisationId,
                statusConceptId,
                isConfidential,
                originalCode,
                originalTerm,
                originalCodeScheme,
                originalSystem,
                outcomeConceptId,
                isConsent);


    }

    private void writeExtraPractitioner(Long careEpisodeId,
                                        Long effectivePractitionerId,
                                        Long patientId,
                                        Long enteredByPractitionerId,
                                        AbstractPcrCsvWriter csvWriter) throws  Exception{
        org.endeavourhealth.transform.pcr.outputModels.CareEpisodeAdditionalPractitioner model =
                (org.endeavourhealth.transform.pcr.outputModels.CareEpisodeAdditionalPractitioner) csvWriter;
        model.writeUpsert(patientId, careEpisodeId, effectivePractitionerId, enteredByPractitionerId);
    }
}

