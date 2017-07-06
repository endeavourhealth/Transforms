package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UICondition;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIProblem;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.Reference;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class UIConditionTransform extends UIClinicalTransform<Condition, UICondition> {

    @Override
    public List<UICondition> transform(UUID serviceId, UUID systemId, List<Condition> resources, ReferencedResources referencedResources) {
        return resources
                .stream()
                .filter(t -> (!t.getMeta().hasProfile(FhirUri.PROFILE_URI_PROBLEM)))
                .map(t -> transform(serviceId, systemId, t, referencedResources, false))
                .collect(Collectors.toList());
    }

    static UICondition transform(UUID serviceId, UUID systemId, Condition condition, ReferencedResources referencedResources, boolean createProblem) {
        try {
            UICondition uiCondition = new UICondition();

            if (createProblem)
                uiCondition = new UIProblem();

            return uiCondition
                    .setId(condition.getId())
                    .setCode(CodeHelper.convert(condition.getCode()))
                    .setEffectivePractitioner(getPractitionerInternalIdentifer(serviceId, systemId, condition.getAsserter()))
                    .setEffectiveDate(getOnsetDateTime(condition))
                    .setRecordingPractitioner(getPractitionerInternalIdentifer(serviceId, systemId, getRecordedByExtensionValue(condition)))
                    .setRecordedDate(getDateRecorded(condition))
                    .setClinicalStatus(condition.getClinicalStatus())
                    .setVerificationStatus(getConditionVerificationStatus(condition))
                    .setAbatementDate(getAbatementDate(condition))
                    .setHasAbated(getAbatement(condition))
                    .setNotes(condition.getNotes());

//            private UIEncounter encounter;     *
//            private UICodeableConcept code;
//            private String clinicalStatus;
//            private String verificationStatus;
//            private Date onset;
//            private Date abatement;
//            private Boolean hasAbated;
//            private String notes;
//            private UIProblem partOfProblem;   *

        } catch (Exception e) {
            throw new TransformRuntimeException(e);
        }
    }

    private static UIDate getDateRecorded(Condition condition) {
        if (!condition.hasDateRecorded())
            return null;

        return DateHelper.convert(condition.getDateRecordedElement());
    }

    private static String getConditionVerificationStatus(Condition condition) {
        if (condition.getVerificationStatus() == null)
            return null;

        return condition.getVerificationStatus().toCode();
    }

    private static UIDate getOnsetDateTime(Condition condition) throws Exception {
        if (!condition.hasOnsetDateTimeType())
            return DateHelper.getUnknownDate();

        return DateHelper.convert(condition.getOnsetDateTimeType());
    }

    private static Boolean getAbatement(Condition condition) throws Exception {
        if (condition.hasAbatementBooleanType())
            return condition.getAbatementBooleanType().getValue();
        else if (condition.hasAbatementDateTimeType())
            return true;

        return false;
    }

    private static UIDate getAbatementDate(Condition condition) throws Exception {
        if (condition.hasAbatement())
            if (condition.hasAbatementDateTimeType())
                return DateHelper.convert(condition.getAbatementDateTimeType());

        return null;
    }

    @Override
    public List<Reference> getReferences(List<Condition> resources) {
        return StreamExtension.concat(
                resources
                        .stream()
                        .filter(t -> t.hasEncounter())
                        .map(t -> t.getEncounter()),
                resources
                        .stream()
                        .filter(t -> t.hasStage())
                        .filter(t -> t.getStage().hasAssessment())
                        .flatMap(t -> t.getStage().getAssessment().stream()))
                .collect(Collectors.toList());
    }
}
