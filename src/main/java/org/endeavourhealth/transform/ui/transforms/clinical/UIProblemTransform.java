package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIProblem;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIProblem;
import org.endeavourhealth.transform.ui.models.types.UICode;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.*;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class UIProblemTransform extends UIClinicalTransform<Condition, UIProblem> {

    @Override
    public List<UIProblem> transform(UUID serviceId, List<Condition> resources, ReferencedResources referencedResources) {
        return resources
                .stream()
                .filter(c -> isProblem(c))
                .map(t -> transform(serviceId, t))
                .collect(Collectors.toList());
    }

    private boolean isProblem(Condition c) {
        if (c.getMeta().hasProfile(FhirUri.PROFILE_URI_PROBLEM))
            return true;

        if (c.hasCategory()
            && c.getCategory().hasCoding()
            && c.getCategory().getCoding().size() > 0
            && c.getCategory().getCoding().get(0).hasCode()
            && c.getCategory().getCoding().get(0).getCode().toLowerCase().equals("complaint"))
            return true;

        return false;
    }

    private UIProblem transform(UUID serviceId, Condition condition) {
        UIProblem uiProblem = (UIProblem) UIConditionTransform.transform(serviceId, condition, true);

        return uiProblem
                .setExpectedDuration(getExpectedDuration(condition))
                .setLastReviewDate(getLastReviewDate(condition))
                .setLastReviewer(getPractitionerInternalIdentifer(serviceId, getLastReviewerReference(condition)))
                .setSignificance(getSignificance(condition))
                .setIsReview(getIsReview(condition));

//        UICondition fields plus
//
//        private Integer expectedDuration;
//        private Date lastReviewDate;
//        private UIPractitioner lastReviewer;
//        private UICode significance;
//        private UIProblem relatedProblem;     *
//        private String relationshipType;      *
    }

    private static UICode getSignificance(Condition condition) {
        CodeableConcept significance = ExtensionHelper.getExtensionValue(condition, FhirExtensionUri.PROBLEM_SIGNIFICANCE, CodeableConcept.class);
        if (significance != null
            && significance.hasCoding()
            && significance.getCoding().size() > 0)
            return CodeHelper.convert(significance.getCoding().get(0));

        return null;
    }

    private static Integer getExpectedDuration(Condition condition) {
        IntegerType expectedDuration = ExtensionHelper.getExtensionValue(condition, FhirExtensionUri.PROBLEM_EXPECTED_DURATION, IntegerType.class);

        if (expectedDuration == null) {
            return null;
        } else {
            return expectedDuration.getValue();
        }
    }

    private static UIDate getLastReviewDate(Condition condition) {
        Extension extension = ExtensionHelper.getExtension(condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
        DateType lastReviewDate = ExtensionHelper.getExtensionValue(extension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__DATE, DateType.class);

        if (lastReviewDate == null)
            return null;

        return DateHelper.convert(lastReviewDate);
    }

    private static Reference getLastReviewerReference(Condition condition) {
        Extension extension = ExtensionHelper.getExtension(condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
        return ExtensionHelper.getExtensionValue(extension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__PERFORMER, Reference.class);
    }

    private static Boolean getIsReview(Condition condition) {
        BooleanType isReview = ExtensionHelper.getExtensionValue(condition, FhirExtensionUri.IS_REVIEW, BooleanType.class);
        if (isReview == null)
            return false;
        else
            return isReview.getValue();
    }

    @Override
    public List<Reference> getReferences(List<Condition> resources) {
        return
        		new UIConditionTransform()
                        .getReferences(resources);
    }
}
