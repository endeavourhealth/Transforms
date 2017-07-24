package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.UISimpleEvent;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIDiagnosticOrder;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIReferral;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.*;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class UIReferralTransform extends UIClinicalTransform<ReferralRequest, UIReferral> {

    @Override
    public List<UIReferral> transform(UUID serviceId, UUID systemId, List<ReferralRequest> resources, ReferencedResources referencedResources) {
        return resources
                .stream()
                .map(t -> transform(serviceId, systemId, t, referencedResources))
                .collect(Collectors.toList());
    }

    public static UIReferral transform(UUID serviceId, UUID systemId, ReferralRequest referralRequest, ReferencedResources referencedResources) {
        try {
            return new UIReferral()
                    .setId(referralRequest.getId())
                    .setEffectiveDate(getRecordedDateExtensionValue(referralRequest))
                    .setRecordingPractitioner(getPractitionerInternalIdentifer(serviceId, systemId, getFiledByExtensionValue(referralRequest)))
                    .setCode(getCode(referralRequest.getServiceRequested()));
        } catch (Exception e) {
            throw new TransformRuntimeException(e);
        }
    }

    protected static Reference getFiledByExtensionValue(DomainResource resource) {
        return ExtensionHelper.getExtensionValue(resource, FhirExtensionUri.RECORDED_BY, Reference.class);
    }

    protected static UIDate getRecordedDateExtensionValue(DomainResource resource) {
        DateTimeType recordedDate = ExtensionHelper.getExtensionValue(resource, FhirExtensionUri.RECORDED_DATE, DateTimeType.class);

        if (recordedDate == null)
            return null;

        return DateHelper.convert(recordedDate);
    }

    private static UICodeableConcept getCode(List<CodeableConcept> codeableConcepts) {
        Optional<CodeableConcept> code = codeableConcepts.stream()
            .findFirst();

        if (code.isPresent())
            return CodeHelper.convert(code.get());

        return null;
    }

    @Override
    public List<Reference> getReferences(List<ReferralRequest> resources) {
        return resources
                .stream()
                .filter(t -> t.hasEncounter())
                .map(t -> t.getEncounter())
            .collect(Collectors.toList());
    }
}
