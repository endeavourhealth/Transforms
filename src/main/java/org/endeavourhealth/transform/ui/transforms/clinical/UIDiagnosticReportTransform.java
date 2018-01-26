package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.helpers.QuantityHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIDiagnosticReport;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIObservation;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIObservationRelation;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIQuantity;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class UIDiagnosticReportTransform extends UIClinicalTransform<DiagnosticReport, UIDiagnosticReport> {

    @Override
    public List<UIDiagnosticReport> transform(UUID serviceId, List<DiagnosticReport> resources, ReferencedResources referencedResources) {
        return resources
                .stream()
                .map(t -> transform(serviceId, t, referencedResources))
                .collect(Collectors.toList());
    }

    public static UIDiagnosticReport transform(UUID serviceId, DiagnosticReport diagnosticReport, ReferencedResources referencedResources) {
        try {
            return new UIDiagnosticReport()
                    .setId(diagnosticReport.getId())
                    .setEffectiveDate(getEffectiveDateTime(diagnosticReport))
                    .setCode(CodeHelper.convert(diagnosticReport.getCode()))
                    .setStatus(diagnosticReport.getStatus().getDisplay())
                    .setRecordingPractitioner(getPractitionerInternalIdentifer(serviceId, getFiledByExtensionValue(diagnosticReport)))
                    .setConclusion(diagnosticReport.getConclusion())
                    .setRelated(getRelated(diagnosticReport, referencedResources));
        } catch (Exception e) {
            throw new TransformRuntimeException(e);
        }
    }

    protected static Reference getFiledByExtensionValue(DomainResource resource) {
        return ExtensionHelper.getExtensionValue(resource, FhirExtensionUri.RECORDED_BY, Reference.class);
    }

    private static UIDate getEffectiveDateTime(DiagnosticReport diagnosticReport) throws Exception {
        if (!diagnosticReport.hasEffectiveDateTimeType())
            return null;

        return DateHelper.convert(diagnosticReport.getEffectiveDateTimeType());
    }

    private static List<UIObservationRelation> getRelated(DiagnosticReport diagnosticReport, ReferencedResources referencedResources) {
        if (!diagnosticReport.hasResult())
            return null;

        List<UIObservationRelation> related = new ArrayList<>();
        for(Reference relatedComponent : diagnosticReport.getResult()) {
            related.add(new UIObservationRelation()
                .setType(Observation.ObservationRelationshipType.HASMEMBER.toCode())
                .setTarget(referencedResources.getUIObservation(relatedComponent))
            );
        }

        return related;
    }

    @Override
    public List<Reference> getReferences(List<DiagnosticReport> resources) {
        return StreamExtension.concat(
            resources
                .stream()
                .filter(t -> t.hasResult())
                .flatMap(t -> t.getResult().stream()),
            resources
                .stream()
                .filter(t -> t.hasEncounter())
                .map(t -> t.getEncounter())
        )
            .collect(Collectors.toList());
    }
}
