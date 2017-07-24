package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.UISimpleEvent;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIDiagnosticOrder;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIDiagnosticReport;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIObservationRelation;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class UIDiagnosticOrderTransform extends UIClinicalTransform<DiagnosticOrder, UIDiagnosticOrder> {

    @Override
    public List<UIDiagnosticOrder> transform(UUID serviceId, UUID systemId, List<DiagnosticOrder> resources, ReferencedResources referencedResources) {
        return resources
                .stream()
                .map(t -> transform(serviceId, systemId, t, referencedResources))
                .collect(Collectors.toList());
    }

    public static UIDiagnosticOrder transform(UUID serviceId, UUID systemId, DiagnosticOrder diagnosticOrder, ReferencedResources referencedResources) {
        try {
            return new UIDiagnosticOrder()
                    .setId(diagnosticOrder.getId())
                    .setEffectiveDate(getRecordedDateExtensionValue(diagnosticOrder))
                    .setRecordingPractitioner(getPractitionerInternalIdentifer(serviceId, systemId, getFiledByExtensionValue(diagnosticOrder)))
                    .setCode(getCode(diagnosticOrder.getItem()))
                    .setEvents(getEvents(diagnosticOrder.getEvent()))
                    .setStatus(getLatestStatus(diagnosticOrder.getEvent()));
        } catch (Exception e) {
            throw new TransformRuntimeException(e);
        }
    }

    private static UISimpleEvent getLatestStatus(List<DiagnosticOrder.DiagnosticOrderEventComponent> events) {
        Optional<DiagnosticOrder.DiagnosticOrderEventComponent> latestStatus = events.stream()
            .sorted((c1, c2) -> Long.compare(c2.getDateTime().getTime(), c1.getDateTime().getTime()))
            .findFirst();

        if (latestStatus.isPresent())
            return new UISimpleEvent()
                .setStatus(latestStatus.get().getStatus().getDisplay())
                .setDate(DateHelper.convert(latestStatus.get().getDateTime()));

        return null;
    }

    private static List<UISimpleEvent> getEvents(List<DiagnosticOrder.DiagnosticOrderEventComponent> events) {
        return events.stream()
            .map(e -> new UISimpleEvent()
                .setStatus(e.getStatus().getDisplay())
                .setDate(DateHelper.convert(e.getDateTime()))
            )
            .collect(Collectors.toList());
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

    private static UICodeableConcept getCode(List<DiagnosticOrder.DiagnosticOrderItemComponent> diagnosticOrderItems) {
        Optional<DiagnosticOrder.DiagnosticOrderItemComponent> code = diagnosticOrderItems.stream()
            .filter(di -> di.hasCode())
            .findFirst();

        if (code.isPresent())
            return CodeHelper.convert(code.get().getCode());

        return null;
    }

    @Override
    public List<Reference> getReferences(List<DiagnosticOrder> resources) {
        return resources
                .stream()
                .filter(t -> t.hasEncounter())
                .map(t -> t.getEncounter())
            .collect(Collectors.toList());
    }
}
