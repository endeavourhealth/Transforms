package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIProcedure;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class UIProcedureTransform extends UIClinicalTransform<Procedure, UIProcedure> {

    @Override
    public List<UIProcedure> transform(UUID serviceId, UUID systemId, List<Procedure> resources, ReferencedResources referencedResources) {
        return resources
                .stream()
                .map(t -> transform(serviceId, systemId, t, referencedResources))
                .collect(Collectors.toList());
    }

    private static UIProcedure transform(UUID serviceId, UUID systemId, Procedure procedure, ReferencedResources referencedResources) {
        try {
            return new UIProcedure()
                    .setId(procedure.getId())
                    .setCode(CodeHelper.convert(procedure.getCode()))
                    .setEffectiveDate(getPerformedDate(procedure))
                    .setEffectivePractitioner(getPractitionerInternalIdentifer(serviceId, systemId, getPerformer(procedure)))
                    .setRecordedDate(getRecordedDateExtensionValue(procedure))
                    .setRecordingPractitioner(getPractitionerInternalIdentifer(serviceId, systemId, getRecordedByExtensionValue(procedure)))
                    .setNotes(getNotes(procedure.getNotes()));

        } catch (Exception e) {
            throw new TransformRuntimeException(e);
        }
    }

    private static UIDate getPerformedDate(Procedure procedure) {
        try {
            if (!procedure.hasPerformedDateTimeType())
                return null;

            return DateHelper.convert(procedure.getPerformedDateTimeType());
        } catch (Exception e) {
            throw new TransformRuntimeException(e);
        }
    }

    private static Reference getPerformer(Procedure procedure) {
        return procedure
                .getPerformer()
                .stream()
                .filter(t -> ReferenceHelper.isResourceType(t.getActor(), ResourceType.Practitioner))
                .map(t -> t.getActor())
                .collect(StreamExtension.firstOrNullCollector());
    }

    @Override
    public List<Reference> getReferences(List<Procedure> resources) {
        return StreamExtension.concat(
                resources
                        .stream()
                        .map(t -> t.getSubject()),
                resources
                        .stream()
                        .filter(t -> t.hasEncounter())
                        .map(t -> t.getEncounter()),
                resources
                        .stream()
                        .filter(t -> t.hasLocation())
                        .map(t -> t.getLocation()))
                .collect(Collectors.toList());
    }
}
