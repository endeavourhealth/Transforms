package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.UISimpleEvent;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIDiagnosticOrder;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UISpecimen;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class UISpecimenTransform extends UIClinicalTransform<Specimen, UISpecimen> {

    @Override
    public List<UISpecimen> transform(UUID serviceId, List<Specimen> resources, ReferencedResources referencedResources) {
        return resources
                .stream()
                .map(t -> transform(serviceId, t))
                .collect(Collectors.toList());
    }

    public static UISpecimen transform(UUID serviceId, Specimen specimen) {
        try {
            return new UISpecimen()
                    .setId(specimen.getId())
                    .setEffectiveDate(getRecordedDateExtensionValue(specimen))
                    .setRecordingPractitioner(getPractitionerInternalIdentifer(serviceId, getFiledByExtensionValue(specimen)))
                    .setCode(CodeHelper.convert(specimen.getType()))
                    .setStatus(getStatus(specimen));
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

    private static UISimpleEvent getStatus(Specimen specimen) throws Exception {
        if (!specimen.hasCollection())
            return null;

        String comment = "";
        if (specimen.getCollection().getComment().size() > 0)
            comment = specimen.getCollection().getComment().get(0).getValueNotNull();

        return new UISimpleEvent()
            .setStatus(comment)
            .setDate(DateHelper.convert(specimen.getCollection().getCollectedDateTimeType()));
    }

    @Override
    public List<Reference> getReferences(List<Specimen> resources) {
        return new ArrayList<>();
    }
}
