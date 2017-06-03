package org.endeavourhealth.transform.ui.transforms.clinical;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.resources.admin.UIPractitioner;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.*;

import java.util.List;
import java.util.stream.Collectors;

public abstract class UIClinicalTransform<T extends Resource, U extends UIResource> {
    public abstract List<U> transform(List<T> resources, ReferencedResources referencedResources);

    public abstract List<Reference> getReferences(List<T> resources);

    protected static UIPractitioner getRecordedByExtensionValue(DomainResource resource, ReferencedResources referencedResources) {
        Reference reference = getRecordedByExtensionValue(resource);
        return referencedResources.getUIPractitioner(reference);
    }

    protected static Reference getRecordedByExtensionValue(DomainResource resource) {
        return ExtensionHelper.getExtensionValue(resource, FhirExtensionUri.RECORDED_BY, Reference.class);
    }

    protected static UIDate getRecordedDateExtensionValue(DomainResource resource) {
        DateTimeType recordedDate = ExtensionHelper.getExtensionValue(resource, FhirExtensionUri.RECORDED_DATE, DateTimeType.class);

        if (recordedDate == null)
            return null;

        return DateHelper.convert(recordedDate);
    }

    protected static String getNotes(Annotation annotation) {
        if (annotation == null)
            return null;

        return annotation.getText();
    }

    protected static String getNotes(List<Annotation> annotations) {
        if (annotations == null || annotations.isEmpty())
            return null;

        return StringUtils.join(annotations
                .stream()
                .map(t -> t.getText())
                .collect(Collectors.toList()), " | ");
    }
}
