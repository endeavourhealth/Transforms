package org.endeavourhealth.transform.ui.transforms.clinical;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIInternalIdentifier;
import org.hl7.fhir.instance.model.*;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class UIClinicalTransform<T extends Resource, U extends UIResource> {
    public abstract List<U> transform(UUID serviceId, UUID systemId, List<T> resources, ReferencedResources referencedResources);

    public abstract List<Reference> getReferences(List<T> resources);

    protected static UIInternalIdentifier getPractitionerInternalIdentifer(UUID serviceId, UUID systemId, Reference reference) {
    	if (reference == null)
    		return null;

    	String referenceId = ReferenceHelper.getReferenceId(reference, ResourceType.Practitioner)
					.replace("{", "")
					.replace("}","");			// Work-around for {} transform issue

    	if (referenceId == null)
    		return null;

        return new UIInternalIdentifier()
						.setServiceId(serviceId)
						.setSystemId(systemId)
						.setResourceId(UUID.fromString(referenceId));
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
