package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.DateHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIAllergyIntolerance;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.hl7.fhir.instance.model.AllergyIntolerance;
import org.hl7.fhir.instance.model.Reference;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class UIAllergyIntoleranceTransform extends UIClinicalTransform<AllergyIntolerance, UIAllergyIntolerance> {

    @Override
    public List<UIAllergyIntolerance> transform(UUID serviceId, UUID systemId, List<AllergyIntolerance> resources, ReferencedResources referencedResources) {
        return resources
                .stream()
                .map(t -> transform(serviceId, systemId, t, referencedResources))
                .collect(Collectors.toList());
    }

    private static UIAllergyIntolerance transform(UUID serviceId, UUID systemId, AllergyIntolerance allergyIntolerance, ReferencedResources referencedResources) {

        return new UIAllergyIntolerance()
                .setId(allergyIntolerance.getId())
                .setCode(CodeHelper.convert(allergyIntolerance.getSubstance()))
                .setEffectivePractitioner(getPractitionerInternalIdentifer(serviceId, systemId, allergyIntolerance.getRecorder()))
                .setEffectiveDate(getOnsetDate(allergyIntolerance))
                .setRecordingPractitioner(getPractitionerInternalIdentifer(serviceId, systemId, getRecordedByExtensionValue(allergyIntolerance)))
                .setRecordedDate(getRecordedDate(allergyIntolerance))
                .setNotes(getNotes(allergyIntolerance.getNote()));
    }

    private static UIDate getRecordedDate(AllergyIntolerance allergyIntolerance) {

        if (!allergyIntolerance.hasRecordedDate())
            return null;

        return DateHelper.convert(allergyIntolerance.getRecordedDateElement());
    }

    private static UIDate getOnsetDate(AllergyIntolerance allergyIntolerance) {
        if (!allergyIntolerance.hasOnset())
            return null;

        return DateHelper.convert(allergyIntolerance.getOnsetElement());
    }

    @Override
    public List<Reference> getReferences(List<AllergyIntolerance> resources) {
			return resources
					.stream()
					.filter(t -> t.hasPatient())
					.map(t -> t.getPatient())
					.collect(Collectors.toList());
		}
}
