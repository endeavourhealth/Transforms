package org.endeavourhealth.transform.ui.transforms.admin;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.ui.helpers.IdentifierHelper;
import org.endeavourhealth.transform.ui.helpers.UINameHelper;
import org.endeavourhealth.transform.ui.models.resources.admin.UIPractitioner;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Practitioner;

import java.util.List;

public class UIPractitionerTransform {
    public static UIPractitioner transform(Practitioner practitioner) {

        return new UIPractitioner()
                .setId(practitioner.getId())
                .setName(UINameHelper.transform(practitioner.getName()))
                .setActive(practitioner.getActive())
								.setGpCode(getGpCode(practitioner.getIdentifier()));
    }

    private static String getGpCode(List<Identifier> identifiers) {
    	return IdentifierHelper.getIdentifierBySystem(identifiers, FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER);
		}
}
