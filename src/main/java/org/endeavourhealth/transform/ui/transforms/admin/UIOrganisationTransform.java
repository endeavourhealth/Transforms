package org.endeavourhealth.transform.ui.transforms.admin;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.ui.helpers.AddressHelper;
import org.endeavourhealth.transform.ui.helpers.IdentifierHelper;
import org.endeavourhealth.transform.ui.models.resources.admin.UIOrganisation;
import org.endeavourhealth.transform.ui.models.types.UIAddress;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Organization;

import java.util.List;

public class UIOrganisationTransform {
    public static UIOrganisation transform(Organization organization) {
        return new UIOrganisation()
                .setId(organization.getId())
                .setName(organization.getName())
                .setType(getOrganizationType(organization))
								.setAddress(getAddress(organization.getAddress()))
								.setOdsCode(getOdsCode(organization));
    }

    public static String getOrganizationType(Organization organization) {
			if (!organization.hasType())
				return null;

			if (organization.getType().hasText())
    		return organization.getType().getText();

    	if (!organization.getType().hasCoding() || organization.getType().getCoding().size() == 0)
    		return null;

    	return organization.getType().getCoding().get(0).getCode();
		}

		private static UIAddress getAddress(List<Address> address) {
    	if (address != null && address.size() > 0)
    		return AddressHelper.transform(address.get(0));

    	return null;
		}

		private static String getOdsCode(Organization organization) {
			return IdentifierHelper.getIdentifierBySystem(organization.getIdentifier(), FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
		}
}
