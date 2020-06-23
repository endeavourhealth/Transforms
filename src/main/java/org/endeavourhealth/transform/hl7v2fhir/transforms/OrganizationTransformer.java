package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Organization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrganizationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OrganizationTransformer.class);

    /**
     *
     * @param pv1
     * @param organization
     * @return
     * @throws Exception
     */
    public static Organization transformPV1ToOrganization(PV1 pv1, Organization organization) throws Exception {
        organization.setId("Imperial College Healthcare NHS Trust");
        organization.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ORGANIZATION));

        Identifier identifier = new Identifier();
        identifier.setUse(Identifier.IdentifierUse.fromCode("official"));
        identifier.setSystem("http://fhir.nhs.net/Id/ods-organization-code");
        identifier.setValue("RYJ");
        organization.addIdentifier(identifier);

        Address address = new Address();
        address.setUse(Address.AddressUse.WORK);
        /*address.setText("a");
        address.addLine(String.valueOf(assignedPatientLocation.getLocationDescription()));
        address.setCity(String.valueOf(assignedPatientLocation.getBuilding()));
        address.setDistrict("b");
        address.setPostalCode("c");*/
        organization.addAddress(address);

        organization.setName("Imperial College Healthcare NHS Trust");
        return organization;
    }
}
