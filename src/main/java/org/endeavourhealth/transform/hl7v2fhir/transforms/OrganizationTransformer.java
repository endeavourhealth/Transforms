package org.endeavourhealth.transform.hl7v2fhir.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrganizationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OrganizationTransformer.class);

    /**
     *
     * @param organization
     * @return
     * @throws Exception
     */
    public static OrganizationBuilder transformPV1ToOrganization(OrganizationBuilder organization) throws Exception {
        organization.setId("Imperial College Healthcare NHS Trust");
        //organization.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ORGANIZATION));

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(organization);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        identifierBuilder.setValue("RYJ");

        AddressBuilder addressBuilder = new AddressBuilder(organization);
        addressBuilder.setUse(Address.AddressUse.WORK);

        organization.setName("Imperial College Healthcare NHS Trust");
        return organization;
    }
}
