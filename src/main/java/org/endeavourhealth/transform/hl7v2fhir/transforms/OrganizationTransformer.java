package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.XON;
import ca.uhn.hl7v2.model.v23.segment.PD1;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.usermanager.caching.OrganisationCache;
import org.endeavourhealth.core.database.rdbms.datasharingmanager.models.OrganisationEntity;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.AcuteHL7Helper;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.ResourceType;

public class OrganizationTransformer {

    /**
     * @return
     * @throws Exception
     */
    public static OrganizationBuilder createHL7OrgBuilder(String odsCode) throws Exception {

        OrganisationEntity org = OrganisationCache.getOrganisationDetailsFromOdsCode(odsCode);
        String name = org.getName();

        OrganizationBuilder  organization = new OrganizationBuilder();
        organization.setId(name);
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(organization);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        identifierBuilder.setValue(odsCode);

        organization.setName(name);
        return organization;
    }

    /**
     *
     * @param pd1
     * @return
     * @throws Exception
     */
    public static OrganizationBuilder transformPD1ToOrganization(PD1 pd1) throws Exception {
        XON[] patientPrimaryFacility = pd1.getPatientPrimaryFacility();
        OrganizationBuilder organization =new OrganizationBuilder();
        if (patientPrimaryFacility[0].getIDNumber().getValue()!=null && !patientPrimaryFacility[0].getIDNumber().getValue().isEmpty() ) {
            organization.setId(patientPrimaryFacility[0].getIDNumber().getValue());
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organization);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setValue(patientPrimaryFacility[0].getIDNumber().getValue());
            organization.setName(patientPrimaryFacility[0].getOrganizationName().getValue());
        }
        return organization;
    }

    public static void saveOrganizationWithLocation(FhirResourceFiler fhirResourceFiler, OrganizationBuilder organizationBuilder, LocationBuilder locationBuilderOrg, OrganizationBuilder organizationBuilderDemographic) throws Exception {
        organizationBuilder.setMainLocation(AcuteHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilder);
        if(organizationBuilderDemographic.getResourceId()!=null) {
           fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        }
    }
}
