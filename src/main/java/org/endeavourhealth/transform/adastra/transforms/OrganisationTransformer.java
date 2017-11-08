package org.endeavourhealth.transform.adastra.transforms;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.adastra.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class OrganisationTransformer {

    public static void transform(AdastraCaseDataExport caseReport, List<Resource> resources) throws Exception {


        createOrganisationFromGPRegistration(caseReport.getPatient().getGpRegistration());
    }

    private static void createOrganisationFromGPRegistration(AdastraCaseDataExport.Patient.GpRegistration gpRegistration) throws Exception {
        Organization organization = new Organization();
        organization.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ORGANIZATION));

        organization.addIdentifier().setSystem("http://fhir.nhs.net/Id/ods-organization-code").setValue(gpRegistration.getSurgeryNationalCode());
        organization.addAddress().setPostalCode(gpRegistration.getSurgeryPostcode());
        AdastraHelper.setUniqueId(organization, gpRegistration.getSurgeryNationalCode(), gpRegistration.getSurgeryPostcode());
    }
}
