package org.endeavourhealth.transform.adastra.transforms.admin;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.guidMapper;

public class OrganisationTransformer {

    public static void transform(AdastraCaseDataExport caseReport, List<Resource> resources) throws Exception {


        AdastraCaseDataExport.Patient.GpRegistration gpRegistration = caseReport.getPatient().getGpRegistration();

        Organization organization = new Organization();
        organization.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ORGANIZATION));

        organization.addIdentifier().setSystem("http://fhir.nhs.net/Id/ods-organization-code").setValue(gpRegistration.getSurgeryNationalCode());
        organization.addAddress().setPostalCode(gpRegistration.getSurgeryPostcode());
        AdastraHelper.setUniqueId(organization, gpRegistration.getSurgeryNationalCode() + gpRegistration.getSurgeryPostcode());
        guidMapper.put(gpRegistration.getSurgeryNationalCode(), organization.getId());

        resources.add(organization);

    }
}
