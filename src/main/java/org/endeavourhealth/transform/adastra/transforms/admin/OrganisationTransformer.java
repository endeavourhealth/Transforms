package org.endeavourhealth.transform.adastra.transforms.admin;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Organization;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.uniqueIdMapper;

public class OrganisationTransformer {

    public static void transform(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        AdastraCaseDataExport.Patient.GpRegistration gpRegistration = caseReport.getPatient().getGpRegistration();

        Organization fhirOrganisation = new Organization();
        fhirOrganisation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ORGANIZATION));

        fhirOrganisation.addIdentifier().setSystem("http://fhir.nhs.net/Id/ods-organization-code").setValue(gpRegistration.getSurgeryNationalCode());
        fhirOrganisation.addAddress().setPostalCode(gpRegistration.getSurgeryPostcode());

        fhirOrganisation.setId(gpRegistration.getSurgeryNationalCode() + ":" + gpRegistration.getSurgeryPostcode());
        uniqueIdMapper.put(gpRegistration.getSurgeryNationalCode(), fhirOrganisation.getId());

        fhirResourceFiler.saveAdminResource(null, fhirOrganisation);
    }
}
