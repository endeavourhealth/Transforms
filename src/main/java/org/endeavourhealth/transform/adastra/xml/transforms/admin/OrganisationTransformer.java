package org.endeavourhealth.transform.adastra.xml.transforms.admin;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.adastra.xml.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Organization;

import static org.endeavourhealth.transform.adastra.AdastraXmlHelper.uniqueIdMapper;

public class OrganisationTransformer {

    public static void transform(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        AdastraCaseDataExport.Patient.GpRegistration gpRegistration = caseReport.getPatient().getGpRegistration();

        Organization fhirOrganisation = new Organization();
        fhirOrganisation.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ORGANIZATION));

        fhirOrganisation.addIdentifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE).setValue(gpRegistration.getSurgeryNationalCode());
        fhirOrganisation.addAddress().setPostalCode(gpRegistration.getSurgeryPostcode());

        fhirOrganisation.setId(gpRegistration.getSurgeryNationalCode() + ":" + gpRegistration.getSurgeryPostcode());
        uniqueIdMapper.put(gpRegistration.getSurgeryNationalCode(), fhirOrganisation.getId());

        fhirResourceFiler.saveAdminResource(null, new OrganizationBuilder(fhirOrganisation));
    }
}
