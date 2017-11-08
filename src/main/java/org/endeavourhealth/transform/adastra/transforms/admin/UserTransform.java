package org.endeavourhealth.transform.adastra.transforms.admin;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.guidMapper;

public class UserTransform {

    public static void transform(AdastraCaseDataExport caseReport, List<Resource> resources) throws Exception {
        for (AdastraCaseDataExport.Consultation con : caseReport.getConsultation()) {
            createUser(con.getConsultationBy(), con.getLocation(), resources);
        }

    }

    private static void createUser(AdastraCaseDataExport.Consultation.ConsultationBy conBy, String locationName, List<Resource> resources) {
        //TODO get the org Id from source message
        String orgId = "Adastra";

        Practitioner fhirPractitioner = new Practitioner();
        fhirPractitioner.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PRACTITIONER));

        AdastraHelper.setUniqueUserId(fhirPractitioner, orgId, conBy.getName());
        guidMapper.put(conBy.getName(), fhirPractitioner.getId());

        HumanName name = (new HumanName()).setUse(HumanName.NameUse.OFFICIAL).setText(conBy.getName());
        fhirPractitioner.setName(name);

        Practitioner.PractitionerPractitionerRoleComponent fhirRole = fhirPractitioner.addPractitionerRole();

        fhirRole.setManagingOrganization(AdastraHelper.createLocationReference(locationName));

        fhirRole.setRole(CodeableConceptHelper.createCodeableConcept(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES, conBy.getProviderType(), ""));

        fhirPractitioner.addPractitionerRole(fhirRole);

        resources.add(fhirPractitioner);

    }
}

