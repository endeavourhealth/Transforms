package org.endeavourhealth.transform.adastra.transforms.admin;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Practitioner;

import java.util.ArrayList;
import java.util.List;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.uniqueIdMapper;

public class UserTransform {

    public static void transform(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        List<String> users = new ArrayList<>();
        String orgId = uniqueIdMapper.get(caseReport.getPatient().getGpRegistration().getSurgeryNationalCode());
        for (AdastraCaseDataExport.Consultation con : caseReport.getConsultation()) {
            if (!users.contains(con.getConsultationBy().getName())) {
                createUser(con.getConsultationBy(), con.getLocation(), orgId, fhirResourceFiler);
                users.add(con.getConsultationBy().getName());
            }
        }

    }

    private static void createUser(AdastraCaseDataExport.Consultation.ConsultationBy conBy, String locationName,
                                   String orgId, FhirResourceFiler fhirResourceFiler) throws Exception {

        Practitioner fhirPractitioner = new Practitioner();
        fhirPractitioner.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PRACTITIONER));

        fhirPractitioner.setId(orgId + ":" + conBy.getName());
        uniqueIdMapper.put(conBy.getName(), fhirPractitioner.getId());

        HumanName name = (new HumanName()).setUse(HumanName.NameUse.OFFICIAL).setText(conBy.getName());
        fhirPractitioner.setName(name);

        Practitioner.PractitionerPractitionerRoleComponent fhirRole = fhirPractitioner.addPractitionerRole();

        fhirRole.setManagingOrganization(AdastraHelper.createLocationReference(locationName));

        fhirRole.setRole(CodeableConceptHelper.createCodeableConcept(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES, conBy.getProviderType(), ""));

        fhirPractitioner.addPractitionerRole(fhirRole);

        fhirResourceFiler.saveAdminResource(null, fhirPractitioner);

    }
}

