package org.endeavourhealth.transform.adastra.xml.transforms.admin;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.adastra.xml.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.AdastraXmlHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Practitioner;

import java.util.ArrayList;
import java.util.List;

import static org.endeavourhealth.transform.adastra.AdastraXmlHelper.uniqueIdMapper;

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
        fhirPractitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));

        fhirPractitioner.setId(orgId + ":" + conBy.getName());
        uniqueIdMapper.put(conBy.getName(), fhirPractitioner.getId());

        HumanName name = (new HumanName()).setUse(HumanName.NameUse.OFFICIAL).setText(conBy.getName());
        fhirPractitioner.setName(name);

        Practitioner.PractitionerPractitionerRoleComponent fhirRole = fhirPractitioner.addPractitionerRole();

        fhirRole.setManagingOrganization(AdastraXmlHelper.createLocationReference(locationName));

        fhirRole.setRole(CodeableConceptHelper.createCodeableConcept(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES, conBy.getProviderType(), ""));

        fhirPractitioner.addPractitionerRole(fhirRole);

        fhirResourceFiler.saveAdminResource(null, fhirPractitioner);

    }
}

