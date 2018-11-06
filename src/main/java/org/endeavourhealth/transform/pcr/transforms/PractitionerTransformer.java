package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
public class PractitionerTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PractitionerTransformer.class);

    public boolean shouldAlwaysTransform() {
        return false;
    }

    protected void transformResource(Long pcrId,
                                     Resource resource,
                                     AbstractPcrCsvWriter csvWriter,
                                     PcrTransformParams params) throws Exception {

        Practitioner fhir = (Practitioner) resource;

        String name = null;
        String title = null;
        String firstName = null;
        String middleName = null;
        String lastName = null;
        String roleTermId = null;
        String specialityTermId = null;
        Long typeConceptId = null;
        int genderTermId;
        Date dateOfBirth;
        boolean isActive;
        long id;
        long organisationId = params.getEnterpriseOrganisationId();


        id = pcrId.longValue();

        if (fhir.hasName()) {
            HumanName fhirName = fhir.getName();
            // Some will be fully formed HumanName with first last etc. Some just name as text.
            // Assume if family is populated we at least have a first name as well
            if (fhirName.hasFamily()) {
                List<StringType> nameList = fhirName.getFamily();
                lastName = nameList.get(nameList.size() - 1).toString();
                nameList = fhirName.getGiven();
                firstName = nameList.get(0).toString();
                if (nameList.size() > 1) {
                    StringBuilder s = new StringBuilder();
                    for (int j = 1; j < nameList.size(); j++) {
                        s.append(nameList.get(j).toString() + " ");
                    }
                    middleName = s.toString();
                }
            } else {
                name = fhirName.getText();
                String[] tokens = name.split(" ");
                ArrayList<String> list = new ArrayList(Arrays.asList(tokens));
                list.removeAll(Arrays.asList("", null));
                tokens = new String[list.size()];
                tokens = list.toArray(tokens);
                // Take last part as surname.  Assume original TPP data has proper HumanNames
                String surname = tokens[tokens.length - 1];
                firstName = tokens[1];
                for (int count = 1; count < tokens.length - 2; count++) {
                    fhirName.addGiven(tokens[count]);
                }
            }
        }

        Long practitionerEnterpriseOrgId = null;
        //LOG.trace("Transforming practitioner " + fhir.getId() + " with " + fhir.getPractitionerRole().size() + " roles and enterpriseOrganisationUuid " + enterpriseOrganisationUuid);
        for (Practitioner.PractitionerPractitionerRoleComponent role : fhir.getPractitionerRole()) {

            CodeableConcept cc = role.getRole();
            for (Coding coding : cc.getCoding()) {
                if (coding.getSystem().equals(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES)) {
                    roleTermId = coding.getCode();
                }
            }

//            //if not role value set option found, just get the free text of the role
//            if (Strings.isNullOrEmpty(roleDesc)) {
//                for (Coding coding : cc.getCoding()) {
//                    if (coding.hasDisplay()) {
//                        roleDesc = coding.getDisplay();
//                    }
//                }
//            }

            if (role.hasManagingOrganization()) {
                Reference organisationReference = role.getManagingOrganization();
                practitionerEnterpriseOrgId = transformOnDemandAndMapId(organisationReference, params);
            }
            //LOG.trace("Got role with org ID " + practitionerEnterpriseOrgId + " from " + organisationReference);
        }

        //if we failed to find a proper organisation ID for the practitioner, assign it to the
        //organisation we're doing the transform for
        if (practitionerEnterpriseOrgId == null) {
            //LOG.trace("No role, so setting to the enterpriseOrganisationUuid " + enterpriseOrganisationUuid);
            practitionerEnterpriseOrgId = params.getEnterpriseOrganisationId();
        }

        organisationId = practitionerEnterpriseOrgId.longValue();

        isActive = fhir.getActive();
        dateOfBirth = fhir.getBirthDate();
        if (fhir.hasGender()) {
            genderTermId = fhir.getGender().ordinal();
        } else {
            genderTermId = Enumerations.AdministrativeGender.UNKNOWN.ordinal();
        }

        org.endeavourhealth.transform.pcr.outputModels.Practitioner model = (org.endeavourhealth.transform.pcr.outputModels.Practitioner) csvWriter;
        model.writeUpsert(id,
                organisationId,
                title,
                firstName,
                middleName,
                lastName,
                genderTermId,
                dateOfBirth,
                isActive,
                roleTermId,
                specialityTermId);

//        List<Identifier> identifiers = fhir.getIdentifier();
//
//        typeConceptId = IMClient.getOrCreateConceptId("");
//TODO smartcard etc identifiers  -how are they set up?
        //TODO work out which identifier to select
        org.endeavourhealth.transform.pcr.outputModels.PractitionerIdentifier idWriter = (org.endeavourhealth.transform.pcr.outputModels.PractitionerIdentifier) csvWriter;
       idWriter.writeUpsert(id,id,typeConceptId,"");

    }


}
