package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PractitionerTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PractitionerTransformer.class);

    public boolean shouldAlwaysTransform() {
        return false;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractEnterpriseCsvWriter csvWriter,
                          EnterpriseTransformParams params) throws Exception {

        Practitioner fhir = (Practitioner)resource;

        long id;
        long organizaationId;
        String name = null;
        String roleCode = null;
        String roleDesc = null;

        id = enterpriseId.longValue();

        if (fhir.hasName()) {
            HumanName fhirName = fhir.getName();
            name = fhirName.getText();

            //the Practitioners from the HL7 Receiver don't have their name set as the transform doesn't use
            //the standard functions written to populate names, so we need to manually build the name from the elements
            if (Strings.isNullOrEmpty(name)) {
                name = createNameFromElements(fhirName);
            }
        }

        Long practitionerEnterpriseOrgId = null;
        //LOG.trace("Transforming practitioner " + fhir.getId() + " with " + fhir.getPractitionerRole().size() + " roles and enterpriseOrganisationUuid " + enterpriseOrganisationUuid);
        for (Practitioner.PractitionerPractitionerRoleComponent role : fhir.getPractitionerRole()) {

            CodeableConcept cc = role.getRole();
            for (Coding coding : cc.getCoding()) {
                if (coding.getSystem().equals(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES)) {
                    roleCode = coding.getCode();
                    roleDesc = coding.getDisplay();
                }
            }

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

        organizaationId = practitionerEnterpriseOrgId.longValue();

        org.endeavourhealth.transform.enterprise.outputModels.Practitioner model = (org.endeavourhealth.transform.enterprise.outputModels.Practitioner)csvWriter;
        model.writeUpsert(id,
            organizaationId,
            name,
            roleCode,
            roleDesc);
    }

    private static String createNameFromElements(HumanName name) {
        String ret = "";

        if (name.hasPrefix()) {
            for (StringType s : name.getPrefix()) {
                ret += s.getValueNotNull();
                ret += " ";
            }
        }

        if (name.hasGiven()) {
            for (StringType s : name.getGiven()) {
                ret += s.getValueNotNull();
                ret += " ";
            }
        }

        if (name.hasFamily()) {
            for (StringType s : name.getFamily()) {
                ret += s.getValueNotNull();
                ret += " ";
            }
        }

        ret = ret.trim();

        if (Strings.isNullOrEmpty(ret)) {
            return null;
        } else {
            return ret;
        }
    }
}
