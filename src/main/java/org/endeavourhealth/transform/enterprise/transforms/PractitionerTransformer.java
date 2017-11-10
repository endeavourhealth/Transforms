package org.endeavourhealth.transform.enterprise.transforms;

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

    protected void transform(Long enterpriseId,
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

            Reference organisationReference = role.getManagingOrganization();
            practitionerEnterpriseOrgId = transformOnDemandAndMapId(organisationReference, params);
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
}
