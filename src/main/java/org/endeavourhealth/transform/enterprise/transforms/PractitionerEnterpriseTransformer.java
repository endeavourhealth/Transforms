package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.ui.helpers.IdentifierHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PractitionerEnterpriseTransformer extends AbstractEnterpriseTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PractitionerEnterpriseTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Practitioner;
    }

    public boolean shouldAlwaysTransform() {
        return false;
    }

    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {

        if (resourceWrapper.isDeleted()) {
            csvWriter.writeDelete(enterpriseId.longValue());
            return;
        }

        Practitioner fhir = (Practitioner)resourceWrapper.getResource();

        long id;
        long organizationId;
        String name = null;
        String roleCode = null;
        String roleDesc = null;
        String gmcCode = null;

        id = enterpriseId.longValue();
        //LOG.debug("Transforming " + resource.getResourceType() + " " + resource.getId() + " -> " + id);

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

            //if not role value set option found, just get the free text of the role
            if (Strings.isNullOrEmpty(roleDesc)) {
                for (Coding coding : cc.getCoding()) {
                    if (coding.hasDisplay()) {
                        roleDesc = coding.getDisplay();
                    }
                }
            }



            if (role.hasManagingOrganization()) {
                Reference organisationReference = role.getManagingOrganization();
                practitionerEnterpriseOrgId = transformOnDemandAndMapId(organisationReference, params);
                //LOG.debug("Transformed managing org " + organisationReference.getReference() + " -> " + practitionerEnterpriseOrgId);
            }
            //LOG.trace("Got role with org ID " + practitionerEnterpriseOrgId + " from " + organisationReference);
        }

        //if we failed to find a proper organisation ID for the practitioner, assign it to the
        //organisation we're doing the transform for
        if (practitionerEnterpriseOrgId == null) {
            //LOG.trace("No role, so setting to the enterpriseOrganisationUuid " + enterpriseOrganisationUuid);
            practitionerEnterpriseOrgId = params.getEnterpriseOrganisationId();
            //LOG.debug("Had no managing org so used main org -> " + practitionerEnterpriseOrgId);
        }

        organizationId = practitionerEnterpriseOrgId.longValue();

        gmcCode = getGMCCode(fhir.getIdentifier());

        org.endeavourhealth.transform.enterprise.outputModels.Practitioner model = (org.endeavourhealth.transform.enterprise.outputModels.Practitioner)csvWriter;
        model.writeUpsert(id,
            organizationId,
            name,
            roleCode,
            roleDesc,
            gmcCode);
    }

    private static String getGMCCode(List<Identifier> identifiers) {
        return IdentifierHelper.getIdentifierBySystem(identifiers, FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER);
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
