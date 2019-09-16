package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PractitionerTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PractitionerTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Practitioner;
    }

    public boolean shouldAlwaysTransform() {
        return false;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Practitioner model = params.getOutputContainer().getPractitioners();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        Practitioner fhir = (Practitioner)FhirResourceHelper.deserialiseResouce(resourceWrapper);

        long organizationId;
        String name = null;
        String roleCode = null;
        String roleDesc = null;

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
                practitionerEnterpriseOrgId = transformOnDemandAndMapId(organisationReference, SubscriberTableId.ORGANIZATION, params);
            }
            //LOG.trace("Got role with org ID " + practitionerEnterpriseOrgId + " from " + organisationReference);
        }

        //if we failed to find a proper organisation ID for the practitioner, assign it to the
        //organisation we're doing the transform for
        if (practitionerEnterpriseOrgId == null) {
            //LOG.trace("No role, so setting to the enterpriseOrganisationUuid " + enterpriseOrganisationUuid);
            practitionerEnterpriseOrgId = params.getSubscriberOrganisationId();
        }

        organizationId = practitionerEnterpriseOrgId.longValue();


        model.writeUpsert(subscriberId,
            organizationId,
            name,
            roleCode,
            roleDesc);


    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.PRACTITIONER;
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
