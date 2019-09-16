package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrganisationEnterpriseTransformer extends AbstractEnterpriseTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationEnterpriseTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Organization;
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

        org.hl7.fhir.instance.model.Organization fhir = (org.hl7.fhir.instance.model.Organization)resourceWrapper.getResource();

        long id;
        String odsCode = null;
        String name = null;
        String typeCode = null;
        String typeDesc = null;
        String postcode = null;
        Long parentOrganisationId = null;

        id = enterpriseId.longValue();

        //LOG.trace("Transforming Organization " + fhir.getId() + " as enterprise ID " + id);

        if (fhir.hasIdentifier()) {
            odsCode = IdentifierHelper.findIdentifierValue(fhir.getIdentifier(), FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        }

        //if the organisation ODS code matches the one we're filing data for, replace the ID with the ID
        //we've pre-generated to use as our org ID
        /*if (odsCode != null
                && odsCode.equalsIgnoreCase(extractOrgOdsCode)) {
            EnterpriseIdHelper.saveEnterpriseOrganisationId(extractOrgOdsCode, enterpriseId);
        }*/

        //we have at least one Emis org without a name, which is against their spec, but we need to handle it
        if (fhir.hasName()) {
            name = fhir.getName();
        } else {
            name = "";
        }
        //name = fhir.getName();

        if (fhir.hasPartOf()) {
            Reference partOfReference = fhir.getPartOf();
            parentOrganisationId = transformOnDemandAndMapId(partOfReference, params);
        }

        if (fhir.hasType()) {
            CodeableConcept cc = fhir.getType();
            for (Coding coding: cc.getCoding()) {
                if (coding.getSystem().equals(FhirValueSetUri.VALUE_SET_ORGANISATION_TYPE)) {

                    typeCode = coding.getCode();
                    typeDesc = coding.getDisplay();

                }
            }
        }

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.ORGANISATION_MAIN_LOCATION)) {

                    Reference locationReference = (Reference)extension.getValue();

                    ResourceWrapper wrapper = params.findOrRetrieveResource(locationReference);
                    if (wrapper == null) {
                        //The Emis data contains organisations that refer to organisations that don't exist
                        LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " refers to " + locationReference.getReference() + " that doesn't exist");
                        continue;
                    }

                    Location location = (Location) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
                    if (location.hasAddress()) {
                        Address address = location.getAddress();
                        if (address.hasPostalCode()) {
                            postcode = address.getPostalCode();
                        }
                    }
                }

            }
        }

        //to align the target DB with TRUD, use the ODS code to find the official name, parent etc. so the
        //DB doesn't end up with whatever weirdness came from Emis, TPP etc.
        if (!Strings.isNullOrEmpty(odsCode)) {
            OdsOrganisation odsOrg = OdsWebService.lookupOrganisationViaRest(odsCode);
            if (odsOrg != null) {
                if (odsOrg.getOrganisationName() != null) {
                    name = odsOrg.getOrganisationName();
                }

                OrganisationType odsType = odsOrg.getOrganisationType();
                if (odsType != null) {
                    typeCode = odsType.getCode();
                    typeDesc = odsType.getDescription();
                }

                if (odsOrg.getPostcode() != null) {
                    postcode = odsOrg.getPostcode();
                }

                /*Map<String, String> parents = odsOrg.getParents();
                if (parents != null) {
                    SubscriberInstanceMappingDalI instanceMappingDal = DalProvider.factorySubscriberInstanceMappingDal(params.getSubscriberConfigName());

                    List<UUID> parentResourceIds = new ArrayList<>();

                    //for each parent ODS code, we need to use the instance mapping table to work back to a Resource UUID
                    for (String parentOdsCode: parents.keySet()) {
                        UUID parentResourceId = instanceMappingDal.findResourceIdFromInstanceMapping(ResourceType.Organization, parentOdsCode);
                        if (parentResourceId != null) {
                            parentResourceIds.add(parentResourceId);
                        }
                    }

                    if (parentResourceIds.isEmpty()) {
                        //not sure how to handle this, but also unsure if it will happen, so throw an exception and we'll see
                        throw new Exception("Failed to find FHIR Organization for parent ODS code(s) of " + odsCode + " for " + resource.getResourceType() + " " + resource.getId());

                    } else if (parentResourceIds.size() > 1) {
                        //not sure how to handle this, but also unsure if it will happen, so throw an exception and we'll see
                        throw new Exception("Multiple FHIR parent Organizations for parent ODS code(s) of " + odsCode + " for " + resource.getResourceType() + " " + resource.getId());

                    } else {
                        UUID parentResourceId = parentResourceIds.get(0);
                        parentOrganisationId = findEnterpriseId(params, ResourceType.Organization.toString(), parentResourceId.toString());
                        if (parentOrganisationId == null) {
                            throw new Exception("Failed to find enterprise ID for Organization " + parentOrganisationId + " which is a parent of " + resource.getResourceType() + " " + resource.getId());
                        }
                    }
                }*/
            }
        }

        org.endeavourhealth.transform.enterprise.outputModels.Organization model = (org.endeavourhealth.transform.enterprise.outputModels.Organization)csvWriter;
        model.writeUpsert(id,
            odsCode,
            name,
            typeCode,
            typeDesc,
            postcode,
            parentOrganisationId);
    }

}
