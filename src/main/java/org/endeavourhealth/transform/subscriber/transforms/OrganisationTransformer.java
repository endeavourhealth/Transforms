package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberInstanceMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class OrganisationTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationTransformer.class);

    public boolean shouldAlwaysTransform() {
        return false;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Organization model = params.getOutputContainer().getOrganisations();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);

            return;
        }

        Organization fhir = (Organization)FhirResourceHelper.deserialiseResouce(resourceWrapper);


        String odsCode = null;
        String name = null;
        String typeCode = null;
        String typeDesc = null;
        String postcode = null;
        Long parentOrganisationId = null;

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

                    Location location = (Location)findResource(locationReference, params);
                    if (location == null) {
                        //The Emis data contains organisations that refer to organisations that don't exist
                        LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " refers to " + locationReference.getReference() + " that doesn't exist");
                        continue;
                    }

                    if (location != null
                            && location.hasAddress()) {
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
                    SubscriberInstanceMappingDalI instanceMappingDal = DalProvider.factorySubscriberInstanceMappingDal(params.getEnterpriseConfigName());

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
                        throw new Exception("Failed to find FHIR Organization for parent ODS code(s) of " + odsCode + " for " + resourceWrapper.getResourceType() + " " + resourceWrapper.getResourceId());

                    } else if (parentResourceIds.size() > 1) {
                        //not sure how to handle this, but also unsure if it will happen, so throw an exception and we'll see
                        throw new Exception("Multiple FHIR parent Organizations for parent ODS code(s) of " + odsCode + " for " + resourceWrapper.getResourceType() + " " + resourceWrapper.getResourceId());

                    } else {
                        UUID parentResourceId = parentResourceIds.get(0);
                        Reference parentReference = ReferenceHelper.createReference(ResourceType.Organization, parentResourceId.toString());
                        parentOrganisationId = findEnterpriseId(params, SubscriberTableId.ORGANIZATION, parentReference);
                        if (parentOrganisationId == null) {
                            throw new Exception("Failed to find subscriber ID for Organization " + parentOrganisationId + " which is a parent of " + resourceWrapper.getResourceType() + " " + resourceWrapper.getResourceId());
                        }
                    }
                }*/
            }
        }

        model.writeUpsert(subscriberId,
            odsCode,
            name,
            typeCode,
            typeDesc,
            postcode,
            parentOrganisationId);


    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.ORGANIZATION;
    }

}
