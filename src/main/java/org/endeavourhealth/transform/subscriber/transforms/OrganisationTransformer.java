package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OrganisationTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Organization;
    }

    public boolean shouldAlwaysTransform() {
        return false;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Organization model = params.getOutputContainer().getOrganisations();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        Organization fhir = (Organization)resourceWrapper.getResource();

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
            parentOrganisationId = transformOnDemandAndMapId(partOfReference, SubscriberTableId.ORGANIZATION, params);
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

                    Location location = (Location)params.findOrRetrieveResource(locationReference);
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

                OrganisationType odsType = findOdsOrganisationType(odsOrg);
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

    /**
     * returns the "best" type of an ODS Organisation (which can contain multiple)
     *
     * this isn't very elegant, but I've based it on looking at what's in ODS for the
     * existing DDS publishers (and their parents) and returns acceptable results (see SD-201)
     */
    public static OrganisationType findOdsOrganisationType(OdsOrganisation odsRecord) throws Exception {

        Set<OrganisationType> types = new HashSet<>(odsRecord.getOrganisationTypes());

        //if nothing to work with, return
        if (types.isEmpty()) {
            return null;
        }

        //if only one type, then use it
        if (types.size() == 1) {
            return types.iterator().next();
        }

        //if multiple types, try removing this specific type, since EVERY GP practice has this
        types.remove(OrganisationType.PRESCRIBING_COST_CENTRE);
        if (types.size() == 1) {
            return types.iterator().next();
        }

        //if still multiple ones, try removing this type, since every STP is also a Strategic Partnership
        types.remove(OrganisationType.STRATEGIC_PARTNERSHIP);
        if (types.size() == 1) {
            return types.iterator().next();
        }

        //if still multiple ones, try removing this type, since some hospital trusts as also foundation trusts
        types.remove(OrganisationType.FOUNDATION_TRUST);
        if (types.size() == 1) {
            return types.iterator().next();
        }

        //if still multiple ones, try removing this type, since Homerton is a Hosptce as well as a trust
        types.remove(OrganisationType.HOSPICE);
        if (types.size() == 1) {
            return types.iterator().next();
        }

        //another one
        types.remove(OrganisationType.REGISTERED_UNDER_PART_2_CARE_STDS_ACT_2000);
        if (types.size() == 1) {
            return types.iterator().next();
        }

        //another one - SD-263
        types.remove(OrganisationType.LEVEL_04_PCT);
        if (types.size() == 1) {
            return types.iterator().next();
        }

        throw new Exception("Unable to determine best org type for ODS record " + odsRecord.getOdsCode());
    }

    /**
     * returns the "best" parent organisation from a ODS record. ODS allows organisations to have multiple parents,
     * so this function tries to find the one best suited for the expected hierarchy (GP practice -> CCG -> STP).
     */
    public static OdsOrganisation findParentOrganisation(OdsOrganisation odsRecord) throws Exception {

        List<OdsOrganisation> parents = new ArrayList<>(odsRecord.getParents().values());

        //if no parents, then nothing to do
        if (parents.isEmpty()) {
            return null;
        }

        //if just one parent, then it's easy
        if (parents.size() == 1) {
            return parents.get(0);
        }

        //most practices have one or more PCNs as a parent, in addition to CCG, so remove thos
        for (int i=parents.size()-1; i>=0; i--) {
            OdsOrganisation parent = parents.get(i);
            if (parent.getOrganisationTypes().contains(OrganisationType.PRIMARY_CARE_NETWORK)) {
                parents.remove(i);
            }
        }
        if (parents.size() == 1) {
            return parents.get(0);
        }

        throw new Exception("Unable to determine best org parent for ODS record " + odsRecord.getOdsCode());
    }

}
