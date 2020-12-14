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
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

                OrganisationType odsType = findOdsOrganisationType(odsOrg, params);
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
    public static OrganisationType findOdsOrganisationType(OdsOrganisation odsRecord, HasServiceSystemAndExchangeIdI hasServiceSystemAndExchangeId) throws Exception {

        Set<OrganisationType> types = new HashSet<>(odsRecord.getOrganisationTypes());

        //if nothing to work with, return
        if (types.isEmpty()) {
            return null;
        }

        //if only one type, then use it
        if (types.size() == 1) {
            return types.iterator().next();
        }

        //if multiple types, try removing specific types until we get down to a single one
        List<OrganisationType> typesToRemove = new ArrayList<>();
        typesToRemove.add(OrganisationType.PRESCRIBING_COST_CENTRE); //EVERY GP practice has this
        typesToRemove.add(OrganisationType.STRATEGIC_PARTNERSHIP); //every STP is also a Strategic Partnership
        typesToRemove.add(OrganisationType.FOUNDATION_TRUST); //some hospital trusts as also foundation trusts
        typesToRemove.add(OrganisationType.HOSPICE); //Homerton is a Hospice as well as a trust
        typesToRemove.add(OrganisationType.REGISTERED_UNDER_PART_2_CARE_STDS_ACT_2000);
        typesToRemove.add(OrganisationType.LEVEL_04_PCT); ////another one - SD-263
        typesToRemove.add(OrganisationType.EXTENDED_ACCESS_HUB); //SD-267
        typesToRemove.add(OrganisationType.EXTENDED_ACCESS_PROVIDER); //SD-267
        typesToRemove.add(OrganisationType.NHS_TRUST_DERIVED); //SD-267
        typesToRemove.add(OrganisationType.LEVEL_04_PCT); //SD-267
        typesToRemove.add(OrganisationType.MEDICINE_SUPPLIER); //SD-267
        typesToRemove.add(OrganisationType.SPECIALISED_COMMISSIONING_HUB); //SD-267
        typesToRemove.add(OrganisationType.TREATMENT_CENTRE); //SD-267

        //for each of the above types, try removing from the set and if we're down to one, use it
        for (OrganisationType t: typesToRemove) {
            types.remove(t);

            if (types.size() == 1) {
                return types.iterator().next();
            }
        }

        //SD-267 we keep hitting this error due to a load of old data that's come through. Rather than just keep hacking
        //at the above, I'm going to just select the first org type and log that it's happened

        if (hasServiceSystemAndExchangeId != null) {
            TransformWarnings.log(LOG, hasServiceSystemAndExchangeId, "Unable to select best type for ODS organisation {}", odsRecord.getOdsCode());
        } else {
            LOG.warn("Unable to select best type for ODS organisation " + odsRecord.getOdsCode());
        }

        List<OrganisationType> typeList = new ArrayList<>(types);
        typeList.sort((o1, o2) -> o1.getDescription().compareTo(o2.getDescription())); //sort so any output is at least consistent
        return typeList.get(0);

        /*List<String> typeList = types
                .stream()
                .map(t -> t.toString())
                .collect(Collectors.toList());
        String typeStr = String.join(", ", typeList);
        throw new Exception("Unable to determine best org type for ODS record " + odsRecord.getOdsCode() + " with types [" + typeStr + "]");*/
    }

    /**
     * returns the "best" parent organisation from a ODS record. ODS allows organisations to have multiple parents,
     * so this function tries to find the one best suited for the expected hierarchy (GP practice -> CCG -> STP).
     */
    public static OdsOrganisation findParentOrganisation(OdsOrganisation odsRecord, HasServiceSystemAndExchangeIdI hasServiceSystemAndExchangeId) throws Exception {

        List<OdsOrganisation> parents = new ArrayList<>(odsRecord.getParents().values());

        //if no parents, then nothing to do
        if (parents.isEmpty()) {
            return null;
        }

        //if just one parent, then it's easy
        if (parents.size() == 1) {
            return parents.get(0);
        }

        List<OrganisationType> parentTypesToIgnore = new ArrayList<>();
        parentTypesToIgnore.add(OrganisationType.PRIMARY_CARE_NETWORK); //most practices have one or more PCNs as a parent, in addition to CCG, so remove thos
        parentTypesToIgnore.add(OrganisationType.SUSTAINABILITY_TRANSFORMATION_PARTNERSHIP); //some orgs have an STP as their direct parent and a parent further up, so remove as the direct parent
        parentTypesToIgnore.add(OrganisationType.GOVERNMENT_OFFICE_REGION); //old SHAs have this as an odd parent
        parentTypesToIgnore.add(OrganisationType.HEALTH_AUTHORITY);

        for (OrganisationType t: parentTypesToIgnore) {

            for (int i = parents.size() - 1; i >= 0; i--) {
                OdsOrganisation parent = parents.get(i);
                if (parent.getOrganisationTypes().contains(t)) {
                    parents.remove(i);
                }
            }
            if (parents.size() == 1) {
                return parents.get(0);
            }
        }

        //SD-267 we keep hitting this error due to a load of old data that's come through. Rather than just keep hacking
        //at the above, I'm going to just select the first org type and log that it's happened

        if (hasServiceSystemAndExchangeId != null) {
            TransformWarnings.log(LOG, hasServiceSystemAndExchangeId, "Unable to select best parent for ODS organisation {}", odsRecord.getOdsCode());
        } else {
            LOG.warn("Unable to select best parent for ODS organisation " + odsRecord.getOdsCode());
        }

        List<OdsOrganisation> typeList = new ArrayList<>(parents);
        typeList.sort((o1, o2) -> o1.getOdsCode().compareTo(o2.getOdsCode())); //sort so any output is at least consistent
        return typeList.get(0);

        //throw new Exception("Unable to determine best org parent for ODS record " + odsRecord.getOdsCode());
    }

}
