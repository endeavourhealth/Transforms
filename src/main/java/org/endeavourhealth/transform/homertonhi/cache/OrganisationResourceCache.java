package org.endeavourhealth.transform.homertonhi.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.endeavourhealth.common.ods.OdsWebService.lookupOrganisationViaRest;
import static org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper.HOMERTON_UNIVERSITY_HOSPITAL_ODS;
import static org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper.ROYAL_FREE_HOSPITAL_ODS;

public class OrganisationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationResourceCache.class);

    private ResourceCache<String, OrganizationBuilder> organizationBuildersBySourceName = new ResourceCache<>();

    public OrganizationBuilder getOrCreateOrganizationBuilder(String organisationName,
                                                                     HomertonHiCsvHelper csvHelper,
                                                                     FhirResourceFiler fhirResourceFiler,
                                                                     AbstractCsvParser parser) throws Exception {

        //check the cache using the name from the data source
        OrganizationBuilder cachedResource
                = organizationBuildersBySourceName.getAndRemoveFromCache(organisationName);
        if (cachedResource != null) {
            return cachedResource;
        }

        //try to derive the ods code from the source name using a simple lookup.  This is based on known
        //organisation source description value in the extracts. Getting an exception here will allow us
        //to investigate the source data and add a new lookup if necessary.
        String odsCode = getOdsFromOrgName(organisationName);
        if (odsCode == null) {
            throw new Exception ("Unable to resolve Ods code for organisation name: "+organisationName);
        }

        OrganizationBuilder organizationBuilder;

        Organization organization
                    = (Organization) csvHelper.retrieveResourceForLocalId(ResourceType.Organization, odsCode);
        if (organization == null) {

            //if the Organization resource doesn't exist yet, create a new one using the ods code as the Id
            organizationBuilder = new OrganizationBuilder();
            organizationBuilder.setId(odsCode);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setValue(odsCode);

            //use the REST API to get official organisation details
            OdsOrganisation org = lookupOrganisationViaRest(odsCode);
            String orgName = org.getOrganisationName();
            organizationBuilder.setName(orgName);

            //save the organization resource
            fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
        } else {
            organizationBuilder = new OrganizationBuilder(organization);
        }

        return organizationBuilder;
    }

    public void returnOrganizationBuilder(String organisationName, OrganizationBuilder organizationBuilder) throws Exception {
        organizationBuildersBySourceName.addToCache(organisationName, organizationBuilder);
    }

    public void removeOrganizationFromCache(String organisationName) throws Exception {
        organizationBuildersBySourceName.removeFromCache(organisationName);
    }

    public void cleanUpResourceCache() {
        try {
            organizationBuildersBySourceName.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

    public String getOdsFromOrgName (String orgName) {

        //At the moment, only three organisation names have been detected in the extract data
        orgName = orgName.trim();
        switch (orgName) {

            case "Homerton University Hospital" : return HOMERTON_UNIVERSITY_HOSPITAL_ODS;
            case "Royal Free Hospital p0349" : return ROYAL_FREE_HOSPITAL_ODS;
            case "Royal Free Hospital p2349" : return ROYAL_FREE_HOSPITAL_ODS;
            default : return null;
        }
    }
}