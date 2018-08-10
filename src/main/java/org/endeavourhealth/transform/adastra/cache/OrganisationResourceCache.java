package org.endeavourhealth.transform.adastra.cache;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrganisationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationResourceCache.class);

    private ResourceCache<String, OrganizationBuilder> organizationBuildersByLocationID = new ResourceCache<>();

    public OrganizationBuilder getOrCreateOrganizationBuilder(String orgId,
                                                              AdastraCsvHelper csvHelper,
                                                              FhirResourceFiler fhirResourceFiler,
                                                              AbstractCsvParser parser) throws Exception {

        OrganizationBuilder cachedResource
                = organizationBuildersByLocationID.getAndRemoveFromCache(orgId);
        if (cachedResource != null) {
            return cachedResource;
        }

        OrganizationBuilder organizationBuilder = null;

        Organization organization
                = (Organization) csvHelper.retrieveResource(orgId, ResourceType.Organization, fhirResourceFiler);
        if (organization == null) {

            //if the Organization resource doesn't exist yet, create a new one using the ServiceId or ODS code (if a provider)
            organizationBuilder = new OrganizationBuilder();
            organizationBuilder.setId(orgId);

        } else {
            organizationBuilder = new OrganizationBuilder(organization);
        }

        return organizationBuilder;
    }

    public boolean organizationInCache(String orgId) {
        return organizationBuildersByLocationID.contains(orgId);
    }

    public void returnOrganizationBuilder(String orgId, OrganizationBuilder organizationBuilder) throws Exception {
        organizationBuildersByLocationID.addToCache(orgId, organizationBuilder);
    }

    public void removeOrganizationFromCache(String orgId) throws Exception {
        organizationBuildersByLocationID.removeFromCache(orgId);
    }

    public void cleanUpResourceCache() {
        try {
            organizationBuildersByLocationID.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }
}
