package org.endeavourhealth.transform.homerton.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class OrganisationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationResourceCache.class);

    private ResourceCache<UUID, OrganizationBuilder> organizationBuildersByLocationUUID = new ResourceCache<>();

    public OrganizationBuilder getOrCreateOrganizationBuilder(UUID serviceId,
                                                                     HomertonCsvHelper csvHelper,
                                                                     FhirResourceFiler fhirResourceFiler,
                                                                     AbstractCsvParser parser) throws Exception {
        //check the cache
        OrganizationBuilder cachedResource = organizationBuildersByLocationUUID.getAndRemoveFromCache(serviceId);
        if (cachedResource != null) {
            return cachedResource;
        }

        OrganizationBuilder organizationBuilder = null;

        Organization organization
                    = (Organization) csvHelper.retrieveResourceForLocalId(ResourceType.Organization, serviceId.toString());
        if (organization == null) {

            //if the Organization resource doesn't exist yet, create a new one using the ServiceId
            organizationBuilder = new OrganizationBuilder();
            organizationBuilder.setId(serviceId.toString());

            //lookup the Service details from DDS
            Service service = csvHelper.getService(serviceId);
            if (service != null) {

                String localId = service.getLocalId();
                if (!localId.isEmpty()) {
                    IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
                    identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
                    identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
                    identifierBuilder.setValue(localId);
                }

                String serviceName = service.getName();
                if (!serviceName.isEmpty()) {
                    organizationBuilder.setName(serviceName);
                }
            }

            //save the Homerton organization resource
            fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
        } else {
            organizationBuilder = new OrganizationBuilder(organization);
        }

        return organizationBuilder;
    }

    public void returnOrganizationBuilder(UUID serviceId, OrganizationBuilder organizationBuilder) throws Exception {
        organizationBuildersByLocationUUID.addToCache(serviceId, organizationBuilder);
    }

    public void removeOrganizationFromCache(UUID serviceId) throws Exception {
        organizationBuildersByLocationUUID.removeFromCache(serviceId);
    }

    public void cleanUpResourceCache() {
        try {
            organizationBuildersByLocationUUID.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }
}
