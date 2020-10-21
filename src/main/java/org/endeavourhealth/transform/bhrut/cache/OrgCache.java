package org.endeavourhealth.transform.bhrut.cache;

import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.endeavourhealth.common.ods.OdsWebService.lookupOrganisationViaRest;

public class OrgCache {
    // Basic org code or org name cache
    private static final Logger LOG = LoggerFactory.getLogger(StaffCache.class);
    private static final String NON_ODS_CODE= "NON ODS CODE";

    private ResourceCache<String, OrganizationBuilder> organizationBuildersByLocationID = new ResourceCache<>();

    private Map<String, String> orgCodeToName = new ConcurrentHashMap<>();

    public void addOrgCode(CsvCell orgCodeCell, CsvCell orgNameCell) throws Exception {

        String cCode = orgCodeCell.getString();
        String name = orgNameCell.getString();
        if (!orgCodeToName.containsKey(cCode)) {
            OdsOrganisation or = lookupOrganisationViaRest(cCode);
            if (or == null) {
                orgCodeToName.put(cCode, NON_ODS_CODE);
                return;
            }  // BHRUT sometimes ePact codes instead of ODS. Ignore them
            orgCodeToName.put(cCode, name);
        }
    }

    public boolean orgCodeInCache(String cCode) {
        return orgCodeToName.containsKey(cCode);
    }

    public boolean organizationInDB(String orgId, BhrutCsvHelper csvHelper) throws Exception  {
        return (csvHelper.retrieveResource(orgId, ResourceType.Organization) != null);
    }



    public String getNameForOrgCode(String cCode) {
        if (!orgCodeInCache(cCode)) {
            return null;
        }
        return orgCodeToName.get(cCode);
    }

    public void empty() {
        orgCodeToName.clear();
    }

    public long size() {
        return orgCodeToName.size();
    }

    public boolean organizationInCache(String orgId) {
        return organizationBuildersByLocationID.contains(orgId);
    }

    public OrganizationBuilder getOrCreateOrganizationBuilder(String orgId,
                                                              BhrutCsvHelper csvHelper) throws Exception {

        OrganizationBuilder cachedResource
                = organizationBuildersByLocationID.getAndRemoveFromCache(orgId);
        if (cachedResource != null) {
            return cachedResource;
        }

        OrganizationBuilder organizationBuilder = null;

        Organization organization
                = (Organization) csvHelper.retrieveResource(orgId, ResourceType.Organization);
        if (organization == null) {

            //if the Organization resource doesn't exist yet, create a new one using the ServiceId or ODS code (if a provider)
            organizationBuilder = new OrganizationBuilder();
            organizationBuilder.setId(orgId);

        } else {
            organizationBuilder = new OrganizationBuilder(organization);
        }

        return organizationBuilder;
    }

    public void cacheOrganizationBuilder(String orgId, OrganizationBuilder organizationBuilder) throws Exception {
        organizationBuildersByLocationID.addToCache(orgId, organizationBuilder);
    }
}
