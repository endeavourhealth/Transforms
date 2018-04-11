package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class OrganisationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationResourceCache.class);

    private static Map<Long, OrganizationBuilder> organizationBuildersByRowId = new HashMap<>();

    public static OrganizationBuilder getOrganizationBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        OrganizationBuilder organizationBuilder = organizationBuildersByRowId.get(rowIdCell.getLong());
        if (organizationBuilder == null) {

            Organization organization
                    = (Organization)csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.Organization, fhirResourceFiler);
            if (organization == null) {
                //if the Location doesn't exist yet, create a new one
                organizationBuilder = new OrganizationBuilder();
                organizationBuilder.setId(rowIdCell.getString(), rowIdCell);
            } else {
                organizationBuilder = new OrganizationBuilder(organization);
            }
            organizationBuildersByRowId.put(rowIdCell.getLong(), organizationBuilder);
        }
        return organizationBuilder;
    }

    public static boolean OrganizationInCache(CsvCell rowIdCell)  {
        return organizationBuildersByRowId.containsKey(rowIdCell.getLong());
    }

    public static void fileOrganizationResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long rowId: organizationBuildersByRowId.keySet()) {
            OrganizationBuilder organizationBuilder = organizationBuildersByRowId.get(rowId);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
        }

        //clear down as everything has been saved
        organizationBuildersByRowId.clear();
    }
}
