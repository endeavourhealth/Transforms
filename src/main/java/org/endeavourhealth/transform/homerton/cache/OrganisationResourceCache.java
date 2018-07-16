package org.endeavourhealth.transform.homerton.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class OrganisationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationResourceCache.class);

    private static Map<UUID, OrganizationBuilder> OrganizationBuildersByUUID = new HashMap<>();

    public static OrganizationBuilder getOrCreateOrganizationBuilder(UUID serviceId,
                                                                     HomertonCsvHelper csvHelper,
                                                                     FhirResourceFiler fhirResourceFiler,
                                                                     AbstractCsvParser parser) throws Exception {

        OrganizationBuilder organizationBuilder = OrganizationBuildersByUUID.get(serviceId);
        if (organizationBuilder == null) {

            Organization organization
                    = (Organization) csvHelper.retrieveResource(ResourceType.Organization, serviceId);
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

            //cache the new resource
            OrganizationBuildersByUUID.put(serviceId, organizationBuilder);
        }
        return organizationBuilder;
    }

    public static void clear() {
        OrganizationBuildersByUUID.clear();
    }
}
