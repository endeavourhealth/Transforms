package org.endeavourhealth.transform.adastra.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
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
                                                                     AdastraCsvHelper csvHelper,
                                                                     FhirResourceFiler fhirResourceFiler,
                                                                     AbstractCsvParser parser) throws Exception {

        OrganizationBuilder organizationBuilder = OrganizationBuildersByUUID.get(serviceId);
        if (organizationBuilder == null) {

            LOG.trace("1 - Organization Builder by serviceId: {} - not cached try  retrieve", serviceId);

            Organization organization
                    = (Organization) csvHelper.retrieveResource(serviceId.toString(), ResourceType.Organization, fhirResourceFiler);
            if (organization == null) {
                LOG.trace("2 - Organization Builder by serviceId: {} - not in DB try to create", serviceId);

                //if the Organization resource doesn't exist yet, create a new one using the ServiceId
                organizationBuilder = new OrganizationBuilder();
                organizationBuilder.setId(serviceId.toString());

                //lookup the Service details from DDS
                Service service = csvHelper.getService(serviceId);
                if (service != null) {
                    LOG.trace("3 - Organization Builder by serviceId: {} - Service object retrieved OK", serviceId);

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

                    LOG.trace("4 - Organization Builder by serviceId: {} - Name set: {}", serviceId, serviceName);
                }

                //save the new OOH organization resource
                LOG.trace("5 - Organization Builder by serviceId: {} - try to save builder instance", serviceId);
                fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
                LOG.trace("6 - Organization Builder by serviceId: {} - builder instance saved OK", serviceId);
            } else {
                organizationBuilder = new OrganizationBuilder(organization);
            }

            //cache the new resource
            OrganizationBuildersByUUID.put(serviceId, organizationBuilder);
            LOG.trace("7 - Organization Builder by serviceId: {} - builder instance cached OK", serviceId);
        }
        return organizationBuilder;
    }

    public static void clear() {
        OrganizationBuildersByUUID.clear();
    }
}
