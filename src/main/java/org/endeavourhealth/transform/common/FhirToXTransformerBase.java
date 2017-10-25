package org.endeavourhealth.transform.common;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FhirToXTransformerBase {

    protected static List<ResourceWrapper> getResources(UUID batchId, Map<ResourceType, List<UUID>> resourceIds) throws Exception {

        //retrieve our resources
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> resourcesByExchangeBatch = resourceDal.getResourcesForBatch(batchId);
        List<ResourceWrapper> filteredResources = filterResources(resourcesByExchangeBatch, resourceIds);
        return filteredResources;
    }

    private static List<ResourceWrapper> filterResources(List<ResourceWrapper> allResources,
                                                                 Map<ResourceType, List<UUID>> resourceIdsToKeep) throws Exception {

        List<ResourceWrapper> ret = new ArrayList<>();

        for (ResourceWrapper resource: allResources) {
            UUID resourceId = resource.getResourceId();
            ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());

            //the map of resource IDs tells us the resources that passed the protocol and should be passed
            //to the subscriber. However, any resources that should be deleted should be passed, whether the
            //protocol says to include it or not, since it may have previously been passed to the subscriber anyway
            if (resource.isDeleted()) {
                ret.add(resource);

            } else {

                //during testing, the resource ID is null, so handle this
                if (resourceIdsToKeep == null) {
                    ret.add(resource);
                    continue;
                }

                List<UUID> uuidsToKeep = resourceIdsToKeep.get(resourceType);
                if (uuidsToKeep != null
                        || uuidsToKeep.contains(resourceId)) {
                    ret.add(resource);
                }
            }
        }

        return ret;
    }
}
