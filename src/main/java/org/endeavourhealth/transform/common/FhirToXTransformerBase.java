package org.endeavourhealth.transform.common;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.*;

public class FhirToXTransformerBase {

    protected static List<ResourceWrapper> getResources(UUID batchId, Map<ResourceType, List<UUID>> resourceIds) throws Exception {

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();

        List<ResourceWrapper> resources = resourceDal.getResourcesForBatch(batchId);
        resources = filterResources(resources, resourceIds);
        resources = pruneOlderDuplicates(resources);

        return resources;
    }

    /**
     * we can end up with multiple instances of the same resource in a batch (or at least the Emis test data can)
     * so strip out all but the latest version of each resource, so we're not wasting time sending over
     * data that will immediately be overwritten and also we don't need to make sure to process them in order
     */
    private static List<ResourceWrapper> pruneOlderDuplicates(List<ResourceWrapper> resources) {

        HashMap<UUID, UUID> hmLatestVersion = new HashMap<>();
        List<ResourceWrapper> ret = new ArrayList<>();

        for (ResourceWrapper resource: resources) {
            UUID id = resource.getResourceId();
            UUID version = resource.getVersion();

            UUID latestVersion = hmLatestVersion.get(id);
            if (latestVersion == null) {
                hmLatestVersion.put(id, version);

            } else {
                int comp = version.compareTo(latestVersion);
                if (comp > 0) {
                    hmLatestVersion.put(id, latestVersion);
                }
            }
        }

        for (ResourceWrapper resource: resources) {
            UUID id = resource.getResourceId();
            UUID version = resource.getVersion();

            UUID latestVersion = hmLatestVersion.get(id);
            if (latestVersion.equals(version)) {
                ret.add(resource);
            }
        }

        return ret;
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
