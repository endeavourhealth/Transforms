package org.endeavourhealth.transform.common;

import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceMergeDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceMergeMap;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ResourceMergeMapHelper {

    private static final ResourceMergeDalI resourceMergeRepository = DalProvider.factoryResourceMergeDal();

    private static Map<UUID, Map<String, String>> cachedResourceMergeMappings = new ConcurrentHashMap<>();

    public static Map<String, String> getResourceMergeMappings(UUID serviceId) throws Exception {

        Map<String, String> ret = cachedResourceMergeMappings.get(serviceId);
        if (ret == null) {

            //if null, hit the sync block and check again
            synchronized (cachedResourceMergeMappings) {

                ret = cachedResourceMergeMappings.get(serviceId);
                if (ret == null) {

                    ret = new HashMap<>();
                    List<ResourceMergeMap> mappings = resourceMergeRepository.retrieveMergeMappings(serviceId);

                    for (ResourceMergeMap mapping : mappings) {
                        String resourceType = mapping.getResourceType();
                        String sourceId = mapping.getSourceResourceId();
                        String destinationId = mapping.getDestinationResourceId();

                        String sourceReferenceValue = ReferenceHelper.createResourceReference(resourceType, sourceId);
                        String destinationReferenceValue = ReferenceHelper.createResourceReference(resourceType, destinationId);

                        ret.put(sourceReferenceValue, destinationReferenceValue);
                    }

                    cachedResourceMergeMappings.put(serviceId, ret);
                }
            }
        }
        return ret;
    }

    public static void saveResourceMergeMapping(UUID serviceId, Map<String, String> referenceMappings) throws Exception {
        for (String referenceFrom: referenceMappings.keySet()) {
            String referenceTo = referenceMappings.get(referenceFrom);
            saveResourceMergeMapping(serviceId, referenceFrom, referenceTo);
        }
    }

    public static void saveResourceMergeMapping(UUID serviceId, String referenceFrom, String referenceTo) throws Exception {

        ReferenceComponents compsFrom = ReferenceHelper.getReferenceComponents(new Reference().setReference(referenceFrom));
        ResourceType typeFrom = compsFrom.getResourceType();

        ReferenceComponents compsTo = ReferenceHelper.getReferenceComponents(new Reference().setReference(referenceTo));
        ResourceType typeTo = compsTo.getResourceType();

        if (typeFrom != typeTo) {
            throw new Exception("Trying to map to different resource type " + referenceFrom + " -> " + referenceTo);
        }

        UUID idFrom = UUID.fromString(compsFrom.getId());
        UUID idTo = UUID.fromString(compsTo.getId());

        //save to the DB
        resourceMergeRepository.insertMergeRecord(serviceId, typeFrom.toString(), idFrom, idTo);

        //but also update the cache
        Map<String, String> cachedMappings = getResourceMergeMappings(serviceId);
        cachedMappings.put(referenceFrom, referenceTo);

        //also check the cache for any reference chains (where we had A->B but are saving B->C)
        for (String oldReferenceFrom: cachedMappings.keySet()) {
            String oldReferenceTo = cachedMappings.get(oldReferenceFrom);
            if (oldReferenceTo.equals(referenceFrom)) {

                //update the DB with this new destination mapping
                ReferenceComponents oldComps = ReferenceHelper.getReferenceComponents(new Reference().setReference(oldReferenceFrom));
                UUID oldResourceIdFrom = UUID.fromString(oldComps.getId());
                resourceMergeRepository.insertMergeRecord(serviceId, typeFrom.toString(), oldResourceIdFrom, idTo);

                //and update the cache
                cachedMappings.put(oldReferenceFrom, referenceTo);
            }
        }
    }
}
