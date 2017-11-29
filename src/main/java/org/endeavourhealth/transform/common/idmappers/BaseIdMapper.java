package org.endeavourhealth.transform.common.idmappers;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseIdMapper {
    private static final Logger LOG = LoggerFactory.getLogger(BaseIdMapper.class);


    public abstract void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception;
    public abstract void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception;
    /*public abstract boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception;
    public abstract void remapIds(Resource resource, Map<String, String> idMappings) throws Exception;*/
    public abstract String getPatientId(Resource resource) throws PatientResourceException;


    protected void addCommonResourceReferences(DomainResource resource, Set<String> referenceValues) throws Exception {

        if (resource.hasExtension()) {

            for (Extension extension : resource.getExtension()) {
                if (extension.hasValue()
                        && extension.getValue() instanceof Reference) {

                    Reference reference = (Reference)extension.getValue();
                    addReference(reference, referenceValues);
                }

                //need to handle nested extensions within extensions
                if (extension.hasExtension()) {
                    for (Extension nestedExtension : extension.getExtension()) {
                        if (nestedExtension.hasValue()
                                && nestedExtension.getValue() instanceof Reference) {

                            Reference reference = (Reference)nestedExtension.getValue();
                            addReference(reference, referenceValues);
                        }
                    }
                }
            }
        }

        if (resource.hasContained()) {
            //for each contained resource, we just use it's own ID mapper class to get its references.
            //Note that this means we don't map the ID on the contained resources, but that's fine as
            //the ID of contained resources is not a global ID
            for (Resource contained : resource.getContained()) {
                BaseIdMapper idMapper = IdHelper.getIdMapper(contained);
                idMapper.getResourceReferences(contained, referenceValues);
            }
        }
    }

    protected void addIndentifierReferences(List<Identifier> identifiers, Set<String> ret) {
        for (Identifier identifier: identifiers) {
            if (identifier.hasAssigner()) {
                addReference(identifier.getAssigner(), ret);
            }
        }
    }

    protected void addReference(Reference reference, Set<String> ret) {
        if (reference == null) {
            return;
        }

        if (!reference.hasReference()) {
            return;
        }

        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        if (comps == null) {
            //if the reference is to an internal contained resource, the above will return null
            return;
        }

        String value = reference.getReference();
        if (Strings.isNullOrEmpty(value)) {
            return;
        }

        ret.add(value);
    }

    protected void addReferences(List<Reference> references, Set<String> ret) {
        if (references == null
                || references.isEmpty()) {
            return;
        }

        for (Reference reference: references) {
            addReference(reference, ret);
        }
    }

    /**
     * maps the extensions and contained resources in a FHIR resource
     */
    protected void mapCommonResourceFields(DomainResource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        mapExtensions(resource, mappings, failForMissingMappings);
        mapContainedResources(resource, mappings, failForMissingMappings);
    }

    /**
     * maps the IDs in any extensions of a resource
     */
    private void mapExtensions(DomainResource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {

        if (!resource.hasExtension()) {
            return;
        }

        for (Extension extension: resource.getExtension()) {
            if (extension.hasValue()
                    && extension.getValue() instanceof Reference) {
                mapReference((Reference)extension.getValue(), mappings, failForMissingMappings);
            }
        }
    }


    /**
     * maps the IDs in any extensions of a resource
     */
    private void mapContainedResources(DomainResource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {

        if (!resource.hasContained()) {
            return;
        }

        for (Resource contained: resource.getContained()) {
            BaseIdMapper idMapper = IdHelper.getIdMapper(contained);
            idMapper.applyReferenceMappings(contained, mappings, failForMissingMappings);
        }
    }

    /**
     * maps the IDs in any identifiers of a resource
     */
    protected void mapIdentifiers(List<Identifier> identifiers, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        for (Identifier identifier: identifiers) {
            if (identifier.hasAssigner()) {
                mapReference(identifier.getAssigner(), mappings, failForMissingMappings);
            }
        }
    }

    protected void mapReference(Reference reference, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        if (reference == null) {
            return;
        }

        if (reference.hasReference()) {

            String referenceValue = reference.getReference();
            String newReferenceValue = mappings.get(referenceValue);
            if (Strings.isNullOrEmpty(newReferenceValue)) {

                //normally, the lack of a mapped reference is a bad thing, but before we log an error,
                //check to see if it's because our reference is actually a reference to an external resource.
                //If it's a reference to an internal resource, then no mapped ID will have been generated
                ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
                if (comps == null) {
                    //if there's no resource type in the reference, then just return out, since there's no mapping to be done
                    //note it's quicker to only check the reference components in the event of a null mapping, since
                    //it's slower to tokenise the reference string than look up in the map first
                    return;
                }

                //when mapping new resources, we want to know about missing mappings, but for ADT A34 merges, we only change some fields
                if (failForMissingMappings) {
                    throw new Exception("Failed to find mapping for reference " + reference.getReference());
                } else {
                    return;
                }
            }

            reference.setReference(newReferenceValue);
        }
    }

    protected void mapReferences(List<Reference> references, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        if (references == null
                || references.isEmpty()) {
            return;
        }

        for (Reference reference: references) {
            mapReference(reference, mappings, failForMissingMappings);
        }
    }

    /**
     * maps the ID, extensions and contained resources in a FHIR resource
     * returns true to indicate the resource is new to EDS, false if not new or we didn't map it's ID
     */
    /*protected boolean mapCommonResourceFields(DomainResource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        boolean isNewResource = false;
        if (mapResourceId) {
            isNewResource = mapResourceId(resource, serviceId, systemId);
        }
        mapExtensions(resource, serviceId, systemId);
        mapContainedResources(resource, serviceId, systemId);

        return isNewResource;
    }*/

    /**
     * maps the main ID of any resource
     * returns true if the resource is new to EDS
     */
    /*private boolean mapResourceId(Resource resource, UUID serviceId, UUID systemId) throws Exception {

        if (!resource.hasId()) {
            return false;
        }

        UUID existingEdsId = IdHelper.getEdsResourceId(serviceId, systemId, resource.getResourceType(), resource.getId());
        if (existingEdsId == null) {
            //if no existing ID was found, create a new one and return true
            String newId = IdHelper.getOrCreateEdsResourceIdString(serviceId, systemId, resource.getResourceType(), resource.getId());
            resource.setId(newId);
            return true;

        } else {
            resource.setId(existingEdsId.toString());
            return false;
        }
    }*/

    /**
     * maps the IDs in any extensions of a resource
     */
    /*private void mapExtensions(DomainResource resource, UUID serviceId, UUID systemId) throws Exception {

        if (!resource.hasExtension()) {
            return;
        }

        for (Extension extension: resource.getExtension()) {
            if (extension.hasValue()
                && extension.getValue() instanceof Reference) {
                mapReference((Reference)extension.getValue(), serviceId, systemId);
            }

            //need to handle nested extensions within extensions
            if (extension.hasExtension()) {
                for (Extension nestedExtension: extension.getExtension()) {
                    if (nestedExtension.hasValue()
                            && nestedExtension.getValue() instanceof Reference) {
                        mapReference((Reference)nestedExtension.getValue(), serviceId, systemId);
                    }
                }
            }
        }
    }*/

    /**
     * maps the IDs in any extensions of a resource
     */
    /*private void mapContainedResources(DomainResource resource, UUID serviceId, UUID systemId) throws Exception {

        if (!resource.hasContained()) {
            return;
        }

        for (Resource contained: resource.getContained()) {
            //pass in false so we don't map the ID of the contained resource, since it's not supposed to be a global ID
            IdHelper.mapIds(serviceId, systemId, contained, false);
        }
    }*/

    /*protected void remapCommonResourceFields(DomainResource resource, Map<String, String> idMappings) throws Exception {
        remapResourceId(resource, idMappings);
        remapExtensions(resource, idMappings);
        remapContainedResources(resource, idMappings);
    }*/

    /*private void remapResourceId(Resource resource, Map<String, String> idMappings) {

        if (!resource.hasId()) {
            LOG.debug("" + resource.getResourceType() + " " + resource.getId() + " doesn't have an ID to remap");
            return;
        }

        String referenceValue = ReferenceHelper.createResourceReference(resource.getResourceType(), resource.getId());
        String newReferenceValue = idMappings.get(referenceValue);
        if (Strings.isNullOrEmpty(newReferenceValue)) {
            LOG.debug("" + resource.getResourceType() + " " + resource.getId() + " maps to [" + newReferenceValue + "] new ID so not remapping");
            return;
        }

        Reference newReference = new Reference().setReference(newReferenceValue);
        String newId = ReferenceHelper.getReferenceId(newReference);
        LOG.debug("" + resource.getResourceType() + " " + resource.getId() + " maps to [" + newReferenceValue + "] so new ID is [" + newId + "]");

        resource.setId(newId);
    }*/

    /*private void remapExtensions(DomainResource resource, Map<String, String> idMappings) throws Exception {

        if (!resource.hasExtension()) {
            return;
        }

        for (Extension extension: resource.getExtension()) {
            if (extension.hasValue()
                    && extension.getValue() instanceof Reference) {
                remapReference((Reference)extension.getValue(), idMappings);
            }
        }
    }*/

    /*private void remapContainedResources(DomainResource resource, Map<String, String> idMappings) throws Exception {

        if (!resource.hasContained()) {
            return;
        }

        for (Resource contained: resource.getContained()) {
            //pass in false so we don't map the ID of the contained resource, since it's not supposed to be a global ID
            IdHelper.remapIds(contained, idMappings);
        }
    }*/

    /**
     * maps the IDs in any identifiers of a resource
     */
    /*protected void mapIdentifiers(List<Identifier> identifiers, UUID serviceId, UUID systemId) throws Exception {
        for (Identifier identifier: identifiers) {
            if (identifier.hasAssigner()) {
                mapReference(identifier.getAssigner(), serviceId, systemId);
            }
        }
    }*/

    /*protected void remapIdentifiers(List<Identifier> identifiers, Map<String, String> idMappings) throws Exception {
        for (Identifier identifier: identifiers) {
            if (identifier.hasAssigner()) {
                remapReference(identifier.getAssigner(), idMappings);
            }
        }
    }*/


    /**
     * maps the ID within any reference
     */
    /*protected void mapReference(Reference reference, UUID serviceId, UUID systemId) throws Exception {
        if (reference == null) {
            return;
        }

        if (reference.hasReference()) {

            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);

            //if the reference is to an internal contained resource, the above will return null
            if (comps != null) {
                String newId = IdHelper.getOrCreateEdsResourceIdString(serviceId, systemId, comps.getResourceType(), comps.getId());
                reference.setReference(ReferenceHelper.createResourceReference(comps.getResourceType(), newId));
            }

        } else {

            //if the reference doesn't have an actual reference, it will have an inline resource
            Resource referredResource = (Resource)reference.getResource();
            IdHelper.mapIds(serviceId, systemId, referredResource);
        }
    }*/

    /**
     * maps the ID within any reference
     */
    /*protected void mapReferences(List<Reference> references, UUID serviceId, UUID systemId) throws Exception {
        if (references == null
                || references.isEmpty()) {
            return;
        }

        for (Reference reference: references) {
            mapReference(reference, serviceId, systemId);
        }
    }*/


    /*protected void remapReference(Reference reference, Map<String, String> idMappings) throws Exception {
        if (reference == null) {
            return;
        }

        if (reference.hasReference()) {

            String referenceValue = reference.getReference();
            String newReferenceValue = idMappings.get(referenceValue);
            if (!Strings.isNullOrEmpty(newReferenceValue)) {
                reference.setReference(newReferenceValue);
            }

        } else {

            //if the reference doesn't have an actual reference, it will have an inline resource
            Resource referredResource = (Resource)reference.getResource();
            IdHelper.remapIds(referredResource, idMappings);
        }
    }

    protected void remapReferences(List<Reference> references, Map<String, String> idMappings) throws Exception {
        if (references == null
                || references.isEmpty()) {
            return;
        }

        for (Reference reference: references) {
            remapReference(reference, idMappings);
        }
    }*/
}
