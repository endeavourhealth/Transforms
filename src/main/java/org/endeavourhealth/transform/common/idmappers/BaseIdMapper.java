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
import java.util.UUID;

public abstract class BaseIdMapper {
    private static final Logger LOG = LoggerFactory.getLogger(BaseIdMapper.class);


    public abstract boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception;

    public abstract String getPatientId(Resource resource) throws PatientResourceException;

    public abstract void remapIds(Resource resource, Map<String, String> idMappings) throws Exception;

    /**
     * maps the ID, extensions and contained resources in a FHIR resource
     * returns true to indicate the resource is new to EDS, false if not new or we didn't map it's ID
     */
    protected boolean mapCommonResourceFields(DomainResource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        boolean isNewResource = false;
        if (mapResourceId) {
            isNewResource = mapResourceId(resource, serviceId, systemId);
        }
        mapExtensions(resource, serviceId, systemId);
        mapContainedResources(resource, serviceId, systemId);

        return isNewResource;
    }

    /**
     * maps the main ID of any resource
     * returns true if the resource is new to EDS
     */
    private boolean mapResourceId(Resource resource, UUID serviceId, UUID systemId) {

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
    }

    /**
     * maps the IDs in any extensions of a resource
     */
    private void mapExtensions(DomainResource resource, UUID serviceId, UUID systemId) throws Exception {

        if (!resource.hasExtension()) {
            return;
        }

        for (Extension extension: resource.getExtension()) {
            if (extension.hasValue()
                && extension.getValue() instanceof Reference) {
                mapReference((Reference)extension.getValue(), resource, serviceId, systemId);
            }
        }
    }

    /**
     * maps the IDs in any extensions of a resource
     */
    private void mapContainedResources(DomainResource resource, UUID serviceId, UUID systemId) throws Exception {

        if (!resource.hasContained()) {
            return;
        }

        for (Resource contained: resource.getContained()) {
            //pass in false so we don't map the ID of the contained resource, since it's not supposed to be a global ID
            IdHelper.mapIds(serviceId, systemId, contained, false);
        }
    }

    protected void remapCommonResourceFields(DomainResource resource, Map<String, String> idMappings) throws Exception {
        remapResourceId(resource, idMappings);
        remapExtensions(resource, idMappings);
        remapContainedResources(resource, idMappings);
    }

    private void remapResourceId(Resource resource, Map<String, String> idMappings) {

        if (!resource.hasId()) {
            return;
        }

        String referenceValue = ReferenceHelper.createResourceReference(resource.getResourceType(), resource.getId());
        String newReferenceValue = idMappings.get(referenceValue);
        if (Strings.isNullOrEmpty(newReferenceValue)) {
            return;
        }

        Reference newReference = new Reference().setReference(newReferenceValue);
        String newId = ReferenceHelper.getReferenceId(newReference);

        resource.setId(newId);
    }

    private void remapExtensions(DomainResource resource, Map<String, String> idMappings) throws Exception {

        if (!resource.hasExtension()) {
            return;
        }

        for (Extension extension: resource.getExtension()) {
            if (extension.hasValue()
                    && extension.getValue() instanceof Reference) {
                remapReference((Reference)extension.getValue(), idMappings);
            }
        }
    }

    private void remapContainedResources(DomainResource resource, Map<String, String> idMappings) throws Exception {

        if (!resource.hasContained()) {
            return;
        }

        for (Resource contained: resource.getContained()) {
            //pass in false so we don't map the ID of the contained resource, since it's not supposed to be a global ID
            IdHelper.remapIds(contained, idMappings);
        }
    }

    /**
     * maps the IDs in any identifiers of a resource
     */
    protected void mapIdentifiers(List<Identifier> identifiers, Resource resource, UUID serviceId, UUID systemId) throws Exception {
        for (Identifier identifier: identifiers) {
            if (identifier.hasAssigner()) {
                mapReference(identifier.getAssigner(), resource, serviceId, systemId);
            }
        }
    }

    protected void remapIdentifiers(List<Identifier> identifiers, Map<String, String> idMappings) throws Exception {
        for (Identifier identifier: identifiers) {
            if (identifier.hasAssigner()) {
                remapReference(identifier.getAssigner(), idMappings);
            }
        }
    }


    /**
     * maps the ID within any reference
     */
    protected void mapReference(Reference reference, Resource resource, UUID serviceId, UUID systemId) throws Exception {
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
    }

    /**
     * maps the ID within any reference
     */
    protected void mapReferences(List<Reference> references, Resource resource, UUID serviceId, UUID systemId) throws Exception {
        if (references == null
                || references.isEmpty()) {
            return;
        }

        for (Reference reference: references) {
            mapReference(reference, resource, serviceId, systemId);
        }
    }


    protected void remapReference(Reference reference, Map<String, String> idMappings) throws Exception {
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
    }
}
