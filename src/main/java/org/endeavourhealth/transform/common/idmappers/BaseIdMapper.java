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
            addExtensionReferences(resource.getExtension(), referenceValues);
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

    private void addExtensionReferences(List<Extension> extensions, Set<String> referenceValues) throws Exception {

        for (Extension extension : extensions) {
            if (extension.hasValue()
                    && extension.getValue() instanceof Reference) {

                Reference reference = (Reference)extension.getValue();
                addReference(reference, referenceValues);
            }

            //need to handle nested extensions within extensions
            if (extension.hasExtension()) {
                addExtensionReferences(extension.getExtension(), referenceValues);
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
        if (resource.hasExtension()) {
            mapExtensions(resource.getExtension(), mappings, failForMissingMappings);
        }

        if (resource.hasContained()) {
            mapContainedResources(resource.getContained(), mappings, failForMissingMappings);
        }
    }

    /**
     * maps the IDs in any extensions of a resource
     */
    private void mapExtensions(List<Extension> extensions, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {

        for (Extension extension: extensions) {

            if (extension.hasValue()
                    && extension.getValue() instanceof Reference) {
                mapReference((Reference)extension.getValue(), mappings, failForMissingMappings);
            }

            if (extension.hasExtension()) {
                mapExtensions(extension.getExtension(), mappings, failForMissingMappings);
            }
        }
    }


    /**
     * maps the IDs in any extensions of a resource
     */
    private void mapContainedResources(List<Resource> containedResources, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        for (Resource contained: containedResources) {
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

}
