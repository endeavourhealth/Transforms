package org.endeavourhealth.transform.common.resourceValidators;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.endeavourhealth.transform.common.idmappers.BaseIdMapper;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.*;

public abstract class ResourceValidatorBase {

    protected abstract void validateResourceFields(Resource resource, List<String> validationErrors);

    private static ResourceIdTransformDalI repository = DalProvider.factoryResourceIdTransformDal();

    public void validateResourceSave(Resource resource, UUID serviceId, boolean mapIds, List<String> validationErrors) throws Exception {

        //ensure we have a good ID and patient reference
        validateIdAndReferences(resource, serviceId, mapIds, validationErrors);
        validatePatientReferenceIsSet(resource, validationErrors);

        //then call into the class-specific validation function
        validateResourceFields(resource, validationErrors);
    }

    public void validateResourceDelete(Resource resource, UUID serviceId, boolean mapIds, List<String> validationErrors) throws Exception {

        //to delete a resource, all we need is to ensure we have a good ID and patient reference
        validateIdAndReferences(resource, serviceId, mapIds, validationErrors);
        validatePatientReferenceIsSet(resource, validationErrors);
    }

    private void validatePatientReferenceIsSet(Resource resource, List<String> validationErrors) throws Exception {

        try {
            String patientId = IdHelper.getPatientId(resource);
            if (Strings.isNullOrEmpty(patientId)) {
                validationErrors.add("No patient reference is set");
            }

        } catch (PatientResourceException notPatientException) {
            //if the resource type doesn't support a patient reference, we'll get this exception
        }
    }


    private void validateIdAndReferences(Resource resource, UUID serviceId, boolean mapIds, List<String> validationErrors) throws Exception {
        if (!resource.hasId()) {
            validationErrors.add("Resource has no ID");

        } else {

            //check that the ID and all references are consistent with whether we're mapping IDs or not
            //i.e. if we're mapping IDs, then the ID and all references shouldn't be post-mapped UUIDs
            String id = resource.getId();
            boolean isIdMapped = isMappedUuid(resource.getResourceType(), id, serviceId);
            if (isIdMapped == mapIds) {
                validationErrors.add("Resource ID mapped state doesn't match mapIds intention (" + mapIds + ")");
            }

            //to get all the references in the resource, just use the ID mapper for that resource type
            BaseIdMapper idMapper = IdHelper.getIdMapper(resource);
            Set<String> referenceValues = new HashSet<>();
            idMapper.getResourceReferences(resource, referenceValues);

            for (String referenceValue: referenceValues) {
                Reference reference = ReferenceHelper.createReference(referenceValue);
                ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
                String referenceId = comps.getId();
                ResourceType referenceType = comps.getResourceType();

                boolean isReferenceIdMapped = isMappedUuid(referenceType, referenceId, serviceId);
                if (isReferenceIdMapped == mapIds) {
                    validationErrors.add("Reference " + referenceType + " " + referenceId + " mapped state doesn't match mapIds intention (" + mapIds + ")");
                }
            }
        }
    }

    private static boolean isMappedUuid(ResourceType resourceType, String id, UUID serviceId) throws Exception {
        try {
            //first test it's a UUID
            UUID.fromString(id);
        } catch (Exception ex) {
            return false;
        }

        //if it is a UUID, check if it's been mapped by making sure there's an entry in the resource mapping
        //table with this UUID as the Discovery UUID. This helps rule out publishers that use UUIDs as their internal IDs
        Reference reference = ReferenceHelper.createReference(resourceType, id);
        List<Reference> list = new ArrayList<>();
        list.add(reference);
        Map<Reference, Reference> map = repository.findSourceReferencesFromEdsReferences(serviceId, list);
        Reference sourceReference = map.get(reference);
        if (sourceReference == null) {
            return false;
        }

        return true;
    }
}
