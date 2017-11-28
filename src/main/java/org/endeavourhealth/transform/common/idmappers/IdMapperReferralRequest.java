package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.hl7.fhir.instance.model.ReferralRequest;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperReferralRequest extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        ReferralRequest referralRequest = (ReferralRequest)resource;
        super.addCommonResourceReferences(referralRequest, referenceValues);

        if (referralRequest.hasIdentifier()) {
            super.addIndentifierReferences(referralRequest.getIdentifier(), referenceValues);
        }
        if (referralRequest.hasPatient()) {
            super.addReference(referralRequest.getPatient(), referenceValues);
        }
        if (referralRequest.hasRequester()) {
            super.addReference(referralRequest.getRequester(), referenceValues);
        }
        if (referralRequest.hasRecipient()) {
            super.addReferences(referralRequest.getRecipient(), referenceValues);
        }
        if (referralRequest.hasEncounter()) {
            super.addReference(referralRequest.getEncounter(), referenceValues);
        }
        if (referralRequest.hasSupportingInformation()) {
            super.addReferences(referralRequest.getSupportingInformation(), referenceValues);
        }

    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        ReferralRequest referralRequest = (ReferralRequest)resource;
        super.mapCommonResourceFields(referralRequest, mappings);

        if (referralRequest.hasIdentifier()) {
            super.mapIdentifiers(referralRequest.getIdentifier(), mappings);
        }
        if (referralRequest.hasPatient()) {
            super.mapReference(referralRequest.getPatient(), mappings);
        }
        if (referralRequest.hasRequester()) {
            super.mapReference(referralRequest.getRequester(), mappings);
        }
        if (referralRequest.hasRecipient()) {
            super.mapReferences(referralRequest.getRecipient(), mappings);
        }
        if (referralRequest.hasEncounter()) {
            super.mapReference(referralRequest.getEncounter(), mappings);
        }
        if (referralRequest.hasSupportingInformation()) {
            super.mapReferences(referralRequest.getSupportingInformation(), mappings);
        }

    }

    @Override
    public String getPatientId(Resource resource) {

        ReferralRequest referralRequest = (ReferralRequest)resource;
        if (referralRequest.hasPatient()) {
            return ReferenceHelper.getReferenceId(referralRequest.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        ReferralRequest referralRequest = (ReferralRequest)resource;

        if (referralRequest.hasIdentifier()) {
            super.mapIdentifiers(referralRequest.getIdentifier(), serviceId, systemId);
        }
        if (referralRequest.hasPatient()) {
            super.mapReference(referralRequest.getPatient(), serviceId, systemId);
        }
        if (referralRequest.hasRequester()) {
            super.mapReference(referralRequest.getRequester(), serviceId, systemId);
        }
        if (referralRequest.hasRecipient()) {
            super.mapReferences(referralRequest.getRecipient(), serviceId, systemId);
        }
        if (referralRequest.hasEncounter()) {
            super.mapReference(referralRequest.getEncounter(), serviceId, systemId);
        }
        if (referralRequest.hasSupportingInformation()) {
            super.mapReferences(referralRequest.getSupportingInformation(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(referralRequest, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
