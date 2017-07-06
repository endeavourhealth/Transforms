package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.Substance;

import java.util.Map;
import java.util.UUID;

public class IdMapperSubstance extends BaseIdMapper {
    @Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Substance substance = (Substance)resource;

        if (substance.hasIdentifier()) {
            super.mapIdentifiers(substance.getIdentifier(), serviceId, systemId);
        }
        if (substance.hasInstance()) {
            for (Substance.SubstanceInstanceComponent instance: substance.getInstance()) {
                if (instance.hasIdentifier()) {
                    if (instance.getIdentifier().hasAssigner()) {
                        super.mapReference(instance.getIdentifier().getAssigner(), serviceId, systemId);
                    }
                }
            }
        }
        if (substance.hasIngredient()) {
            for (Substance.SubstanceIngredientComponent ingredient: substance.getIngredient()) {
                if (ingredient.hasSubstance()) {
                    super.mapReference(ingredient.getSubstance(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(substance, serviceId, systemId, mapResourceId);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }
}
