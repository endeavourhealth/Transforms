package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.Substance;

import java.util.Map;
import java.util.Set;

public class IdMapperSubstance extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Substance substance = (Substance)resource;
        super.addCommonResourceReferences(substance, referenceValues);

        if (substance.hasIdentifier()) {
            super.addIndentifierReferences(substance.getIdentifier(), referenceValues);
        }
        if (substance.hasInstance()) {
            for (Substance.SubstanceInstanceComponent instance: substance.getInstance()) {
                if (instance.hasIdentifier()) {
                    if (instance.getIdentifier().hasAssigner()) {
                        super.addReference(instance.getIdentifier().getAssigner(), referenceValues);
                    }
                }
            }
        }
        if (substance.hasIngredient()) {
            for (Substance.SubstanceIngredientComponent ingredient: substance.getIngredient()) {
                if (ingredient.hasSubstance()) {
                    super.addReference(ingredient.getSubstance(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        Substance substance = (Substance)resource;
        super.mapCommonResourceFields(substance, mappings);

        if (substance.hasIdentifier()) {
            super.mapIdentifiers(substance.getIdentifier(), mappings);
        }
        if (substance.hasInstance()) {
            for (Substance.SubstanceInstanceComponent instance: substance.getInstance()) {
                if (instance.hasIdentifier()) {
                    if (instance.getIdentifier().hasAssigner()) {
                        super.mapReference(instance.getIdentifier().getAssigner(), mappings);
                    }
                }
            }
        }
        if (substance.hasIngredient()) {
            for (Substance.SubstanceIngredientComponent ingredient: substance.getIngredient()) {
                if (ingredient.hasSubstance()) {
                    super.mapReference(ingredient.getSubstance(), mappings);
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    /*@Override
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
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
