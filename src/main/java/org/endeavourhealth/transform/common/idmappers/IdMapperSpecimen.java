package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.hl7.fhir.instance.model.Specimen;

import java.util.Map;
import java.util.Set;

public class IdMapperSpecimen extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Specimen specimen = (Specimen)resource;
        super.addCommonResourceReferences(specimen, referenceValues);

        if (specimen.hasIdentifier()) {
            super.addIndentifierReferences(specimen.getIdentifier(), referenceValues);
        }
        if (specimen.hasParent()) {
            super.addReference(specimen.getSubject(), referenceValues);
        }
        if (specimen.hasSubject()) {
            super.addReference(specimen.getSubject(), referenceValues);
        }
        if (specimen.hasCollection()) {
            if (specimen.getCollection().hasCollector()) {
                super.addReference(specimen.getCollection().getCollector(), referenceValues);
            }
        }
        if (specimen.hasTreatment()) {
            for (Specimen.SpecimenTreatmentComponent treatment: specimen.getTreatment()) {
                if (treatment.hasAdditive()) {
                    super.addReferences(treatment.getAdditive(), referenceValues);
                }
            }
        }
        if (specimen.hasContainer()) {
            for (Specimen.SpecimenContainerComponent container: specimen.getContainer()) {
                if (container.hasAdditive()) {
                    try {
                        super.addReference(container.getAdditiveReference(), referenceValues);
                    } catch (Exception ex) {
                        //do nothing if not a reference
                    }
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        Specimen specimen = (Specimen)resource;
        super.mapCommonResourceFields(specimen, mappings);

        if (specimen.hasIdentifier()) {
            super.mapIdentifiers(specimen.getIdentifier(), mappings);
        }
        if (specimen.hasParent()) {
            super.mapReference(specimen.getSubject(), mappings);
        }
        if (specimen.hasSubject()) {
            super.mapReference(specimen.getSubject(), mappings);
        }
        if (specimen.hasCollection()) {
            if (specimen.getCollection().hasCollector()) {
                super.mapReference(specimen.getCollection().getCollector(), mappings);
            }
        }
        if (specimen.hasTreatment()) {
            for (Specimen.SpecimenTreatmentComponent treatment: specimen.getTreatment()) {
                if (treatment.hasAdditive()) {
                    super.mapReferences(treatment.getAdditive(), mappings);
                }
            }
        }
        if (specimen.hasContainer()) {
            for (Specimen.SpecimenContainerComponent container: specimen.getContainer()) {
                if (container.hasAdditive()) {
                    try {
                        super.mapReference(container.getAdditiveReference(), mappings);
                    } catch (Exception ex) {
                        //do nothing if not a reference
                    }
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Specimen specimen = (Specimen)resource;
        if (specimen.hasSubject()) {
            return ReferenceHelper.getReferenceId(specimen.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Specimen specimen = (Specimen)resource;

        if (specimen.hasIdentifier()) {
            super.mapIdentifiers(specimen.getIdentifier(), serviceId, systemId);
        }
        if (specimen.hasParent()) {
            super.mapReference(specimen.getSubject(), serviceId, systemId);
        }
        if (specimen.hasSubject()) {
            super.mapReference(specimen.getSubject(), serviceId, systemId);
        }
        if (specimen.hasCollection()) {
            if (specimen.getCollection().hasCollector()) {
                super.mapReference(specimen.getCollection().getCollector(), serviceId, systemId);
            }
        }
        if (specimen.hasTreatment()) {
            for (Specimen.SpecimenTreatmentComponent treatment: specimen.getTreatment()) {
                if (treatment.hasAdditive()) {
                    super.mapReferences(treatment.getAdditive(), serviceId, systemId);
                }
            }
        }
        if (specimen.hasContainer()) {
            for (Specimen.SpecimenContainerComponent container: specimen.getContainer()) {
                if (container.hasAdditive()) {
                    try {
                        super.mapReference(container.getAdditiveReference(), serviceId, systemId);
                    } catch (Exception ex) {
                        //do nothing if not a reference
                    }
                }
            }
        }

        return super.mapCommonResourceFields(specimen, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
