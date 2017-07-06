package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.MedicationOrder;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.UUID;

public class IdMapperMedicationOrder extends BaseIdMapper {

    @Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        MedicationOrder medicationOrder = (MedicationOrder)resource;

        if (medicationOrder.hasIdentifier()) {
            super.mapIdentifiers(medicationOrder.getIdentifier(), serviceId, systemId);
        }
        if (medicationOrder.hasPatient()) {
            super.mapReference(medicationOrder.getPatient(), serviceId, systemId);
        }
        if (medicationOrder.hasPrescriber()) {
            super.mapReference(medicationOrder.getPrescriber(), serviceId, systemId);
        }
        if (medicationOrder.hasEncounter()) {
            super.mapReference(medicationOrder.getEncounter(), serviceId, systemId);
        }
        if (medicationOrder.hasReason()) {
            try {
                super.mapReference(medicationOrder.getReasonReference(), serviceId, systemId);
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationOrder.hasMedication()) {
            try {
                super.mapReference(medicationOrder.getMedicationReference(), serviceId, systemId);
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationOrder.hasDosageInstruction()) {
            for (MedicationOrder.MedicationOrderDosageInstructionComponent dosage: medicationOrder.getDosageInstruction()) {
                if (dosage.hasSite()) {
                    try {
                        super.mapReference(dosage.getSiteReference(), serviceId, systemId);
                    } catch (Exception ex) {
                        //do nothing if not a reference
                    }
                }
            }
        }
        if (medicationOrder.hasDispenseRequest()) {
            try {
                super.mapReference(medicationOrder.getDispenseRequest().getMedicationReference(), serviceId, systemId);
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationOrder.hasPriorPrescription()) {
            super.mapReference(medicationOrder.getPriorPrescription(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(medicationOrder, serviceId, systemId, mapResourceId);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        MedicationOrder medicationOrder = (MedicationOrder)resource;
        if (medicationOrder.hasPatient()) {
            return ReferenceHelper.getReferenceId(medicationOrder.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }
}
