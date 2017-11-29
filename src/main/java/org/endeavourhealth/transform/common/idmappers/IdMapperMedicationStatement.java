package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperMedicationStatement extends BaseIdMapper {

    

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        MedicationStatement medicationStatement = (MedicationStatement)resource;
        super.addCommonResourceReferences(medicationStatement, referenceValues);

        if (medicationStatement.hasIdentifier()) {
            super.addIndentifierReferences(medicationStatement.getIdentifier(), referenceValues);
        }
        if (medicationStatement.hasPatient()) {
            super.addReference(medicationStatement.getPatient(), referenceValues);
        }
        if (medicationStatement.hasInformationSource()) {
            super.addReference(medicationStatement.getInformationSource(), referenceValues);
        }
        if (medicationStatement.hasReasonForUse()) {
            try {
                if (medicationStatement.hasReasonForUseReference()) {
                    super.addReference(medicationStatement.getReasonForUseReference(), referenceValues);
                }
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationStatement.hasMedication()) {
            try {
                if (medicationStatement.hasMedicationReference()) {
                    super.addReference(medicationStatement.getMedicationReference(), referenceValues);
                }
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationStatement.hasDosage()) {
            for (MedicationStatement.MedicationStatementDosageComponent dosage: medicationStatement.getDosage()) {
                try {
                    if (dosage.hasSiteReference()) {
                        super.addReference(dosage.getSiteReference(), referenceValues);
                    }
                } catch (Exception ex) {
                    //do nothing if not a reference
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        MedicationStatement medicationStatement = (MedicationStatement)resource;
        super.mapCommonResourceFields(medicationStatement, mappings, failForMissingMappings);

        if (medicationStatement.hasIdentifier()) {
            super.mapIdentifiers(medicationStatement.getIdentifier(), mappings, failForMissingMappings);
        }
        if (medicationStatement.hasPatient()) {
            super.mapReference(medicationStatement.getPatient(), mappings, failForMissingMappings);
        }
        if (medicationStatement.hasInformationSource()) {
            super.mapReference(medicationStatement.getInformationSource(), mappings, failForMissingMappings);
        }
        if (medicationStatement.hasReasonForUse()) {
            try {
                if (medicationStatement.hasReasonForUseReference()) {
                    super.mapReference(medicationStatement.getReasonForUseReference(), mappings, failForMissingMappings);
                }
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationStatement.hasMedication()) {
            try {
                if (medicationStatement.hasMedicationReference()) {
                    super.mapReference(medicationStatement.getMedicationReference(), mappings, failForMissingMappings);
                }
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationStatement.hasDosage()) {
            for (MedicationStatement.MedicationStatementDosageComponent dosage: medicationStatement.getDosage()) {
                try {
                    if (dosage.hasSiteReference()) {
                        super.mapReference(dosage.getSiteReference(), mappings, failForMissingMappings);
                    }
                } catch (Exception ex) {
                    //do nothing if not a reference
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        MedicationStatement medicationStatement = (MedicationStatement)resource;
        if (medicationStatement.hasPatient()) {
            return ReferenceHelper.getReferenceId(medicationStatement.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        MedicationStatement medicationStatement = (MedicationStatement)resource;

        if (medicationStatement.hasIdentifier()) {
            super.mapIdentifiers(medicationStatement.getIdentifier(), serviceId, systemId);
        }
        if (medicationStatement.hasPatient()) {
            super.mapReference(medicationStatement.getPatient(), serviceId, systemId);
        }
        if (medicationStatement.hasInformationSource()) {
            super.mapReference(medicationStatement.getInformationSource(), serviceId, systemId);
        }
        if (medicationStatement.hasReasonForUse()) {
            try {
                if (medicationStatement.hasReasonForUseReference()) {
                    super.mapReference(medicationStatement.getReasonForUseReference(), serviceId, systemId);
                }
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationStatement.hasMedication()) {
            try {
                if (medicationStatement.hasMedicationReference()) {
                    super.mapReference(medicationStatement.getMedicationReference(), serviceId, systemId);
                }
            } catch (Exception ex) {
                //do nothing if not a reference
            }
        }
        if (medicationStatement.hasDosage()) {
            for (MedicationStatement.MedicationStatementDosageComponent dosage: medicationStatement.getDosage()) {
                try {
                    if (dosage.hasSiteReference()) {
                        super.mapReference(dosage.getSiteReference(), serviceId, systemId);
                    }
                } catch (Exception ex) {
                    //do nothing if not a reference
                }
            }
        }

        return super.mapCommonResourceFields(medicationStatement, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
