package org.endeavourhealth.transform.adastra;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.UUID;

public class AdastraCsvHelper {
    private static final String ID_DELIMITER = ":";

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;

    public AdastraCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
    }

    /**
     * to ensure globally unique IDs for all resources, a new ID is created
     * from the patientGuid and sourceGuid (e.g. observationGuid)
     */
    public static String createUniqueId(String patientGuid, String sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid;
        } else {
            return patientGuid + ID_DELIMITER + sourceGuid;
        }
    }

    public Reference createEncounterReference(String encounterGuid, String patientGuid) {
        return ReferenceHelper.createReference(ResourceType.Encounter, createUniqueId(patientGuid, encounterGuid));
    }

    public Reference createPractitionerReference(String practitionerGuid) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }

    public Reference createPatientReference(CsvCell patientGuid) {
        return ReferenceHelper.createReference(ResourceType.Patient, patientGuid.getString());
    }
}
