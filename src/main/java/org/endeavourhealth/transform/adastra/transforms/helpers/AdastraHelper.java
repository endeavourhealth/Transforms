package org.endeavourhealth.transform.adastra.transforms.helpers;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.hl7.fhir.instance.model.*;

import java.util.HashMap;

public class AdastraHelper {
    private static final String ID_DELIMITER = ":";

    public static HashMap<String, String> guidMapper = new HashMap<String, String>();

    public static void setUniqueId(Resource resource, String sourceGuid) {
        resource.setId(createUniqueId(guidMapper.get("patient"), sourceGuid));
    }

    public static void setUniquePatientId(Resource resource, String nhsNumber) {
        resource.setId(createUniqueId(nhsNumber, null));
    }

    public static void setUniqueLocationId(Resource resource, String orgId, String locationName) {
        resource.setId(createUniqueId(orgId, locationName));
    }

    public static void setUniqueUserId(Resource resource, String orgId, String userName) {
        resource.setId(createUniqueId(orgId, userName));
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

    public static CodeableConcept createClinicalCode(String codeTerm) {

        Coding coding = new Coding();
        coding.setSystem(FhirUri.CODE_SYSTEM_CTV3);
        coding.setDisplay(codeTerm);

        CodeableConcept codeableConcept = new CodeableConcept();
        codeableConcept.setText(codeTerm);
        codeableConcept.addCoding(coding);
        return codeableConcept;
    }

    //Administrative references
    public static Reference createOrganisationReference(String nationalCode) {
        return ReferenceHelper.createReference(ResourceType.Organization, guidMapper.get(nationalCode));
    }

    public static Reference createLocationReference(String locationName) {
        return ReferenceHelper.createReference(ResourceType.Location, guidMapper.get(locationName));
    }

    public static Reference createPatientReference() {
        return ReferenceHelper.createReference(ResourceType.Patient, guidMapper.get("patient"));
    }

    public static Reference createEpisodeReference() {
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, guidMapper.get("episode"));
    }

    public static Reference createAppointmentReference() {
        return ReferenceHelper.createReference(ResourceType.Appointment, guidMapper.get("episode"));
    }

    public static Reference createUserReference(String name) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, guidMapper.get(name));
    }

    //Clinical References
    public static Reference createEncounterReference(String encounterId) {
        return ReferenceHelper.createReference(ResourceType.Encounter, guidMapper.get("latestAppointment"));
    }


    public static CodeableConcept createCodableConcept(CodedItem codedItem) {
        Coding coding = new Coding();
        coding.setSystem(FhirUri.CODE_SYSTEM_CTV3);
        coding.setCode(codedItem.getCode());
        coding.setDisplay(codedItem.getDescription());

        CodeableConcept codeableConcept = new CodeableConcept();
        codeableConcept.setText(codedItem.getDescription());
        codeableConcept.addCoding(coding);

        return codeableConcept;
    }




}
