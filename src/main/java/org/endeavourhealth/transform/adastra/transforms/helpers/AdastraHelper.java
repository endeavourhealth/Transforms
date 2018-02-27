package org.endeavourhealth.transform.adastra.transforms.helpers;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.hl7.fhir.instance.model.*;

import javax.xml.datatype.XMLGregorianCalendar;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class AdastraHelper {
    private static final String ID_DELIMITER = ":";

    public static HashMap<String, String> uniqueIdMapper = new HashMap<String, String>();
    public static List<String> consultationIds = new ArrayList<>();
    public static List<String> observationIds = new ArrayList<>();

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
        coding.setSystem(FhirCodeUri.CODE_SYSTEM_CTV3);
        coding.setDisplay(codeTerm);

        CodeableConcept codeableConcept = new CodeableConcept();
        codeableConcept.setText(codeTerm);
        codeableConcept.addCoding(coding);
        return codeableConcept;
    }

    //Administrative references
    public static Reference createOrganisationReference(String nationalCode) {
        return ReferenceHelper.createReference(ResourceType.Organization, uniqueIdMapper.get(nationalCode));
    }

    public static Reference createLocationReference(String locationName) {
        return ReferenceHelper.createReference(ResourceType.Location, uniqueIdMapper.get(locationName));
    }

    public static Reference createPatientReference() {
        return ReferenceHelper.createReference(ResourceType.Patient, uniqueIdMapper.get("patient"));
    }

    public static Reference createEpisodeReference() {
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, uniqueIdMapper.get("episode"));
    }

    public static Reference createAppointmentReference() {
        return ReferenceHelper.createReference(ResourceType.Appointment, uniqueIdMapper.get("episode"));
    }

    public static Reference createUserReference(String name) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, uniqueIdMapper.get(name));
    }

    //Clinical References
    public static Reference createEncounterReference(String encounterId) {
        return ReferenceHelper.createReference(ResourceType.Encounter, encounterId);
    }


    public static CodeableConcept createCodableConcept(CodedItem codedItem) {
        Coding coding = new Coding();
        coding.setSystem(FhirCodeUri.CODE_SYSTEM_CTV3);
        coding.setCode(codedItem.getCode());
        coding.setDisplay(codedItem.getDescription());

        CodeableConcept codeableConcept = new CodeableConcept();
        codeableConcept.setText(codedItem.getDescription());
        codeableConcept.addCoding(coding);

        return codeableConcept;
    }

    public static DateTimeType getDateTimeType(XMLGregorianCalendar date) {

        Date dateString = XmlDateHelper.convertDate(date);
        return new DateTimeType(dateString, TemporalPrecisionEnum.SECOND);
    }




}
