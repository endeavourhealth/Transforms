package org.endeavourhealth.transform.adastra.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.endeavourhealth.transform.emis.csv.EmisDateTimeHelper;
import org.hl7.fhir.instance.model.*;

import javax.xml.datatype.XMLGregorianCalendar;
import java.util.List;
import java.util.UUID;

public class ObservationTransformer {

    public static void observationFromPresentingCondition(AdastraCaseDataExport caseReport, List<Resource> resources)  throws Exception {

        AdastraCaseDataExport.PresentingCondition presentingCondition = caseReport.getPresentingCondition();

        Observation fhirObservation = createStandardObservation();

        fhirObservation.setComments(presentingCondition.getComments());

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference(caseReport.getAdastraCaseReference()));

        fhirObservation.setCode(AdastraHelper.createClinicalCode(presentingCondition.getSymptoms()));

        fhirObservation.setStatus(Observation.ObservationStatus.PRELIMINARY);

        fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(XmlDateHelper.convertDate(caseReport.getActiveDate()), "YMDT"));

    }

    public static void observationFromFreeText(String freeText, String consultationID, XMLGregorianCalendar consultationDate, List<Resource> resources)  throws Exception {

        Observation fhirObservation = createStandardObservation();

        fhirObservation.setComments(freeText);

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference(consultationID));

        fhirObservation.setStatus(Observation.ObservationStatus.FINAL);

        fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(XmlDateHelper.convertDate(consultationDate), "YMDT"));

    }

    public static void observationFromCodedItem(CodedItem codedItem, String consultationID, XMLGregorianCalendar consultationDate, List<Resource> resources)  throws Exception {

        Observation fhirObservation = createStandardObservation();

        fhirObservation.setComments(codedItem.getDescription());
        fhirObservation.setCode(AdastraHelper.createCodableConcept(codedItem));

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference(consultationID));

        fhirObservation.setStatus(Observation.ObservationStatus.FINAL);

        fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(XmlDateHelper.convertDate(consultationDate), "YMDT"));

        resources.add(fhirObservation);
    }

    private static Observation createStandardObservation() {
        Observation fhirObservation = new Observation();
        fhirObservation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_OBSERVATION));

        //Need to check if this needs to be consistent
        String observationGuid = UUID.randomUUID().toString();

        AdastraHelper.setUniqueId(fhirObservation, observationGuid);

        fhirObservation.setSubject(AdastraHelper.createPatientReference());

        return fhirObservation;
    }
}
