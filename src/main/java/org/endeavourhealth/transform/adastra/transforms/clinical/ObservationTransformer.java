package org.endeavourhealth.transform.adastra.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.emisopen.transforms.common.DateConverter;
import org.hl7.fhir.instance.model.*;

import javax.xml.datatype.XMLGregorianCalendar;
import java.util.List;
import java.util.UUID;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.consultationIds;
import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.observationIds;

public class ObservationTransformer {

    public static void observationFromPresentingCondition(AdastraCaseDataExport caseReport, List<Resource> resources)  throws Exception {

        AdastraCaseDataExport.PresentingCondition presentingCondition = caseReport.getPresentingCondition();

        Observation fhirObservation = createStandardObservation();

        fhirObservation.setId(caseReport.getAdastraCaseReference() + ":" + presentingCondition.getSymptoms());
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.setComments(presentingCondition.getComments());

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference("caseEncounter"));

        fhirObservation.setCode(AdastraHelper.createClinicalCode(presentingCondition.getSymptoms()));

        fhirObservation.setStatus(Observation.ObservationStatus.PRELIMINARY);

        fhirObservation.setEffective(AdastraHelper.getDateTimeType(caseReport.getActiveDate()));
    }

    public static void observationFromFreeText(String freeText, String consultationID, XMLGregorianCalendar consultationDate, String caseRef, List<Resource> resources)  throws Exception {

        Observation fhirObservation = createStandardObservation();

        fhirObservation.setId(caseRef + ":" + freeText);
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.setComments(freeText);

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference(consultationID));

        fhirObservation.setStatus(Observation.ObservationStatus.FINAL);

        //fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(XmlDateHelper.convertDate(consultationDate), "YMDT"));

        fhirObservation.setEffective(AdastraHelper.getDateTimeType(consultationDate));

    }

    public static void observationFromCodedItem(CodedItem codedItem, String consultationID, XMLGregorianCalendar consultationDate, String caseRef, List<Resource> resources)  throws Exception {

        Observation fhirObservation = createStandardObservation();

        fhirObservation.setId(caseRef + ":" + codedItem.getCode());
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.setComments(codedItem.getDescription());
        fhirObservation.setCode(AdastraHelper.createCodableConcept(codedItem));

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference(consultationID));

        fhirObservation.setStatus(Observation.ObservationStatus.FINAL);

        fhirObservation.setEffective(AdastraHelper.getDateTimeType(consultationDate));

        resources.add(fhirObservation);
    }

    private static Observation createStandardObservation() {
        Observation fhirObservation = new Observation();
        fhirObservation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_OBSERVATION));

        fhirObservation.setSubject(AdastraHelper.createPatientReference());

        return fhirObservation;
    }

    private static void checkForDuplicateObservations(String observationId) throws Exception {
        if (observationIds.contains(observationId)) {
            throw new TransformException("Duplicate observation Id found : " + observationId);
        } else {
            observationIds.add(observationId);
        }
    }
}
