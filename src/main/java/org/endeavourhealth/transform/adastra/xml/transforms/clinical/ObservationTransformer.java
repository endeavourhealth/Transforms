package org.endeavourhealth.transform.adastra.xml.transforms.clinical;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.adastra.xml.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.xml.schema.CodedItem;
import org.endeavourhealth.transform.adastra.AdastraHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Observation;

import javax.xml.datatype.XMLGregorianCalendar;

import static org.endeavourhealth.transform.adastra.AdastraHelper.observationIds;

public class ObservationTransformer {

    public static void observationFromPresentingCondition(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler)  throws Exception {

        AdastraCaseDataExport.PresentingCondition presentingCondition = caseReport.getPresentingCondition();

        Observation fhirObservation = createStandardObservation();

        fhirObservation.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, "Presenting Condition"));

        fhirObservation.setId(caseReport.getAdastraCaseReference() + ":" + presentingCondition.getSymptoms());
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.setComments(presentingCondition.getComments());

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference("caseEncounter"));

        fhirObservation.setCode(AdastraHelper.createClinicalCode(presentingCondition.getSymptoms()));

        fhirObservation.setStatus(Observation.ObservationStatus.PRELIMINARY);

        fhirObservation.setEffective(AdastraHelper.getDateTimeType(caseReport.getActiveDate()));

        fhirResourceFiler.savePatientResource(null, fhirObservation);
    }

    public static void observationFromFreeText(String freeText, String consultationID,
                                               XMLGregorianCalendar consultationDate, String caseRef,
                                               String context, FhirResourceFiler fhirResourceFiler)  throws Exception {

        Observation fhirObservation = createStandardObservation();

        fhirObservation.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, context));

        fhirObservation.setId(caseRef + ":" + freeText);
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.setComments(freeText);

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference(consultationID));

        fhirObservation.setStatus(Observation.ObservationStatus.FINAL);

        fhirObservation.setEffective(AdastraHelper.getDateTimeType(consultationDate));

        fhirResourceFiler.savePatientResource(null, fhirObservation);

    }

    public static void observationFromCodedItem(CodedItem codedItem, String consultationID,
                                                XMLGregorianCalendar consultationDate, String caseRef,
                                                String context, FhirResourceFiler fhirResourceFiler)  throws Exception {

        Observation fhirObservation = createStandardObservation();

        fhirObservation.setId(caseRef + ":" + codedItem.getCode());
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, context));

        fhirObservation.setComments(codedItem.getDescription());
        fhirObservation.setCode(AdastraHelper.createCodableConcept(codedItem));

        fhirObservation.setEncounter(AdastraHelper.createEncounterReference(consultationID));

        fhirObservation.setStatus(Observation.ObservationStatus.FINAL);

        fhirObservation.setEffective(AdastraHelper.getDateTimeType(consultationDate));

        fhirResourceFiler.savePatientResource(null, fhirObservation);
    }

    private static Observation createStandardObservation() {
        Observation fhirObservation = new Observation();
        fhirObservation.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_OBSERVATION));

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
