package org.endeavourhealth.transform.adastra.xml.transforms.clinical;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.adastra.AdastraXmlHelper;
import org.endeavourhealth.transform.adastra.xml.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.xml.schema.CodedItem;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Observation;

import javax.xml.datatype.XMLGregorianCalendar;

import static org.endeavourhealth.transform.adastra.AdastraXmlHelper.observationIds;

public class ObservationTransformer {

    public static void observationFromPresentingCondition(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler)  throws Exception {

        AdastraCaseDataExport.PresentingCondition presentingCondition = caseReport.getPresentingCondition();

        Observation fhirObservation = createStandardObservation();

        fhirObservation.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, "Presenting Condition"));

        fhirObservation.setId(caseReport.getAdastraCaseReference() + ":" + presentingCondition.getSymptoms());
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.setComments(presentingCondition.getComments());

        fhirObservation.setEncounter(AdastraXmlHelper.createEncounterReference("caseEncounter"));

        fhirObservation.setCode(AdastraXmlHelper.createClinicalCode(presentingCondition.getSymptoms()));

        fhirObservation.setStatus(Observation.ObservationStatus.PRELIMINARY);

        fhirObservation.setEffective(AdastraXmlHelper.getDateTimeType(caseReport.getActiveDate()));

        fhirResourceFiler.savePatientResource(null, new ObservationBuilder(fhirObservation));
    }

    public static void observationFromFreeText(String freeText, String consultationID,
                                               XMLGregorianCalendar consultationDate, String caseRef,
                                               String context, FhirResourceFiler fhirResourceFiler)  throws Exception {

        Observation fhirObservation = createStandardObservation();

        fhirObservation.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, context));

        fhirObservation.setId(caseRef + ":" + freeText);
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.setComments(freeText);

        fhirObservation.setEncounter(AdastraXmlHelper.createEncounterReference(consultationID));

        fhirObservation.setStatus(Observation.ObservationStatus.FINAL);

        fhirObservation.setEffective(AdastraXmlHelper.getDateTimeType(consultationDate));

        fhirResourceFiler.savePatientResource(null, new ObservationBuilder(fhirObservation));

    }

    public static void observationFromCodedItem(CodedItem codedItem, String consultationID,
                                                XMLGregorianCalendar consultationDate, String caseRef,
                                                String context, FhirResourceFiler fhirResourceFiler)  throws Exception {

        Observation fhirObservation = createStandardObservation();

        fhirObservation.setId(caseRef + ":" + codedItem.getCode());
        checkForDuplicateObservations(fhirObservation.getId());

        fhirObservation.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, context));

        fhirObservation.setComments(codedItem.getDescription());
        fhirObservation.setCode(AdastraXmlHelper.createCodableConcept(codedItem));

        fhirObservation.setEncounter(AdastraXmlHelper.createEncounterReference(consultationID));

        fhirObservation.setStatus(Observation.ObservationStatus.FINAL);

        fhirObservation.setEffective(AdastraXmlHelper.getDateTimeType(consultationDate));

        fhirResourceFiler.savePatientResource(null, new ObservationBuilder(fhirObservation));
    }

    private static Observation createStandardObservation() {
        Observation fhirObservation = new Observation();
        fhirObservation.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_OBSERVATION));

        fhirObservation.setSubject(AdastraXmlHelper.createPatientReference());

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
