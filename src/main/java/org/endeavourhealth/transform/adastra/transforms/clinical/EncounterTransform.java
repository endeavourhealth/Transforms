package org.endeavourhealth.transform.adastra.transforms.clinical;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.consultationIds;
import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.uniqueIdMapper;

public class EncounterTransform {

    public static void createMainCaseEncounter(AdastraCaseDataExport caseReport, List<Resource> resources) {

        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ENCOUNTER));

        if (caseReport.getCaseType().getDescription() != null)
            fhirEncounter.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, caseReport.getCaseType().getDescription()));

        if (caseReport.getLatestAppointment() != null)
            fhirEncounter.setAppointment(AdastraHelper.createAppointmentReference());

        fhirEncounter.setId(caseReport.getAdastraCaseReference());
        uniqueIdMapper.put("caseEncounter", fhirEncounter.getId());

        fhirEncounter.setPatient(AdastraHelper.createPatientReference());
        fhirEncounter.addEpisodeOfCare(AdastraHelper.createEpisodeReference());

        Date caseStart = XmlDateHelper.convertDate(caseReport.getActiveDate());
        Period fhirPeriod = PeriodHelper.createPeriod(caseStart, null);
        fhirEncounter.setPeriod(fhirPeriod);

        if (caseReport.getCompletedDate() != null) {
            //if we have a completion date, the care is finish
            fhirEncounter.setStatus(Encounter.EncounterState.FINISHED);

            Date caseEnd = XmlDateHelper.convertDate(caseReport.getCompletedDate());
            fhirPeriod.setEnd(caseEnd);

        } else {
            //if we don't have a completion date, the case is still active. although this should never happen
            fhirEncounter.setStatus(Encounter.EncounterState.ARRIVED);
        }

        resources.add(fhirEncounter);
    }

    public static void createChildEncountersFromConsultations(AdastraCaseDataExport caseReport, List<Resource> resources) throws Exception {
        for (AdastraCaseDataExport.Consultation consultation : caseReport.getConsultation()) {
            createEncounterFromConsultation(consultation, caseReport.getAdastraCaseReference(), resources);
        }
    }

    private static void createEncounterFromConsultation(AdastraCaseDataExport.Consultation consultation, String caseRef, List<Resource> resources) throws Exception {
        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ENCOUNTER));

        fhirEncounter.setId(caseRef + ":" + consultation.getStartTime() + ":" + consultation.getEndTime() + ":" + consultation.getConsultationBy().getName());
        String consultationID = fhirEncounter.getId();
        checkForDuplicateConsultations(consultationID);

        fhirEncounter.setPatient(AdastraHelper.createPatientReference());
        fhirEncounter.addEpisodeOfCare(AdastraHelper.createEpisodeReference());

        Date consultationStart = XmlDateHelper.convertDate(consultation.getStartTime());
        Period fhirPeriod = PeriodHelper.createPeriod(consultationStart, null);
        fhirEncounter.setPeriod(fhirPeriod);

        if (consultation.getEndTime() != null) {
            //if we have a end time, the encounter is finished
            fhirEncounter.setStatus(Encounter.EncounterState.FINISHED);

            Date caseEnd = XmlDateHelper.convertDate(consultation.getEndTime());
            fhirPeriod.setEnd(caseEnd);

        } else {
            //if we don't have a end time, the encounter is still active. although this should never happen
            fhirEncounter.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        if (consultation.getLocation() != null) {
            Encounter.EncounterLocationComponent location = fhirEncounter.addLocation();
            location.setLocation(AdastraHelper.createLocationReference(consultation.getLocation()));
        }

        Encounter.EncounterParticipantComponent fhirParticipant = fhirEncounter.addParticipant();
        fhirParticipant.addType(CodeableConceptHelper.createCodeableConcept(EncounterParticipantType.PRIMARY_PERFORMER));
        fhirParticipant.setIndividual(AdastraHelper.createUserReference(consultation.getConsultationBy().getName()));

        resources.add(fhirEncounter);

        if (consultation.getSummary() != null) {
            ObservationTransformer.observationFromFreeText(consultation.getSummary(), consultationID, consultation.getStartTime(), caseRef, resources);
        }

        if (consultation.getMedicalHistory() != null) {
            ObservationTransformer.observationFromFreeText(consultation.getMedicalHistory(), consultationID, consultation.getStartTime(), caseRef, resources);
        }

        if (consultation.getEventOutcome() != null) {
            for (CodedItem codedItem : consultation.getEventOutcome()) {
                ObservationTransformer.observationFromCodedItem(codedItem, consultationID, consultation.getStartTime(), caseRef, resources);
            }
        }

        if (consultation.getClinicalCode() != null) {
            for (CodedItem codedItem : consultation.getClinicalCode()) {
                ObservationTransformer.observationFromCodedItem(codedItem, consultationID, consultation.getStartTime(), caseRef, resources);
            }
        }
    }

    private static void checkForDuplicateConsultations(String consultationId) throws Exception {
        if (consultationIds.contains(consultationId)) {
            throw new TransformException("Duplicate consultation Id found : " + consultationId);
        } else {
            consultationIds.add(consultationId);
        }
    }
}
