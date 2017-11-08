package org.endeavourhealth.transform.adastra.transforms.clinical;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.guidMapper;

public class EncounterTransform {

    public static void createMainCaseEncounter(AdastraCaseDataExport caseReport, List<Resource> resources) {

        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ENCOUNTER));

        if (caseReport.getLatestAppointment() != null)
            fhirEncounter.setAppointment(AdastraHelper.createAppointmentReference());

        AdastraHelper.setUniqueId(fhirEncounter, caseReport.getAdastraCaseReference());
        guidMapper.put("caseEncounter", fhirEncounter.getId());

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

    }

    public static void createChildEncountersFromConsultations(AdastraCaseDataExport caseReport, List<Resource> resources) throws Exception {
        for (AdastraCaseDataExport.Consultation consultation : caseReport.getConsultation()) {
            createEncounterFromConsultation(consultation, resources);
        }
    }

    private static void createEncounterFromConsultation(AdastraCaseDataExport.Consultation consultation, List<Resource> resources) throws Exception {
        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ENCOUNTER));

        AdastraHelper.setUniqueId(fhirEncounter, UUID.randomUUID().toString());
        String consultationID = fhirEncounter.getId();

        fhirEncounter.setPatient(AdastraHelper.createPatientReference());
        fhirEncounter.addEpisodeOfCare(AdastraHelper.createEpisodeReference());

        fhirEncounter.setPartOf(AdastraHelper.createEncounterReference("caseEncounter"));

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
            ObservationTransformer.observationFromFreeText(consultation.getSummary(), consultationID, consultation.getStartTime(), resources);
        }

        if (consultation.getMedicalHistory() != null) {
            ObservationTransformer.observationFromFreeText(consultation.getMedicalHistory(), consultationID, consultation.getStartTime(), resources);
        }

        if (consultation.getEventOutcome() != null) {
            for (CodedItem codedItem : consultation.getEventOutcome()) {
                ObservationTransformer.observationFromCodedItem(codedItem, consultationID, consultation.getStartTime(), resources);
            }
        }

        if (consultation.getClinicalCode() != null) {
            for (CodedItem codedItem : consultation.getClinicalCode()) {
                ObservationTransformer.observationFromCodedItem(codedItem, consultationID, consultation.getStartTime(), resources);
            }
        }

    }
}
