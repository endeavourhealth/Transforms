package org.endeavourhealth.transform.adastra.transforms.clinical;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Period;

import java.util.Date;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.consultationIds;
import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.uniqueIdMapper;

public class EncounterTransform {

    public static void createMainCaseEncounter(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ENCOUNTER));

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

        fhirResourceFiler.savePatientResource(null, fhirEncounter);
    }

    public static void createChildEncountersFromConsultations(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler) throws Exception {
        for (AdastraCaseDataExport.Consultation consultation : caseReport.getConsultation()) {
            createEncounterFromConsultation(consultation, caseReport.getAdastraCaseReference(), fhirResourceFiler);
        }
    }

    private static void createEncounterFromConsultation(AdastraCaseDataExport.Consultation consultation,
                                                        String caseRef, FhirResourceFiler fhirResourceFiler) throws Exception {
        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ENCOUNTER));

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

        fhirResourceFiler.savePatientResource(null, fhirEncounter);

        if (consultation.getSummary() != null) {
            ObservationTransformer.observationFromFreeText(consultation.getSummary(), consultationID,
                    consultation.getStartTime(), caseRef,
                    "Summary", fhirResourceFiler);
        }

        if (consultation.getMedicalHistory() != null) {
            ObservationTransformer.observationFromFreeText(consultation.getMedicalHistory(), consultationID,
                    consultation.getStartTime(), caseRef,
                    "Medical History", fhirResourceFiler);
        }

        if (consultation.getEventOutcome() != null) {
            for (CodedItem codedItem : consultation.getEventOutcome()) {
                ObservationTransformer.observationFromCodedItem(codedItem, consultationID,
                        consultation.getStartTime(), caseRef,
                        "Event Outcome", fhirResourceFiler);
            }
        }

        if (consultation.getClinicalCode() != null) {
            for (CodedItem codedItem : consultation.getClinicalCode()) {
                ObservationTransformer.observationFromCodedItem(codedItem, consultationID,
                        consultation.getStartTime(), caseRef,
                        "Clinical Note", fhirResourceFiler);
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
