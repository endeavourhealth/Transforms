package org.endeavourhealth.transform.tpp.xml.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.FhirHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.tpp.xml.schema.Event;
import org.endeavourhealth.transform.tpp.xml.schema.EventMethod;
import org.hl7.fhir.instance.model.*;

import javax.xml.datatype.XMLGregorianCalendar;
import java.time.Instant;
import java.util.List;

public class EventTransformer {

    public static void transform(Event tppEvent, List<Resource> fhirResources) throws TransformException {

        Encounter fhirEncounter = createEncounter(tppEvent, fhirResources);

        MedicationTransformer.transform(tppEvent.getMedication(), tppEvent, fhirEncounter, fhirResources);
        ClinicalCodeTransformer.transform(tppEvent.getClinicalCode(), tppEvent, fhirEncounter, fhirResources);
        ReminderTransformer.transform(tppEvent.getReminder(), tppEvent, fhirEncounter, fhirResources);
        RecallTransformer.transform(tppEvent.getRecall(), tppEvent, fhirEncounter, fhirResources);
        VaccinationTransformer.transform(tppEvent.getVaccination(), tppEvent, fhirEncounter, fhirResources);
        RepeatMedicationTransformer.transform(tppEvent.getRepeatMedication(), tppEvent, fhirEncounter, fhirResources);
        LetterTransformer.transform(tppEvent.getLetter(), tppEvent, fhirEncounter, fhirResources);
        PatientPlanTransformer.transform(tppEvent.getPatientPlan(), tppEvent, fhirEncounter, fhirResources);
        CarePlanTransformer.transform(tppEvent.getCarePlan(), tppEvent, fhirEncounter, fhirResources);
        RelationshipTransformer.transform(tppEvent.getRelationship(), tppEvent, fhirEncounter, fhirResources);
        ReportTransformer.transform(tppEvent.getReport(), tppEvent, fhirEncounter, fhirResources);
        AttachmentTransformer.transform(tppEvent.getAttachment(), tppEvent, fhirEncounter, fhirResources);
        ActivityTransformer.transform(tppEvent.getActivity(), tppEvent, fhirEncounter, fhirResources);
        NarrativeTransformer.transform(tppEvent.getNarrative(), tppEvent, fhirEncounter, fhirResources);
        DrugSensitivityTransformer.transform(tppEvent.getDrugSensitivity(), tppEvent, fhirEncounter, fhirResources);


    }

    private static Encounter createEncounter(Event tppEvent, List<Resource> fhirResources) throws TransformException {

        //TODO - need a way to work out what Events should become FHIR Encounters or not

        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setId(tppEvent.getEventUID());
        fhirEncounter.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ENCOUNTER));
        fhirResources.add(fhirEncounter);

        //TODO - link encounter to problem?
        //List<String> linkedProblemUID = tppEvent.getLinkedProblemUID();

        //whether events happened or are expected is based on the date
        //future-dated events are rare, but do exist - typically to record expected date of delivery
        XMLGregorianCalendar dateTime = tppEvent.getDateTime();
        if (dateTime.toGregorianCalendar().toInstant().isAfter(Instant.now())) {
            fhirEncounter.setStatus(Encounter.EncounterState.PLANNED);
        } else {
            fhirEncounter.setStatus(Encounter.EncounterState.FINISHED);
        }

        Period fhirPeriod = new Period();
        fhirEncounter.setPeriod(fhirPeriod);
        fhirPeriod.setStart(dateTime.toGregorianCalendar().getTime());
        //TODO - do we need an end date for the FHIR Encounter?

        String userName = tppEvent.getUserName();
        String doneBy = tppEvent.getDoneBy();

        if (!Strings.isNullOrEmpty(userName)) {
            //if a userName is present, use that to link to the practitioner resource
            Reference fhirReference = ReferenceHelper.createReference(ResourceType.Practitioner, userName);
            fhirEncounter.addParticipant(new Encounter.EncounterParticipantComponent().setIndividual(fhirReference));

        } else if (!Strings.isNullOrEmpty(doneBy)) {
            //if no userName is present, the textual doneBy to indicate the participant
            fhirEncounter.addParticipant(new Encounter.EncounterParticipantComponent().addType(CodeableConceptHelper.createCodeableConcept(doneBy)));
        }

        String doneAt = tppEvent.getDoneAt();

        //the software used to record the event isn't interesting to third parties
        //String software = tppEvent.getSoftware();

        EventMethod method = tppEvent.getMethod();
        String linkedReferralUID = tppEvent.getLinkedReferralUID();

        fhirEncounter.setPatient(FhirHelper.findAndCreateReference(Patient.class, fhirResources));

        return fhirEncounter;
    }
}
