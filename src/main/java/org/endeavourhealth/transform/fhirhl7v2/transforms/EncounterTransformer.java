package org.endeavourhealth.transform.fhirhl7v2.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class EncounterTransformer {



    public static Resource updateDwEncounter(Encounter oldEncounter, Encounter newEncounter) throws Exception {

        if (oldEncounter == null) {
            return newEncounter;
        }

        //since the Encounter has been last updated by the DW feed, we only want to update fields
        //that will tell us something new about the patient (e.g. they've been discharged)
        //updateEncounterIdentifiers(oldEncounter, newEncounter);
        updateEncounterStatus(oldEncounter, newEncounter);
        updateEncounterStatusHistory(oldEncounter, newEncounter);
        updateEncounterClass(oldEncounter, newEncounter);
        updateEncounterType(oldEncounter, newEncounter);
        updateEncounterPriority(oldEncounter, newEncounter);
        updateEncounterPatient(oldEncounter, newEncounter);
        //updateEncounterEpisode(oldEncounter, newEncounter);
        updateEncounterIncomingReferral(oldEncounter, newEncounter);
        updateEncounterParticipant(oldEncounter, newEncounter);
        updateEncounterAppointment(oldEncounter, newEncounter);
        updateEncounterPeriod(oldEncounter, newEncounter);
        updateEncounterLength(oldEncounter, newEncounter);
        updateEncounterReason(oldEncounter, newEncounter);
        updateEncounterIndication(oldEncounter, newEncounter);
        updateEncounterHospitalisation(oldEncounter, newEncounter);
        updateEncounterLocation(oldEncounter, newEncounter);
        //updateEncounterServiceProvider(oldEncounter, newEncounter);
        updateEncounterPartOf(oldEncounter, newEncounter);
        updateExtensions(oldEncounter, newEncounter);

        return oldEncounter;
    }

    public static Resource updateHl7Encounter(Encounter oldEncounter, Encounter newEncounter) throws Exception {

        if (oldEncounter == null) {
            return newEncounter;
        }

        //field got from http://hl7.org/fhir/DSTU2/encounter.html
        updateEncounterIdentifiers(oldEncounter, newEncounter);
        updateEncounterStatus(oldEncounter, newEncounter);
        updateEncounterStatusHistory(oldEncounter, newEncounter);
        updateEncounterClass(oldEncounter, newEncounter);
        updateEncounterType(oldEncounter, newEncounter);
        updateEncounterPriority(oldEncounter, newEncounter);
        updateEncounterPatient(oldEncounter, newEncounter);
        updateEncounterEpisode(oldEncounter, newEncounter);
        updateEncounterIncomingReferral(oldEncounter, newEncounter);
        updateEncounterParticipant(oldEncounter, newEncounter);
        updateEncounterAppointment(oldEncounter, newEncounter);
        updateEncounterPeriod(oldEncounter, newEncounter);
        updateEncounterLength(oldEncounter, newEncounter);
        updateEncounterReason(oldEncounter, newEncounter);
        updateEncounterIndication(oldEncounter, newEncounter);
        updateEncounterHospitalisation(oldEncounter, newEncounter);
        updateEncounterLocation(oldEncounter, newEncounter);
        updateEncounterServiceProvider(oldEncounter, newEncounter);
        updateEncounterPartOf(oldEncounter, newEncounter);
        updateExtensions(oldEncounter, newEncounter);

        return oldEncounter;
    }


    private static void updateExtensions(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasExtension()) {
            return;
        }

        for (Extension newExtension: newEncounter.getExtension()) {
            String newUrl = newExtension.getUrl();

            Extension oldExtension = ExtensionConverter.findExtension(oldEncounter, newUrl);
            if (oldExtension == null) {
                newExtension = newExtension.copy();
                oldEncounter.addExtension(newExtension);

            } else {
                Type newValue = newExtension.getValue();
                newValue = newValue.copy();
                oldExtension.setValue(newValue);
            }
        }
    }

    private static void updateEncounterPartOf(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasPartOf()) {
            Reference ref = newEncounter.getPartOf();
            ref = ref.copy();
            oldEncounter.setPartOf(ref);
        }
    }

    private static void updateEncounterServiceProvider(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasServiceProvider()) {
            Reference ref = newEncounter.getServiceProvider();
            ref = ref.copy();
            oldEncounter.setServiceProvider(ref);
        }
    }

    private static void updateEncounterLocation(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasLocation()) {
            return;
        }

        for (Encounter.EncounterLocationComponent newLocation: newEncounter.getLocation()) {

            //find any locations on the old encounter that match the same location reference
            List<Encounter.EncounterLocationComponent> oldLocationsForSamePlace = new ArrayList<>();

            if (oldEncounter.hasLocation()) {
                for (Encounter.EncounterLocationComponent oldLocation: oldEncounter.getLocation()) {
                    if (ReferenceHelper.equals(oldLocation.getLocation(), newLocation.getLocation())) {
                        oldLocationsForSamePlace.add(oldLocation);
                    }
                }
            }

            boolean addNewLocation;

            if (oldLocationsForSamePlace.isEmpty()) {
                //if this is the first time we've heard of this location, just add it
                addNewLocation = true;

            } else {

                if (!newLocation.hasPeriod()
                        && !newLocation.hasStatus()) {
                    //if the new participant doesn't have a status or period, there's no new info, so don't add it
                    addNewLocation = false;

                } else {
                    addNewLocation = true;

                    for (Encounter.EncounterLocationComponent oldLocation: oldLocationsForSamePlace) {

                        if (newLocation.hasPeriod()) {
                            Period newPeriod = newLocation.getPeriod();

                            Period oldPeriod = null;
                            if (oldLocation.hasPeriod()) {
                                oldPeriod = oldLocation.getPeriod();
                            }

                            //see if we can merge the old and new periods
                            Period mergedPeriod = compareAndMergePeriods(oldPeriod, newPeriod);
                            if (mergedPeriod != null) {
                                oldLocation.setPeriod(mergedPeriod);
                                addNewLocation = false;

                            } else {
                                //if we couldn't merge the period, then skip this location
                                //and don't try to set the new status on this old location
                                continue;
                            }
                        }

                        if (newLocation.hasStatus()) {
                            Encounter.EncounterLocationStatus newStatus = newLocation.getStatus();
                            oldLocation.setStatus(newStatus);
                            addNewLocation = false;
                        }

                        if (!addNewLocation ) {
                            break;
                        }
                    }
                }
            }

            if (addNewLocation) {
                newLocation = newLocation.copy();
                oldEncounter.getLocation().add(newLocation);
            }
        }
    }

    private static void updateEncounterHospitalisation(Encounter oldEncounter, Encounter newEncounter) {

        if (newEncounter.hasHospitalization()) {
            Encounter.EncounterHospitalizationComponent h = newEncounter.getHospitalization();
            h = h.copy();
            oldEncounter.setHospitalization(h);
        }
    }

    private static void updateEncounterIndication(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasIndication()) {
            for (Reference newIndication: newEncounter.getIndication()) {

                if (!oldEncounter.hasIndication()
                        || !ReferenceHelper.contains(oldEncounter.getIndication(), newIndication)) {
                    newIndication = newIndication.copy();
                    oldEncounter.addIndication(newIndication);
                }
            }
        }
    }

    private static void updateEncounterReason(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasReason()) {
            return;
        }

        for (CodeableConcept newConcept: newEncounter.getReason()) {

            boolean add = true;
            if (oldEncounter.hasReason()) {
                for (CodeableConcept oldConcept: oldEncounter.getReason()) {
                    if (oldConcept.equalsDeep(newConcept)) {
                        add = false;
                        break;
                    }
                }
            }

            if (add) {
                newConcept = newConcept.copy();
                oldEncounter.addReason(newConcept);
            }
        }
    }

    private static void updateEncounterLength(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasLength()) {
            Duration d = newEncounter.getLength();
            d = d.copy();
            oldEncounter.setLength(d);
        }
    }

    private static void updateEncounterPeriod(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasPeriod()) {
            return;
        }

        Period oldPeriod = null;
        if (oldEncounter.hasPeriod()) {
            oldPeriod = oldEncounter.getPeriod();
        } else {
            oldPeriod = new Period();
            oldEncounter.setPeriod(oldPeriod);
        }

        Period newPeriod = newEncounter.getPeriod();

        if (newPeriod.hasStart()) {
            Date d = newPeriod.getStart();
            oldPeriod.setStart(d);
        }

        if (newPeriod.hasEnd()) {
            Date d = newPeriod.getEnd();
            oldPeriod.setEnd(d);
        }
    }

    private static void updateEncounterAppointment(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasAppointment()) {
            return;
        }

        Reference ref = newEncounter.getAppointment();
        ref = ref.copy();
        oldEncounter.setAppointment(ref);
    }

    private static void updateEncounterParticipant(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasParticipant()) {
            return;
        }

        for (Encounter.EncounterParticipantComponent newParticipant: newEncounter.getParticipant()) {

            //find all old participants for the same person and type
            List<Encounter.EncounterParticipantComponent> oldParticipantsSamePersonAndType = new ArrayList<>();

            for (Encounter.EncounterParticipantComponent oldParticipant : oldEncounter.getParticipant()) {

                if (oldParticipant.hasIndividual() != newParticipant.hasIndividual()) {
                    continue;
                }

                if (oldParticipant.hasIndividual()
                        && !ReferenceHelper.equals(oldParticipant.getIndividual(), newParticipant.getIndividual())) {
                    continue;
                }

                if (oldParticipant.hasType() != newParticipant.hasType()) {
                    continue;
                }

                if (oldParticipant.hasType()) {
                    int oldCount = oldParticipant.getType().size();
                    int newCount = newParticipant.getType().size();
                    if (oldCount != newCount) {
                        continue;
                    }

                    boolean typesMatch = true;
                    for (CodeableConcept oldType: oldParticipant.getType()) {
                        boolean foundType = false;
                        for (CodeableConcept newType: newParticipant.getType()) {
                            if (oldType.equalsDeep(newType)) {
                                foundType = true;
                                break;
                            }
                        }
                        if (!foundType) {
                            typesMatch = false;
                            break;
                        }
                    }
                    if (!typesMatch) {
                        continue;
                    }
                }

                //if we finally make it here, this old participant matches the new one on person and type
                oldParticipantsSamePersonAndType.add(oldParticipant);
            }

            boolean addNewParticipant;

            if (oldParticipantsSamePersonAndType.isEmpty()) {
                //if there are no old participants with the same type and person, then just add the new one
                addNewParticipant = true;

            } else {

                if (!newParticipant.hasPeriod()) {
                    //if the new participant doesn't have a period, there's no new info, so don't add it
                    addNewParticipant = false;

                } else {
                    addNewParticipant = true;
                    Period newPeriod = newParticipant.getPeriod();

                    for (Encounter.EncounterParticipantComponent oldParticipant : oldParticipantsSamePersonAndType) {

                        Period oldPeriod = null;
                        if (oldParticipant.hasPeriod()) {
                            oldPeriod = oldParticipant.getPeriod();
                        }

                        //see if we can merge the old and new periods
                        Period mergedPeriod = compareAndMergePeriods(oldPeriod, newPeriod);
                        if (mergedPeriod != null) {
                            oldParticipant.setPeriod(mergedPeriod);
                            addNewParticipant = false;
                            break;
                        }
                    }
                }
            }

            if (addNewParticipant) {
                newParticipant = newParticipant.copy();
                oldEncounter.getParticipant().add(newParticipant);
            }
        }
    }

    /**
     * compares old and new periods, returning a new merged one if they can be merged, null if they can't
     */
    private static Period compareAndMergePeriods(Period oldPeriod, Period newPeriod) {

        //if the nwe period is empty, just return the old period
        if (newPeriod == null
                || (!newPeriod.hasStart() && !newPeriod.hasEnd())) {
            return oldPeriod.copy();
        }

        //if the old period is empty, just return the new period
        if (oldPeriod == null
                || (!oldPeriod.hasStart() && !oldPeriod.hasEnd())) {
            return newPeriod.copy();
        }

        //if here, then we have start, end or both on our new period and start, end of both on our old period too
        Date newStart = null;
        Date newEnd = null;
        if (newPeriod.hasStart()) {
            newStart = newPeriod.getStart();
        }
        if (newPeriod.hasEnd()) {
            newEnd = newPeriod.getEnd();
        }

        Date oldStart = null;
        Date oldEnd = null;
        if (oldPeriod.hasStart()) {
            oldStart = oldPeriod.getStart();
        }
        if (oldPeriod.hasEnd()) {
            oldEnd = oldPeriod.getEnd();
        }

        //if we have old and new starts, and they don't match, then skip this participant
        if (newStart != null
                && oldStart != null
                && !oldStart.equals(newStart)) {
            return null;
        }

        //if we have a new start date, make sure it's not after the existing end date, if we have one
        if (oldEnd != null
                && newStart != null
                && newStart.after(oldEnd)) {
            return null;
        }

        //if we have old and new end dates, and they don't match, then skip this participant
        if (newEnd != null
                && oldEnd != null
                && !oldEnd.equals(newEnd)) {
            return null;
        }

        //if we have a new end date, make sure it's not before the start date, if we have one
        if (oldStart != null
                && newEnd != null
                && newEnd.before(oldStart)) {
            return null;
        }

        //if we make it here, set the new dates in the old period
        Period merged = oldPeriod.copy();

        if (newStart != null) {
            merged.setStart(newStart);
        }
        if (newEnd != null) {
            merged.setEnd(newEnd);
        }

        return merged;
    }

    private static void updateEncounterIncomingReferral(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasIncomingReferral()) {
            return;
        }

        for (Reference newReference: newEncounter.getIncomingReferral()) {

            if (!oldEncounter.hasIncomingReferral()
                    || !ReferenceHelper.contains(oldEncounter.getIncomingReferral(), newReference)) {
                newReference = newReference.copy();
                oldEncounter.addIncomingReferral(newReference);
            }
        }
    }

    private static void updateEncounterEpisode(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasEpisodeOfCare()) {
            return;
        }

        for (Reference newReference: newEncounter.getEpisodeOfCare()) {

            if (!oldEncounter.hasEpisodeOfCare()
                    || !ReferenceHelper.contains(oldEncounter.getEpisodeOfCare(), newReference)) {
                newReference = newReference.copy();
                oldEncounter.addEpisodeOfCare(newReference);
            }
        }
    }

    private static void updateEncounterPatient(Encounter oldEncounter, Encounter newEncounter) throws Exception {
        //the subject (i.e. patient) should never change, so check this and validate rather than apply changes
        if (!oldEncounter.hasPatient()) {
            throw new TransformException("No patient on OLD " + oldEncounter.getResourceType() + " " + oldEncounter.getId());
        }
        if (!newEncounter.hasPatient()) {
            throw new TransformException("No patient on NEW " + newEncounter.getResourceType() + " " + newEncounter.getId());
        }

        //this validation doesn't work now that we persist merge mappings and automatically apply them
        /*String oldPatientRef = oldEncounter.getPatient().getReference();
        String newPatientRef = newEncounter.getPatient().getReference();
        if (!oldPatientRef.equals(newPatientRef)) {
            throw new TransformException("Old " + oldEncounter.getResourceType() + " " + oldEncounter.getId() + " links to " + oldPatientRef + " but new version to " + newPatientRef);
        }*/
    }

    private static void updateEncounterPriority(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasPriority()) {
            CodeableConcept codeableConcept = newEncounter.getPriority();
            codeableConcept = codeableConcept.copy(); //not strictly necessary, but can't hurt
            oldEncounter.setPriority(codeableConcept);
        }
    }

    private static void updateEncounterType(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasType()) {
            return;
        }

        for (CodeableConcept newConcept: newEncounter.getType()) {

            boolean add = true;
            if (oldEncounter.hasType()) {
                for (CodeableConcept oldConcept: oldEncounter.getType()) {
                    if (oldConcept.equalsDeep(newConcept)) {
                        add = false;
                        break;
                    }
                }
            }

            if (add) {
                newConcept = newConcept.copy();
                oldEncounter.addType(newConcept);
            }
        }
    }

    private static void updateEncounterClass(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasClass_()) {
            return;
        }

        Encounter.EncounterClass cls = newEncounter.getClass_();
        oldEncounter.setClass_(cls);
    }

    private static void updateEncounterStatusHistory(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasStatusHistory()) {
            return;
        }

        for (Encounter.EncounterStatusHistoryComponent newStatus: newEncounter.getStatusHistory()) {

            //see if we can find a matching status element that we can update with new info (i.e. dates)
            boolean add = true;

            if (oldEncounter.hasStatusHistory()) {

                //status and period are mandatory on the status history, so we don't need to mess about checking the has... fns
                Encounter.EncounterState newState = newStatus.getStatus();
                Period newPeriod = newStatus.getPeriod();

                for (Encounter.EncounterStatusHistoryComponent oldStatus: oldEncounter.getStatusHistory()) {

                    Encounter.EncounterState oldState = oldStatus.getStatus();
                    Period oldPeriod = oldStatus.getPeriod();
                    if (oldState != newState) {
                        continue;
                    }

                    Period mergedPeriod = compareAndMergePeriods(oldPeriod, newPeriod);
                    if (mergedPeriod == null) {
                        continue;
                    }

                    //if we make it here, we can set the new merged period on the old status and skip adding it as a new status entirely
                    oldStatus.setPeriod(mergedPeriod);
                    add = false;
                }
            }

            if (add) {
                newStatus = newStatus.copy();
                oldEncounter.getStatusHistory().add(newStatus);
            }
        }
    }

    private static void updateEncounterStatus(Encounter oldEncounter, Encounter newEncounter) {
        if (!newEncounter.hasStatus()) {
            return;
        }

        Encounter.EncounterState oldStatus = null;
        if (oldEncounter.hasStatus()) {
            oldStatus = oldEncounter.getStatus();
        }

        //set the new status
        Encounter.EncounterState newStatus = newEncounter.getStatus();
        oldEncounter.setStatus(newStatus);

        //if we actually changed the status, see if we need to move the old status into the status history
        if (oldStatus != null
                && oldStatus != newStatus) {

            boolean add = true;

            //see if we have a status history record with the old status in
            if (oldEncounter.hasStatusHistory()) {
                for (Encounter.EncounterStatusHistoryComponent oldStatusHistory: oldEncounter.getStatusHistory()) {
                    if (oldStatusHistory.getStatus() == oldStatus) {
                        add = false;
                        break;
                    }
                }
            }

            if (add) {
                Encounter.EncounterStatusHistoryComponent newStatusHistory = oldEncounter.addStatusHistory();
                newStatusHistory.setStatus(oldStatus);
                newStatusHistory.setPeriod(new Period()); //period is mandatory, but we don't have any dates to go in there
            }
        }
    }



    private static void updateEncounterIdentifiers(Encounter oldEncounter, Encounter newEncounter) {

        if (!newEncounter.hasIdentifier()) {
            return;
        }

        for (Identifier newIdentifier: newEncounter.getIdentifier()) {

            boolean add = true;

            if (oldEncounter.hasIdentifier()) {
                for (Identifier oldIdentifier: oldEncounter.getIdentifier()) {

                    if (oldIdentifier.hasUse() != newIdentifier.hasUse()) {
                        continue;
                    }
                    if (oldIdentifier.hasUse()
                            && oldIdentifier.getUse() != newIdentifier.getUse()) {
                        continue;
                    }
                    if (oldIdentifier.hasType() != newIdentifier.hasType()) {
                        continue;
                    }
                    if (oldIdentifier.hasType()
                            && !oldIdentifier.getType().equalsDeep(newIdentifier.getType())) {
                        continue;
                    }
                    if (oldIdentifier.hasSystem() != newIdentifier.hasSystem()) {
                        continue;
                    }
                    if (oldIdentifier.hasSystem()
                            && !oldIdentifier.getSystem().equals(newIdentifier.getSystem())) {
                        continue;
                    }
                    if (oldIdentifier.hasValue() != newIdentifier.hasValue()) {
                        continue;
                    }
                    if (oldIdentifier.hasValue()
                            && !oldIdentifier.getValue().equals(newIdentifier.getValue())) {
                        continue;
                    }
                    if (oldIdentifier.hasPeriod() != newIdentifier.hasPeriod()) {
                        continue;
                    }
                    if (oldIdentifier.hasPeriod()
                            && !oldIdentifier.getPeriod().equalsDeep(newIdentifier.getPeriod())) {
                        continue;
                    }
                    if (oldIdentifier.hasAssigner() != newIdentifier.hasAssigner()) {
                        continue;
                    }
                    if (oldIdentifier.hasAssigner()
                            && !oldIdentifier.getAssigner().equalsDeep(newIdentifier.getAssigner())) {
                        continue;
                    }

                    //if we make it here, the old identifier matches the new one, so we don't need to add it
                    add = false;
                    break;
                }
            }

            if (add) {
                newIdentifier = newIdentifier.copy();
                oldEncounter.addIdentifier(newIdentifier);
            }
        }
    }

}