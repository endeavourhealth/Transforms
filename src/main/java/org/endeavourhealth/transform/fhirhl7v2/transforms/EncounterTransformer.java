package org.endeavourhealth.transform.fhirhl7v2.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.ExchangeBatchDalI;
import org.endeavourhealth.core.database.dal.audit.ExchangeDalI;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class EncounterTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    public static void updateDwEncounter(Encounter oldEncounter, Encounter newEncounter, FhirResourceFiler fhirResourceFiler) throws Exception {

        if (oldEncounter == null) {
            fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(newEncounter));
            return;
        }

        if (true) {
            throw new Exception("Merging of HL7 and Data Warehouse encounters not supported since change to generate multiple encounters from ADT feed");
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
        //updateEncounterPartOf(oldEncounter, newEncounter);
        updateExtensions(oldEncounter, newEncounter);

        fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(oldEncounter));
    }

    /**
     * updates TWO FHIR Encounter records for each Encounter passed in:
     * - creates/updates a top-level/parent Encounter, representing the Encounter's current state
     * - creates/replaces a child-level Encounter, representing the Encounter's individual events
     *
     * The child encounter is linked to the parent using the partOf element
     * The parent encounter has a link to all children using a Contained List
     */
    public static void updateHl7Encounter(Encounter existingParentEncounter, Encounter newEncounter, FhirResourceFiler fhirResourceFiler) throws Exception {

        String originalEncounterId = newEncounter.getId();
        UUID serviceId = fhirResourceFiler.getServiceId();
        LOG.debug("Received encounter with ID " + originalEncounterId + " for exchange " + fhirResourceFiler.getExchangeId());

        //if the parent already exists, then we merge the content of the new encounter into it, otherwise
        //just use the new encounter as the parent
        EncounterBuilder parentEncounterBuilder = null;
        if (existingParentEncounter == null) {
            //don't trust the FHIR copy function, since it skips at least the ID, so serialise and deserialise
            String json = FhirSerializationHelper.serializeResource(newEncounter);
            Encounter parentCopy = (Encounter)FhirSerializationHelper.deserializeResource(json);
            parentEncounterBuilder = new EncounterBuilder(parentCopy); //COPY the new encounter
            //parentEncounterBuilder = new EncounterBuilder(newEncounter.copy()); //COPY the new encounter
            LOG.debug("Existing encounter is null, so will create new parent Encounter");

        } else {
            parentEncounterBuilder = new EncounterBuilder(existingParentEncounter);
            LOG.debug("Existing encounter is NOT null, so will merge new data into parent Encounter");

            //we sometimes receive ADT messages out of order, so will receive a re-send of the admission
            //after the discarge, for example. So avoid messing up the parent Encounter by only merging
            //in the changes if the new one is newer
            Date dtExistingParent = getRecordedDate(existingParentEncounter);
            Date dtNewEncounter = getRecordedDate(newEncounter);
            if (!dtNewEncounter.before(dtExistingParent)) {

                //copy the contents of the new Encounter into the existing parent
                //fields got from http://hl7.org/fhir/DSTU2/encounter.html
                updateEncounterIdentifiers(existingParentEncounter, newEncounter);
                updateEncounterStatus(existingParentEncounter, newEncounter);
                updateEncounterStatusHistory(existingParentEncounter, newEncounter);
                updateEncounterClass(existingParentEncounter, newEncounter);
                updateEncounterType(existingParentEncounter, newEncounter);
                updateEncounterPriority(existingParentEncounter, newEncounter);
                updateEncounterPatient(existingParentEncounter, newEncounter);
                updateEncounterEpisode(existingParentEncounter, newEncounter);
                updateEncounterIncomingReferral(existingParentEncounter, newEncounter);
                updateEncounterParticipant(existingParentEncounter, newEncounter);
                updateEncounterAppointment(existingParentEncounter, newEncounter);
                updateEncounterPeriod(existingParentEncounter, newEncounter);
                updateEncounterLength(existingParentEncounter, newEncounter);
                updateEncounterReason(existingParentEncounter, newEncounter);
                updateEncounterIndication(existingParentEncounter, newEncounter);
                updateEncounterHospitalisation(existingParentEncounter, newEncounter);
                updateEncounterLocation(existingParentEncounter, newEncounter);
                updateEncounterServiceProvider(existingParentEncounter, newEncounter);
                //updateEncounterPartOf(existingParentEncounter, newEncounter); //never used
                updateExtensions(existingParentEncounter, newEncounter);
            }
        }

        //the child is always just the new Encounter passed in
        EncounterBuilder childEncounterBuilder = new EncounterBuilder(newEncounter);

        //generate a new unique ID for the child encounter using the data date of the exchange (which is the HL7v2 timestamp)
        Date newDataDate = fhirResourceFiler.getDataDate();
        String newChildSourceId = originalEncounterId + ":" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(newDataDate);
        UUID newChildId = IdHelper.getOrCreateEdsResourceId(serviceId, ResourceType.Encounter, newChildSourceId);
        childEncounterBuilder.setId(newChildId.toString());
        LOG.debug("Creating child encounter with ID " + newChildId + ", mapped from source ID " + newChildSourceId);

        //link the child to the parent
        Reference parentRef = ReferenceHelper.createReference(ResourceType.Encounter, originalEncounterId);
        childEncounterBuilder.setPartOf(parentRef);
        LOG.debug("PartOf reference set to " + parentRef);

        //and link the parent to the child
        Reference childRef = ReferenceHelper.createReference(ResourceType.Encounter, newChildId.toString());
        ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
        listBuilder.addReference(childRef); //this fn ignores duplicates, so we don't need to check if already present
        LOG.debug("Child ref added to parent " + parentRef);

        //A23 messages tell us to delete an "event". Analysing a sample of data has shown that the STATUS A23 encounter
        //tells us either to delete the entire encounter or just a specific event in the encounter history
        String adtMessageType = getAdtMessageType(newEncounter);
        if (adtMessageType.equals("ADT^A23")) {
            Encounter.EncounterState status = getStatus(newEncounter);
            if (status == Encounter.EncounterState.CANCELLED) {
                //if the Encounter is cancelled, then delete all child events for the encounter and the top-level one itself
                LOG.debug("Message is A23, so deleting entire encounter");
                ResourceDalI resourceDal = DalProvider.factoryResourceDal();
                for (List_.ListEntryComponent item: listBuilder.getContainedListItems()) {
                    Reference ref = item.getItem();
                    ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                    if (comps.getResourceType() != ResourceType.Encounter) {
                        throw new Exception("Expecting only Encounter references in parent Encounter");
                    }
                    Encounter childEncounter = (Encounter)resourceDal.getCurrentVersionAsResource(serviceId, ResourceType.Encounter, comps.getId());
                    if (childEncounter != null) {
                        LOG.debug("Deleting child encounter " + childEncounter.getId());
                        fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                    }
                }

                //delete the parent encounter
                LOG.debug("Deleting parent encounter " + parentEncounterBuilder.getResourceId());
                fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

                return;

            } else if (status == Encounter.EncounterState.INPROGRESS) {
                //if the Encounter is still in progress, just delete this specific event
                LOG.debug("Message is A23, so deleting child encounter " + childEncounterBuilder.getResourceId());
                fhirResourceFiler.deletePatientResource(null, false, childEncounterBuilder);
                return;

            } else {
                throw new Exception("Unexpected encounter status " + status + " for A23");
            }
        }

        //the child encounter should have the partOf set and no contained list
        if (!childEncounterBuilder.hasPartOf()) {
            throw new Exception("Child encounter " + childEncounterBuilder.getResourceId() + " has no partOf element");
        }
        if (childEncounterBuilder.getResource().hasContained()) {
            throw new Exception("Child encounter " + childEncounterBuilder.getResourceId() + " has contained list");
        }

        //the parent encounter should have a contained list but no partOf element
        if (parentEncounterBuilder.hasPartOf()) {
            throw new Exception("Parent encounter " + parentEncounterBuilder.getResourceId() + " has partOf element");
        }
        if (!parentEncounterBuilder.getResource().hasContained()) {
            throw new Exception("Parent encounter " + parentEncounterBuilder.getResourceId() + " has no contained list");
        }


        LOG.debug("Saving both encounters " + parentEncounterBuilder.getResourceId() + " and " + childEncounterBuilder.getResourceId());
        fhirResourceFiler.savePatientResource(null, false, parentEncounterBuilder, childEncounterBuilder);
    }

    private static Encounter.EncounterState getStatus(Encounter encounter) throws Exception {
        if (!encounter.hasStatus()) {
            throw new Exception("No status found on Encounter");
        }
        return encounter.getStatus();
    }

    private static String getAdtMessageType(Encounter encounter) throws Exception {
        CodeableConcept messageTypeConcept = (CodeableConcept)ExtensionConverter.findExtensionValue(encounter, FhirExtensionUri.HL7_MESSAGE_TYPE);
        if (messageTypeConcept == null
                || !messageTypeConcept.hasCoding()) {
            throw new Exception("No ADT message type found in Encounter");
        }

        Coding coding = messageTypeConcept.getCoding().get(0);
        return coding.getCode();
    }

    private static Date getRecordedDate(Encounter encounter) throws Exception {
        DateTimeType dtRecorded = (DateTimeType)ExtensionConverter.findExtensionValue(encounter, FhirExtensionUri.RECORDED_DATE);
        if (dtRecorded == null) {
            //this should never happen
            throw new Exception("Failed to find recorded date in Encounter");
        }

        return dtRecorded.getValue();
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

    /*private static void updateEncounterPartOf(Encounter oldEncounter, Encounter newEncounter) {
        if (newEncounter.hasPartOf()) {
            Reference ref = newEncounter.getPartOf();
            ref = ref.copy();
            oldEncounter.setPartOf(ref);
        }
    }*/

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
