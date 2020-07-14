package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppConfigListOption;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SREvent;
import org.endeavourhealth.transform.tpp.csv.transforms.patient.SRPatientRegistrationTransformer;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SREventTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SREventTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SREvent.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SREvent) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SREvent parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell eventId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            Encounter encounter = (Encounter)csvHelper.retrieveResource(eventId.getString(), ResourceType.Encounter);
            if (encounter != null) {
                EncounterBuilder encounterBuilder = new EncounterBuilder(encounter);
                encounterBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, encounterBuilder);
            }
            return;
        }

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(eventId.getString(), eventId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        encounterBuilder.setPatient(patientReference, patientId);

        //link the encounter to the episode of care
        String episodeId = findEpisodeIdForPatient(patientId, csvHelper);
        if (!Strings.isNullOrEmpty(episodeId)) {
            Reference episodeReference = csvHelper.createEpisodeReference(episodeId);
            encounterBuilder.setEpisodeOfCare(episodeReference);
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            encounterBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell eventDate = parser.getDateEvent();
        if (!eventDate.isEmpty()) {
            encounterBuilder.setPeriodStart(eventDate.getDateTime(), eventDate);
        }

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            encounterBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        CsvCell orgDoneAtCell = parser.getIDOrganisationDoneAt();
        Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, orgDoneAtCell);
        if (staffReference != null) {
            encounterBuilder.addParticipant(staffReference, EncounterParticipantType.PRIMARY_PERFORMER, staffMemberIdDoneBy);
        }

        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        CsvCell visitOrg = parser.getIDOrganisation();
        if (!visitOrg.isEmpty()) {
            Reference orgReference = csvHelper.createOrganisationReference(visitOrg);
            encounterBuilder.setServiceProvider(orgReference, visitOrg);
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

        //carry over linked items from any previous instance of this encounter.
        ReferenceList previousReferences = csvHelper.findConsultationPreviousLinkedResources(eventId);
        containedListBuilder.addReferences(previousReferences);

        //apply any new linked items from this extract. Encounter links set-up in Codes/Referral/Medication etc. pre-transformers
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(eventId);
        containedListBuilder.addReferences(newLinkedResources);

        //apply any linked appointments / visits
        ReferenceList appLinkedResources = csvHelper.getAndRemoveEncounterAppointmentOrVisitMap(encounterBuilder.getResourceId());
        if (appLinkedResources != null) {
            encounterBuilder.setAppointment(appLinkedResources.getReference(0));
        }

        CsvCell branchIdCell = parser.getIDBranch();
        if (!branchIdCell.isEmpty()) {
            Reference locationReference = ReferenceHelper.createReference(ResourceType.Location, branchIdCell.getString());
            encounterBuilder.addLocation(locationReference, branchIdCell);
        } else {
            //if no branchID is present, this means it was done at the main surgery, so link to the location we will have
            //creating using the ID of the org as it's source ID
            CsvCell orgId = parser.getIDOrganisation();
            if (!orgId.isEmpty()) { //empty in test pack for some reason
                Reference locationReference = ReferenceHelper.createReference(ResourceType.Location, orgId.getString());
                encounterBuilder.addLocation(locationReference, branchIdCell);
            }
        }

        CsvCell contactEventLocationCell = parser.getContactEventLocation();
        if (!TppCsvHelper.isEmptyOrNegative(contactEventLocationCell)) {
            TppConfigListOption tppConfigListOption = TppCsvHelper.lookUpTppConfigListOption(contactEventLocationCell);
            if (tppConfigListOption != null) {
                String contactEventLocation = tppConfigListOption.getListOptionName();
                if (!Strings.isNullOrEmpty(contactEventLocation)) {
                    //possible values Telephone, Home, Surgery etc.
                    CodeableConceptBuilder codeableConceptbuilder = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Location_Type);
                    codeableConceptbuilder.setText(contactEventLocation, contactEventLocationCell);
                }
            }
        }

        CsvCell eventIncomplete = parser.getEventIncomplete();
        if (!eventIncomplete.isEmpty() && eventIncomplete.getBoolean()) {
            encounterBuilder.setIncomplete(true, eventIncomplete);
        }

        String methodDesc;

        CsvCell clinicalEventCell = parser.getClinicalEvent();
        if (clinicalEventCell.getBoolean()) {
            methodDesc = "Clinical";

        } else {
            methodDesc = "Administrative";
        }

        CsvCell contactTypeCell = parser.getContactMethod();
        if (!TppCsvHelper.isEmptyOrNegative(contactTypeCell)) {
            TppConfigListOption tppConfigListOption = TppCsvHelper.lookUpTppConfigListOption(contactTypeCell);
            if (tppConfigListOption != null) {
                String contactType = tppConfigListOption.getListOptionName();
                if (!Strings.isNullOrEmpty(contactType)) {
                    methodDesc += ", " + contactType;
                }
            }
        }

        CodeableConceptBuilder codeableConceptbuilder = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptbuilder.setText(methodDesc, clinicalEventCell, contactTypeCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }


    private static String findEpisodeIdForPatient(CsvCell patientId, TppCsvHelper csvHelper) throws Exception {

        //we have an internal ID mapping to tell us the active episode for each patient
        String episodeId = csvHelper.getInternalId(SRPatientRegistrationTransformer.PATIENT_ID_TO_ACTIVE_EPISODE_ID, patientId.getString());
        if (!Strings.isNullOrEmpty(episodeId)) {
            return episodeId;
        }

        //if the patient has no active episode, work out the most relevant one and use that
        ResourceWrapper mostRecentEnded = null;
        Date mostRecentEndDate = null;

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        UUID patientUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, patientId.getString());
        if (patientUuid==null) {
            TransformWarnings.log(LOG,csvHelper, "SEVERE: Resource lookup failed for patient Id {}: Service{}", patientId.getString(), csvHelper.getServiceId());
            return null;
        }
        List<ResourceWrapper> episodeWrappers = resourceDal.getResourcesByPatient(csvHelper.getServiceId(), patientUuid, ResourceType.EpisodeOfCare.toString());
        for (ResourceWrapper wrapper: episodeWrappers) {
            if (wrapper.isDeleted()) {
                continue;
            }

            EpisodeOfCare episodeOfCare = (EpisodeOfCare) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());

            Date endDate = null;
            if (episodeOfCare.hasPeriod()) {
                endDate = episodeOfCare.getPeriod().getEnd();
            }

            //if no end date, then this episode is active and should be the one used
            if (endDate == null) {
                mostRecentEnded = wrapper;
                break;
            }

            if (mostRecentEnded == null
                    || endDate.after(mostRecentEndDate)) {
                mostRecentEnded = wrapper;
                mostRecentEndDate = endDate;
            }
        }

        if (mostRecentEnded != null) {
            Reference episodeReference = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, mostRecentEnded.getResourceId().toString());
            episodeReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(csvHelper, episodeReference);
            episodeId = ReferenceHelper.getReferenceId(episodeReference);

            //and save the mapping so we don't have to repeat this again
            csvHelper.saveInternalId(SRPatientRegistrationTransformer.PATIENT_ID_TO_ACTIVE_EPISODE_ID, patientId.getString(), episodeId);
        }

        return null;
    }
}
