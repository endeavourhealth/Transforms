package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SREvent;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, encounterBuilder);
            }
            return;
        }

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(eventId.getString(), eventId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        encounterBuilder.setPatient(patientReference, patientId);

        //link the encounter to the episode of care
        Reference episodeReference = csvHelper.createEpisodeReference(patientId);
        encounterBuilder.setEpisodeOfCare(episodeReference);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            encounterBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell eventDate = parser.getDateEvent();
        if (!eventDate.isEmpty()) {
            encounterBuilder.setPeriodStart(eventDate.getDate(), eventDate);
        }

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            encounterBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong()> 0) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy);
            encounterBuilder.addParticipant(staffReference, EncounterParticipantType.PRIMARY_PERFORMER, staffMemberIdDoneBy);
        }

        //TPP consultation authoriser is not useful (in SystmOne for that matter), so not transforming
        /*CsvCell encounterAuthoriserId = parser.getIDAuthorisedBy();
        if (!encounterAuthoriserId.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, encounterAuthoriserId.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                encounterBuilder.addParticipant(staffReference, EncounterParticipantType.PARTICIPANT);
            }
        }*/

        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        CsvCell contactTypeCell = parser.getContactMethod();
        if (!contactTypeCell.isEmpty() && contactTypeCell.getLong()> 0) {
            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(contactTypeCell, parser);
            if (tppConfigListOption != null) {
                String contactType = tppConfigListOption.getListOptionName();
                if (!Strings.isNullOrEmpty(contactType)) {
                    CodeableConceptBuilder codeableConceptbuilder = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                    codeableConceptbuilder.setText(contactType);
                }
            }
        }

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
        if (!contactEventLocationCell.isEmpty() && contactEventLocationCell.getInt() != -1) {
            //TODO - verify that this cell IS a number and work out how that maps to our Location resources
            Reference locationReference = ReferenceHelper.createReference(ResourceType.Location, contactEventLocationCell.getString());
            encounterBuilder.addLocation(locationReference, branchIdCell);
        }

        CsvCell eventIncomplete = parser.getEventIncomplete();
        if (!eventIncomplete.getBoolean()) {
            encounterBuilder.setIncomplete(true, eventIncomplete);
        }

        //TODO - the following column need transforming:
        // Cannot find any clinic related information in Encounter - FG
        //getClinicalEvent()


        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }
}
