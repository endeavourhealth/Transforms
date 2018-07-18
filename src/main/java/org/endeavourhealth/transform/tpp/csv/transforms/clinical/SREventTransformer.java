package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.*;
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
    }

    private static void createResource(SREvent parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell eventId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if ((deleteData != null) && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else if (!deleteData.isEmpty() && deleteData.getIntAsBoolean()) {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.Encounter encounter
                        = (org.hl7.fhir.instance.model.Encounter) csvHelper.retrieveResource(eventId.getString(),
                        ResourceType.Encounter,
                        fhirResourceFiler);

                if (encounter != null) {
                    EncounterBuilder encounterBuilder = new EncounterBuilder(encounter);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
                }
                return;
            }
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

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                encounterBuilder.setRecordedBy(staffReference, recordedBy);
            }
        }

        CsvCell encounterDoneBy = parser.getIDDoneBy();
        if (!encounterDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(encounterDoneBy);
            encounterBuilder.addParticipant(staffReference, EncounterParticipantType.PRIMARY_PERFORMER, encounterDoneBy);
        }

        CsvCell encounterAuthoriserId = parser.getIDAuthorisedBy();
        if (!encounterAuthoriserId.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                if (encounterBuilder.isIdMapped()) {
                    staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference,fhirResourceFiler);
                }
                encounterBuilder.addParticipant(staffReference, EncounterParticipantType.PARTICIPANT);
            }
        }

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
            if (encounterBuilder.isIdMapped()) {
                orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference,fhirResourceFiler);
            }
            encounterBuilder.setServiceProvider(orgReference, visitOrg);
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

        //carry over linked items from any previous instance of this encounter.
        ReferenceList previousReferences = csvHelper.findConsultationPreviousLinkedResources(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(previousReferences);

        //apply any new linked items from this extract. Encounter links set-up in Codes/Referral/Medication etc. pre-transformers
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(newLinkedResources);

        //apply any linked appointments / visits
        ReferenceList appLinkedResources = csvHelper.getAndRemoveEncounterAppointmentOrVisitMap(encounterBuilder.getResourceId());
        if (appLinkedResources != null) {
            encounterBuilder.setAppointment(appLinkedResources.getReference(0));
        }
        boolean mapIds = !encounterBuilder.isIdMapped();
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapIds, encounterBuilder);
    }
}
