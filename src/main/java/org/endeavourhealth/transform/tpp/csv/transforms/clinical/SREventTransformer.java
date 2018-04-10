package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SREvent;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SREventTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SREventTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SREvent.class);
        while (parser.nextRecord()) {

            try {
                createResource((SREvent)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SREvent parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell visitId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        TppCsvHelper.setUniqueId(encounterBuilder, patientId, visitId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        encounterBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            encounterBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell eventDate = parser.getDateEvent();
        encounterBuilder.setPeriodStart(eventDate.getDate(), eventDate);

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            encounterBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell encounterDoneBy = parser.getIDDoneBy();
        if (!encounterDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(encounterDoneBy);
            encounterBuilder.addParticipant(staffReference, EncounterParticipantType.PRIMARY_PERFORMER, encounterDoneBy);
        }

        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        CsvCell contactTypeCell = parser.getContactMethod();
        if (!contactTypeCell.isEmpty()) {
            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(contactTypeCell.getLong());
            if (tppConfigListOption != null) {
                String contactType = tppConfigListOption.getListOptionName();
                if (!Strings.isNullOrEmpty(contactType)) {

                    CodeableConceptBuilder codeableConceptbuilder
                            = new CodeableConceptBuilder(encounterBuilder, encounterBuilder.TAG_SOURCE);
                    codeableConceptbuilder.setCodingCode(contactTypeCell.getString());
                    codeableConceptbuilder.setCodingDisplay(contactType);
                    codeableConceptbuilder.setText(contactType);
                }
            }
        }

        CsvCell visitOrg = parser.getIDOrganisation();
        if (!visitOrg.isEmpty()) {
            //TODO: these are practice ODS codes NOT linked references
            Reference orgReference = csvHelper.createOrganisationReference(visitOrg);
            encounterBuilder.setServiceProvider(orgReference, visitOrg);
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

        //carry over linked items from any previous instance of this encounter.
        ReferenceList previousReferences = csvHelper.findConsultationPreviousLinkedResources(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(previousReferences);

        //apply any new linked items from this extract. Encounter links set-up in Codes/Referral/Medication etc. pre-transformers
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(newLinkedResources);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }
}
