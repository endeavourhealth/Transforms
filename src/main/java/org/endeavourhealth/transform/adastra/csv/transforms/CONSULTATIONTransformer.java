package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.CONSULTATION;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CONSULTATIONTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(CONSULTATIONTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(CONSULTATION.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((CONSULTATION) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(CONSULTATION parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell consultationId = parser.getConsultationId();

        //create Encounter Resource builder
        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(consultationId.getString(),consultationId);

        //link the Episode of care resource
        CsvCell caseId = parser.getCaseId();
        if (!caseId.isEmpty()) {

            Reference episodeOfCareReference = csvHelper.createEpisodeReference(caseId);
            encounterBuilder.setEpisodeOfCare(episodeOfCareReference);
        }

        //get cached patientId from case
        CsvCell patientId = csvHelper.findCasePatient(caseId.getString());
        if (!patientId.isEmpty()) {

            encounterBuilder.setPatient(csvHelper.createPatientReference(patientId));
        } else {
            TransformWarnings.log(LOG, parser, "No Patient id in record for CaseId: {},  file: {}",
                    caseId.getString(), parser.getFilePath());
            return;
        }

        CsvCell startDateTime = parser.getStartDateTime();
        if (!startDateTime.isEmpty()) {
            encounterBuilder.setPeriodStart(startDateTime.getDateTime(), startDateTime);

            //cache the consultation date to use with linked codes and prescriptions
            csvHelper.cacheConsultationDate(consultationId.getString(), startDateTime);
        }

        CsvCell endDateTime = parser.getEndDateTime();
        if (!endDateTime.isEmpty()) {

            encounterBuilder.setPeriodEnd(endDateTime.getDateTime(), endDateTime);
            encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            encounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CsvCell consultationCaseType = parser.getConsultationCaseType();
        if (!consultationCaseType.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText(consultationCaseType.getString(), consultationCaseType);
        }

        //v2 userRef - //TODO this is the incorrect value from Adastra, i.e. ProviderRef, should be UserRef
        CsvCell userRef = parser.getUserRef();
        if (userRef != null && !userRef.isEmpty()) {

            Reference practitionerReference = csvHelper.createPractitionerReference(userRef.getString());
            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, userRef);

            //cache the consultation user to use with linked clinical codes and prescriptions
            csvHelper.cacheConsultationUserRef(consultationId.getString(), userRef);
        }

        //v2 ODS code from Case
        CsvCell caseODSCodeCell = csvHelper.findCaseODSCode(caseId.getString());
        if (caseODSCodeCell != null && !caseODSCodeCell.isEmpty()) {

            encounterBuilder.setServiceProvider(csvHelper.createOrganisationReference(caseODSCodeCell.getString()), caseODSCodeCell);
        }

        //set the location by creating a unique location code reference to a resource already filed in the Case pre-transformer
        CsvCell locationNameCell = parser.getLocation();
        if (!locationNameCell.isEmpty()) {
            String uniqueLocationCode
                    = caseODSCodeCell.getString() + ":" + locationNameCell.getString().replaceAll(" ", "");

            encounterBuilder.addLocation(csvHelper.createLocationReference(uniqueLocationCode));
        }

        //collect free text from history, exam, diagnosis and treatment
        StringBuilder encounterTextBuilder = new StringBuilder();

        CsvCell historyText = parser.getHistory();
        if (!historyText.isEmpty()) {

            encounterTextBuilder.append(historyText.getString()+" ");
        }

        CsvCell examinationText = parser.getExamination();
        if (!examinationText.isEmpty()) {

            encounterTextBuilder.append(examinationText.getString()+" ");
        }

        CsvCell diagnosisText = parser.getDiagnosis();
        if (!diagnosisText.isEmpty()) {

            encounterTextBuilder.append(diagnosisText.getString()+" ");
        }

        CsvCell treatmentPlanText = parser.getTreatmentPlan();
        if (!treatmentPlanText.isEmpty()) {

            encounterTextBuilder.append(treatmentPlanText.getString()+" ");
        }

        ObservationBuilder observationBuilder = null;
        if (encounterTextBuilder.length() > 0) {

            //create a linked free text observation from the encounter free text
            observationBuilder = new ObservationBuilder();

            //create a unique observation Id
            String observationId = caseId.getString()
                    + ":" + consultationId.getString();
            observationBuilder.setId(observationId, caseId, consultationId);

            DateTimeType dateTimeType = new DateTimeType(startDateTime.getDateTime());
            observationBuilder.setEffectiveDate(dateTimeType, startDateTime);
            observationBuilder.setPatient(csvHelper.createPatientReference(patientId));
            observationBuilder.setEncounter(csvHelper.createEncounterReference(consultationId));
            observationBuilder.setNotes(encounterTextBuilder.toString().trim());

            //v2 userRef - add consulting clinician to observation
            if (userRef != null && !userRef.isEmpty()) {

                Reference practitionerReference = csvHelper.createPractitionerReference(userRef.getString());
                observationBuilder.setClinician(practitionerReference, userRef);
            }

            //cache the link to this new observation
            csvHelper.cacheNewConsultationChildRelationship(consultationId,
                    observationId,
                    ResourceType.Observation);
        }

        //apply any linked items from this extract set-up in pre-transformer plus the new observation created above
        ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(newLinkedResources);

        if (observationBuilder != null) {

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder, observationBuilder);
        } else {
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
        }
    }
}
