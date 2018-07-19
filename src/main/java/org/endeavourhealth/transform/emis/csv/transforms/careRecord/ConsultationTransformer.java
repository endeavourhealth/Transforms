package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Consultation;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class ConsultationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ConsultationTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Consultation.class);
        while (parser.nextRecord()) {

            try {
                createResource((Consultation)parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(Consultation parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper,
                                      String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();

        CsvCell consultationGuid = parser.getConsultationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(encounterBuilder, patientGuid, consultationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        encounterBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted().getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
            return;
        }

        //link the consultation to our episode of care
        Reference episodeReference = csvHelper.createEpisodeReference(patientGuid);
        encounterBuilder.setEpisodeOfCare(episodeReference, patientGuid);

        //we have no status field in the source data, but will only receive completed encounters, so we can infer this
        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        CsvCell appointmentGuid = parser.getAppointmentSlotGuid();
        if (!appointmentGuid.isEmpty()) {
            Reference apptReference = csvHelper.createAppointmentReference(appointmentGuid, patientGuid);
            encounterBuilder.setAppointment(apptReference, appointmentGuid);
        }

        CsvCell clinicianUuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianUuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianUuid);
            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, clinicianUuid);
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            encounterBuilder.setRecordedBy(reference, enteredByGuid);
        }

        //in the earliest version of the extract, we only got the entered date and not time
        CsvCell dateCell = parser.getEnteredDate();
        CsvCell timeCell = null;
        if (!version.equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            timeCell = parser.getEnteredTime();
        }
        Date enteredDateTime = CsvCell.getDateTimeFromTwoCells(dateCell, timeCell);
        if (enteredDateTime != null) {
            encounterBuilder.setRecordedDate(enteredDateTime, dateCell, timeCell);
        }

        CsvCell effectiveDateCell = parser.getEffectiveDate();
        CsvCell precisionCell = parser.getEffectiveDatePrecision();
        DateTimeType effectiveDate = EmisDateTimeHelper.createDateTimeType(effectiveDateCell, precisionCell);
        if (effectiveDate != null) {
            encounterBuilder.setPeriodStart(effectiveDate, effectiveDateCell, precisionCell);
        }

        CsvCell organisationGuid = parser.getOrganisationGuid();
        Reference organisationReference = csvHelper.createOrganisationReference(organisationGuid);
        encounterBuilder.setServiceProvider(organisationReference, organisationGuid);

        CsvCell codeId = parser.getConsultationSourceCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(encounterBuilder, false, codeId, CodeableConceptBuilder.Tag.Encounter_Source, csvHelper);

        CsvCell termCell = parser.getConsultationSourceTerm();
        if (!termCell.isEmpty()) {

            //the concept builder may be null if there was no code ID, so check and create if necessary
            if (codeableConceptBuilder == null) {
                codeableConceptBuilder = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            }
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        //since complete consultations are by far the default, only record the incomplete extension if it's not complete
        CsvCell completeCell = parser.getComplete();
        if (!completeCell.getBoolean()) {
            encounterBuilder.setIncomplete(true, completeCell);
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

        //carry over linked items from any previous instance of this consultation
        ReferenceList previousReferences = csvHelper.findConsultationPreviousLinkedResources(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(previousReferences);

        //apply any new linked items from this extract
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(newLinkedResources);

        CsvCell confidentialCell = parser.getIsConfidential();
        if (confidentialCell.getBoolean()) {
            encounterBuilder.setConfidential(true, confidentialCell);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }

}