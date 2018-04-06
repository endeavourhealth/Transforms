package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
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
        encounterBuilder.addEpisodeOfCare(episodeReference, patientGuid);

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
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(encounterBuilder, false, codeId, EncounterBuilder.TAG_SOURCE, csvHelper);

        CsvCell termCell = parser.getConsultationSourceTerm();
        if (!termCell.isEmpty()) {
            String term = termCell.getString();
            if (codeableConceptBuilder == null) {
                codeableConceptBuilder = new CodeableConceptBuilder(encounterBuilder, null);
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

    /*public static void createResource(Consultation parser,
                                        FhirResourceFiler fhirResourceFiler,
                                        EmisCsvHelper csvHelper,
                                        String version) throws Exception {

        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ENCOUNTER));

        String consultationGuid = parser.getConsultationGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirEncounter, patientGuid, consultationGuid);

        fhirEncounter.setPatient(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirEncounter);
            return;
        }

        //link the consultation to our episode of care
        Reference episodeReference = csvHelper.createEpisodeReference(patientGuid);
        fhirEncounter.addEpisodeOfCare(episodeReference);

        fhirEncounter.setStatus(Encounter.EncounterState.FINISHED);

        String appointmentGuid = parser.getAppointmentSlotGuid();
        if (!Strings.isNullOrEmpty(appointmentGuid)) {
            fhirEncounter.setAppointment(csvHelper.createAppointmentReference(appointmentGuid, patientGuid));
        }

        String clinicianUuid = parser.getClinicianUserInRoleGuid();
        if (!Strings.isNullOrEmpty(clinicianUuid)) {
            Encounter.EncounterParticipantComponent fhirParticipant = fhirEncounter.addParticipant();
            fhirParticipant.addType(CodeableConceptHelper.createCodeableConcept(EncounterParticipantType.PRIMARY_PERFORMER));
            fhirParticipant.setIndividual(csvHelper.createPractitionerReference(clinicianUuid));
        }

        String enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!Strings.isNullOrEmpty(enteredByGuid)) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_BY, reference));
        }

        //in the earliest version of the extract, we only got the entered date and not time
        Date enteredDateTime = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            enteredDateTime = parser.getEnteredDate();
        } else {
            enteredDateTime = parser.getEnteredDateTime();
        }

        if (enteredDateTime != null) {
            fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
        }

        Date date = parser.getEffectiveDate();
        String precision = parser.getEffectiveDatePrecision();
        Period fhirPeriod = createPeriod(date, precision);
        if (fhirPeriod != null) {
            fhirEncounter.setPeriod(fhirPeriod);
        }

        String organisationGuid = parser.getOrganisationGuid();
        fhirEncounter.setServiceProvider(csvHelper.createOrganisationReference(organisationGuid));

        Long codeId = parser.getConsultationSourceCodeId();
        String term = parser.getConsultationSourceTerm();
        if (codeId != null || !Strings.isNullOrEmpty(term)) {

            CodeableConcept fhirCodeableConcept = null;
            if (codeId != null) {
                fhirCodeableConcept = csvHelper.findClinicalCode(codeId);
            }
            if (!Strings.isNullOrEmpty(term)) {
                if (fhirCodeableConcept == null) {
                    fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(term);
                } else {
                    fhirCodeableConcept.setText(term);
                }
            }

            fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ENCOUNTER_SOURCE, fhirCodeableConcept));
        }

        //since complete consultations are by far the default, only record the incomplete extension if it's not complete
        if (!parser.getComplete()) {
            BooleanType b = new BooleanType(false);
            fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ENCOUNTER_INCOMPLETE, b));
        }

        //carry over linked items from any previous instance of this problem
        //use the cache in the CSV helper now, rather than hitting the DB
        //List<Reference> previousReferences = csvHelper.findPreviousLinkedReferences(fhirResourceFiler, fhirEncounter.getId(), ResourceType.Encounter);
        List<String> previousReferences = csvHelper.findConsultationPreviousLinkedResources(fhirEncounter.getId());

        if (previousReferences != null && !previousReferences.isEmpty()) {
            List<Reference> references = ReferenceHelper.createReferences(previousReferences);
            csvHelper.addLinkedItemsToResource(fhirEncounter, references, FhirExtensionUri.ENCOUNTER_COMPONENTS);
        }

        //apply any linked items from this extract
        List<String> linkedResources = csvHelper.getAndRemoveConsultationRelationships(consultationGuid, patientGuid);
        if (linkedResources != null) {
            List<Reference> references = ReferenceHelper.createReferences(linkedResources);
            csvHelper.addLinkedItemsToResource(fhirEncounter, references, FhirExtensionUri.ENCOUNTER_COMPONENTS);
        }

        if (parser.getIsConfidential()) {
            fhirEncounter.addExtension(ExtensionConverter.createBooleanExtension(FhirExtensionUri.IS_CONFIDENTIAL, true));
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirEncounter);
    }


    private static Period createPeriod(Date date, String precision) throws Exception {
        if (date == null) {
            return null;
        }

        VocDatePart vocPrecision = VocDatePart.fromValue(precision);
        if (vocPrecision == null) {
            throw new IllegalArgumentException("Unsupported consultation precision [" + precision + "]");
        }

        Period fhirPeriod = new Period();
        switch (vocPrecision) {
            case U:
                return null;
            case Y:
                fhirPeriod.setStartElement(new DateTimeType(date, TemporalPrecisionEnum.YEAR));
                break;
            case YM:
                fhirPeriod.setStartElement(new DateTimeType(date, TemporalPrecisionEnum.MONTH));
                break;
            case YMD:
                fhirPeriod.setStartElement(new DateTimeType(date, TemporalPrecisionEnum.DAY));
                break;
            case YMDT:
                fhirPeriod.setStartElement(new DateTimeType(date, TemporalPrecisionEnum.SECOND));
                break;
            default:
                throw new IllegalArgumentException("Unexpected date precision " + vocPrecision);
        }
        return fhirPeriod;
    }*/

}