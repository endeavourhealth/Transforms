package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureRequestBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Diary;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ProcedureRequest;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.Map;

public class DiaryTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Diary.class);
        while (parser.nextRecord()) {

            try {
                createResource((Diary)parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(Diary parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper,
                                      String version) throws Exception {

        ProcedureRequestBuilder procedureRequestBuilder = new ProcedureRequestBuilder();

        CsvCell diaryGuid = parser.getDiaryGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(procedureRequestBuilder, patientGuid, diaryGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        procedureRequestBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureRequestBuilder);
            return;
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(procedureRequestBuilder, false, codeId, CodeableConceptBuilder.Tag.Procedure_Request_Main_Code, csvHelper);

        //this MUST be done after doing the codeId as this will potentially overwrite the codeable concept text the above sets
        CsvCell termCell = parser.getOriginalTerm();
        if (!termCell.isEmpty()) {
            if (codeableConceptBuilder == null) {
                codeableConceptBuilder = new CodeableConceptBuilder(procedureRequestBuilder, CodeableConceptBuilder.Tag.Procedure_Request_Main_Code);
            }
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        CsvCell effectiveDateCell = parser.getEffectiveDate();
        CsvCell precisionCell = parser.getEffectiveDatePrecision();
        DateTimeType effectiveDate = EmisDateTimeHelper.createDateTimeType(effectiveDateCell, precisionCell);
        if (effectiveDate != null) {
            procedureRequestBuilder.setScheduledDate(effectiveDate, effectiveDateCell, precisionCell);
        }

        CsvCell freeTextDuration = parser.getDurationTerm();
        if (!freeTextDuration.isEmpty()) {
            procedureRequestBuilder.setScheduleFreeText(freeTextDuration.getString(), freeTextDuration);
        }

        //handle mis-spelt column in EMIS test pack
        //String clinicianGuid = diaryParser.getClinicianUserInRoleGuid();
        CsvCell clinicianGuid = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            clinicianGuid = parser.getClinicanUserInRoleGuid();
        } else {
            clinicianGuid = parser.getClinicianUserInRoleGuid();
        }

        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            procedureRequestBuilder.setPerformer(practitionerReference, clinicianGuid);
        }

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            procedureRequestBuilder.addNotes(associatedText.getString(), associatedText);
        }

        CsvCell locationTypeDescription = parser.getLocationTypeDescription();
        if (!locationTypeDescription.isEmpty()) {
            procedureRequestBuilder.setLocationTypeDesc(locationTypeDescription.getString(), locationTypeDescription);
        }

        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = parser.getEnteredTime();
        Date enteredDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (enteredDateTime != null) {
            procedureRequestBuilder.setRecordedDateTime(enteredDateTime, enteredDate, enteredTime);
        }

        CsvCell enterdByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enterdByGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(enterdByGuid);
            procedureRequestBuilder.setRecordedBy(practitionerReference, enterdByGuid);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference encounterReference = csvHelper.createEncounterReference(consultationGuid, patientGuid);
            procedureRequestBuilder.setEncounter(encounterReference, consultationGuid);
        }

        CsvCell isComplete = parser.getIsComplete();
        CsvCell isActive = parser.getIsActive();
        if (isComplete.getBoolean()) {
            procedureRequestBuilder.setStatus(ProcedureRequest.ProcedureRequestStatus.COMPLETED, isComplete);

        } else if (isActive.getBoolean()) {
            procedureRequestBuilder.setStatus(ProcedureRequest.ProcedureRequestStatus.REQUESTED, isActive);

        } else {
            //if it's neither active or complete, then leave without a status
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            procedureRequestBuilder.setIsConfidential(true, confidential);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureRequestBuilder);
    }


    /*public static void createResource(Diary parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version) throws Exception {

        ProcedureRequest fhirRequest = new ProcedureRequest();
        fhirRequest.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PROCEDURE_REQUEST));

        String diaryGuid = parser.getDiaryGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirRequest, patientGuid, diaryGuid);

        fhirRequest.setSubject(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirRequest);
            return;
        }

        Long codeId = parser.getCodeId();
        fhirRequest.setCode(csvHelper.findClinicalCode(codeId));

        String originalTerm = parser.getOriginalTerm();
        if (!Strings.isNullOrEmpty(originalTerm)) {
            CodeableConcept fhirConcept = fhirRequest.getCode();
            fhirConcept.setText(originalTerm);
        }

        Date effectiveDate = parser.getEffectiveDate();
        if (effectiveDate != null) {
            String effectiveDatePrecision = parser.getEffectiveDatePrecision();
            fhirRequest.setScheduled(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));
        }

        String freeTextDuration = parser.getDurationTerm();
        if (!Strings.isNullOrEmpty(freeTextDuration)) {
            fhirRequest.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PROCEDURE_REQUEST_SCHEDULE_TEXT, new StringType(freeTextDuration)));
        }

        //handle mis-spelt column in EMIS test pack
        //String clinicianGuid = diaryParser.getClinicianUserInRoleGuid();
        String clinicianGuid = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            clinicianGuid = parser.getClinicanUserInRoleGuid();
        } else {
            clinicianGuid = parser.getClinicianUserInRoleGuid();
        }

        if (!Strings.isNullOrEmpty(clinicianGuid)) {
            fhirRequest.setPerformer(csvHelper.createPractitionerReference(clinicianGuid));
        }

        String associatedText = parser.getAssociatedText();
        fhirRequest.addNotes(AnnotationHelper.createAnnotation(associatedText));

        String locationTypeDescription = parser.getLocationTypeDescription();
        if (!Strings.isNullOrEmpty(locationTypeDescription)) {
            fhirRequest.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PROCEDURE_REQUEST_LOCATION_DESCRIPTION, new StringType(locationTypeDescription)));
        }

        Date entererdDateTime = parser.getEnteredDateTime();
        if (entererdDateTime != null) {
            fhirRequest.setOrderedOn(entererdDateTime);
        }

        String enterdByGuid = parser.getEnteredByUserInRoleGuid();
        if (!Strings.isNullOrEmpty(enterdByGuid)) {
            fhirRequest.setOrderer(csvHelper.createPractitionerReference(enterdByGuid));
        }

        String consultationGuid = parser.getConsultationGuid();
        if (!Strings.isNullOrEmpty(consultationGuid)) {
            fhirRequest.setEncounter(csvHelper.createEncounterReference(consultationGuid, patientGuid));
        }

        if (parser.getIsComplete()) {
            fhirRequest.setStatus(ProcedureRequest.ProcedureRequestStatus.COMPLETED);
        } else if (parser.getIsActive()) {
            fhirRequest.setStatus(ProcedureRequest.ProcedureRequestStatus.REQUESTED);
        } else {
            fhirRequest.setStatus(ProcedureRequest.ProcedureRequestStatus.SUSPENDED);
        }

        if (parser.getIsConfidential()) {
            fhirRequest.addExtension(ExtensionConverter.createBooleanExtension(FhirExtensionUri.IS_CONFIDENTIAL, true));
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirRequest);
    }*/
}
