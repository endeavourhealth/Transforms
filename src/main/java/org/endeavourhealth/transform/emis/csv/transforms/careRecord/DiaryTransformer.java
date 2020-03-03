package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.core.exceptions.RecordNotFoundException;
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

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

       AbstractCsvParser parser = parsers.get(Diary.class);
      while (parser != null && parser.nextRecord()) {

            try {
                createResource((Diary) parser, fhirResourceFiler, csvHelper);
            } catch (RecordNotFoundException ex) {
                String codeIdString= ex.getMessage();
                String errorRecClsName = Thread.currentThread().getStackTrace()[1].getClassName();
                codeIdString = codeIdString.contains(":") ? codeIdString.split(":")[1] :codeIdString;
                System.out.println("CodeIdStringValue " + codeIdString);
                csvHelper.logErrorRecord(Long.parseLong(codeIdString),((Diary) parser).getPatientGuid(),((Diary) parser).getDiaryGuid(),errorRecClsName);
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(Diary parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper) throws Exception {

        ProcedureRequestBuilder procedureRequestBuilder = new ProcedureRequestBuilder();

        CsvCell diaryGuid = parser.getDiaryGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(procedureRequestBuilder, patientGuid, diaryGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        procedureRequestBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            procedureRequestBuilder.setDeletedAudit(deletedCell);
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
        if (parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
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


}
