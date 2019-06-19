package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.*;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.DrugRecord;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.Map;

public class DrugRecordTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(DrugRecord.class);
        while (parser != null && parser.nextRecord()) {

            try {
                createResource((DrugRecord)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(DrugRecord parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper) throws Exception {

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();

        CsvCell drugRecordGuid = parser.getDrugRecordGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(medicationStatementBuilder, patientGuid, drugRecordGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        medicationStatementBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            medicationStatementBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationStatementBuilder);
            return;
        }

        //need to handle mis-spelt column name in EMIS test pack
        //String clinicianGuid = parser.getClinicianUserInRoleGuid();
        CsvCell clinicianGuid = null;
        if (parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            clinicianGuid = parser.getClinicanUserInRoleGuid();
        } else {
            clinicianGuid = parser.getClinicianUserInRoleGuid();
        }

        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            medicationStatementBuilder.setInformationSource(practitionerReference, clinicianGuid);
        }

        CsvCell effectiveDateCell = parser.getEffectiveDate();
        CsvCell effectiveDatePrecisionCell = parser.getEffectiveDatePrecision();
        DateTimeType date = EmisDateTimeHelper.createDateTimeType(effectiveDateCell, effectiveDatePrecisionCell);
        if (date != null) {
            medicationStatementBuilder.setAssertedDate(date, effectiveDateCell, effectiveDatePrecisionCell);
        }

        CsvCell isActive = parser.getIsActive();
        if (isActive.getBoolean()) {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE, isActive);

        } else {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED, isActive);
        }

        CsvCell codeId = parser.getCodeId();
        EmisCodeHelper.createCodeableConcept(medicationStatementBuilder, true, codeId, CodeableConceptBuilder.Tag.Medication_Statement_Drug_Code, csvHelper);

        CsvCell dose = parser.getDosage();
        if (!dose.isEmpty()) {
            medicationStatementBuilder.setDose(dose.getString(), dose);
        }

        CsvCell quantity = parser.getQuantity();
        medicationStatementBuilder.setQuantityValue(quantity.getDouble(), quantity);

        CsvCell quantityUnit = parser.getQuantityUnit();
        medicationStatementBuilder.setQuantityUnit(quantityUnit.getString(), quantityUnit);

        CsvCell issuesAuthorised = parser.getNumberOfIssuesAuthorised();
        if (!issuesAuthorised.isEmpty()) {
            medicationStatementBuilder.setNumberIssuesAuthorised(issuesAuthorised.getInt(), issuesAuthorised);
        }

        CsvCell issuesReceived = parser.getNumberOfIssues();
        if (!issuesReceived.isEmpty()) {
            medicationStatementBuilder.setNumberIssuesIssued(issuesReceived.getInt(), issuesReceived);
        }

        //if the Medication is linked to a Problem, then use the problem's Observation as the Medication reason
        CsvCell problemObservationGuid = parser.getProblemObservationGuid();
        if (!problemObservationGuid.isEmpty()) {
            Reference conditionReference = csvHelper.createConditionReference(problemObservationGuid, patientGuid);
            medicationStatementBuilder.setReasonForUse(conditionReference, problemObservationGuid);
        }

        CsvCell cancellationDate = parser.getCancellationDate();
        if (!cancellationDate.isEmpty()) {
            medicationStatementBuilder.setCancellationDate(cancellationDate.getDate(), cancellationDate);
        }

        IssueRecordIssueDate firstIssueDate = csvHelper.getDrugRecordFirstIssueDate(drugRecordGuid, patientGuid);
        if (firstIssueDate != null) {
            medicationStatementBuilder.setFirstIssueDate(firstIssueDate.getIssueDateType(), firstIssueDate.getSourceCells());
        }

        IssueRecordIssueDate mostRecentDate = csvHelper.getDrugRecordLastIssueDate(drugRecordGuid, patientGuid);
        if (mostRecentDate != null) {
            medicationStatementBuilder.setLastIssueDate(mostRecentDate.getIssueDateType(), mostRecentDate.getSourceCells());
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            medicationStatementBuilder.setRecordedBy(reference, enteredByGuid);
        }

        //in the earliest version of the extract, we only got the entered date and not time
        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = null;
        if (!parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            enteredTime = parser.getEnteredTime();
        }
        Date enteredDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (enteredDateTime != null) {
            medicationStatementBuilder.setRecordedDate(enteredDateTime, enteredDate, enteredTime);
        }

        CsvCell authorisationType = parser.getPrescriptionType();
        if (!authorisationType.isEmpty()) {
            MedicationAuthorisationType fhirAuthorisationType = EmisMappingHelper.findMedicationAuthorisationType(authorisationType.getString());
            medicationStatementBuilder.setAuthorisationType(fhirAuthorisationType, authorisationType);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            medicationStatementBuilder.setIsConfidential(true, confidential);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }


}