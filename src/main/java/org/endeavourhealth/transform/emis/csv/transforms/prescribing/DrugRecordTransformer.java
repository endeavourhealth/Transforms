package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.*;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.DrugRecord;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class DrugRecordTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DrugRecordTransformer.class);

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

        CsvCell isActiveCell = parser.getIsActive();
        if (isActiveCell.getBoolean()) {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE, isActiveCell);

        } else {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED, isActiveCell);
        }

        CsvCell cancellationDateCell = parser.getCancellationDate();
        if (!cancellationDateCell.isEmpty()) {
            medicationStatementBuilder.setCancellationDate(cancellationDateCell.getDate(), cancellationDateCell);
        }

        //adding validation to detect if we receive inconsistent end dates
        boolean isActive = isActiveCell.getBoolean();
        Date cancellationDate = cancellationDateCell.getDate();
        if (isActive) {
            //if active, we don't expect a cancellation date (or don't expect a past one anyway)
            if (cancellationDate != null
                    && !cancellationDate.after(new Date())) {
                TransformWarnings.log(LOG, fhirResourceFiler, "Emis DrugRecord is active (is_active = {}) but has cancellation date {}", isActiveCell, cancellationDateCell);
            }

        } else {
            //if not active, we always expect some kind of cancellation date
            if (cancellationDate == null) {
                TransformWarnings.log(LOG, fhirResourceFiler, "Emis DrugRecord is NOT active (is_active = {}) but has cancellation date {}", isActiveCell, cancellationDateCell);
            }
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