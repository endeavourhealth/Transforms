package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.IssueRecordIssueDate;
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
            MedicationAuthorisationType fhirAuthorisationType = MedicationAuthorisationType.fromDescription(authorisationType.getString());
            medicationStatementBuilder.setAuthorisationType(fhirAuthorisationType, authorisationType);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            medicationStatementBuilder.setIsConfidential(true, confidential);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }


    /*public static void createResource(DrugRecord parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version) throws Exception {

        MedicationStatement fhirMedicationStatement = new MedicationStatement();
        fhirMedicationStatement.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_MEDICATION_AUTHORISATION));

        String drugRecordGuid = parser.getDrugRecordGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirMedicationStatement, patientGuid, drugRecordGuid);

        fhirMedicationStatement.setPatient(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirMedicationStatement);
            return;
        }

        //need to handle mis-spelt column name in EMIS test pack
        //String clinicianGuid = parser.getClinicianUserInRoleGuid();
        String clinicianGuid = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            clinicianGuid = parser.getClinicanUserInRoleGuid();
        } else {
            clinicianGuid = parser.getClinicianUserInRoleGuid();
        }

        fhirMedicationStatement.setInformationSource(csvHelper.createPractitionerReference(clinicianGuid));

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        fhirMedicationStatement.setDateAssertedElement(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        if (parser.getIsActive()) {
            fhirMedicationStatement.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE);
        } else {
            fhirMedicationStatement.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED);
        }

        Long codeId = parser.getCodeId();
        fhirMedicationStatement.setMedication(csvHelper.findMedication(codeId));

        String dose = parser.getDosage();
        MedicationStatement.MedicationStatementDosageComponent fhirDose = fhirMedicationStatement.addDosage();
        fhirDose.setText(dose);

        Double quantity = parser.getQuantity();
        String quantityUnit = parser.getQuantityUnit();
        Quantity fhirQuantity = new Quantity();
        fhirQuantity.setValue(BigDecimal.valueOf(quantity.doubleValue()));
        fhirQuantity.setUnit(quantityUnit);
        fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_QUANTITY, fhirQuantity));

        Integer issuesAuthorised = parser.getNumberOfIssuesAuthorised();
        if (issuesAuthorised != null) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_NUMBER_OF_REPEATS_ALLOWED, new PositiveIntType(issuesAuthorised)));
        }

        Integer issuesReceived = parser.getNumberOfIssues();
        if (issuesReceived != null) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_NUMBER_OF_REPEATS_ISSUED, new PositiveIntType(issuesReceived)));
        }

        //if the Medication is linked to a Problem, then use the problem's Observation as the Medication reason
        String problemObservationGuid = parser.getProblemObservationGuid();
        if (!Strings.isNullOrEmpty(problemObservationGuid)) {
            fhirMedicationStatement.setReasonForUse(csvHelper.createConditionReference(problemObservationGuid, patientGuid));
        }

        Date cancellationDate = parser.getCancellationDate();
        if (cancellationDate != null) {
            //the cancellation extension is a compound extension, so we have one extension inside another
            Extension extension = ExtensionConverter.createExtension("date", new DateType(cancellationDate));
            fhirMedicationStatement.addExtension(ExtensionConverter.createCompoundExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_CANCELLATION, extension));
        }

        DateType firstIssueDate = csvHelper.getDrugRecordFirstIssueDate(drugRecordGuid, patientGuid);
        if (firstIssueDate != null) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_FIRST_ISSUE_DATE, firstIssueDate));
        }

        DateType mostRecentDate = csvHelper.getDrugRecordLastIssueDate(drugRecordGuid, patientGuid);
        if (mostRecentDate != null) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_MOST_RECENT_ISSUE_DATE, mostRecentDate));
        }

        String enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!Strings.isNullOrEmpty(enteredByGuid)) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_BY, reference));
        }

        //in the earliest version of the extract, we only got the entered date and not time
        Date enteredDateTime = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            enteredDateTime = parser.getEnteredDate();
        } else {
            enteredDateTime = parser.getEnteredDateTime();
        }

        if (enteredDateTime != null) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
        }

        String authorisationType = parser.getPrescriptionType();
        MedicationAuthorisationType fhirAuthorisationType = MedicationAuthorisationType.fromDescription(authorisationType);
        Coding fhirCoding = CodingHelper.createCoding(fhirAuthorisationType);
        fhirMedicationStatement.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_AUTHORISATION_TYPE, fhirCoding));

        if (parser.getIsConfidential()) {
            fhirMedicationStatement.addExtension(ExtensionConverter.createBooleanExtension(FhirExtensionUri.IS_CONFIDENTIAL, true));
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirMedicationStatement);
    }*/

}