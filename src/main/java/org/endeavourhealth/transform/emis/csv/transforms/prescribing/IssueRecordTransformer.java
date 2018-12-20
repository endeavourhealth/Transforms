package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationOrderBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.IssueRecordIssueDate;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.IssueRecord;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class IssueRecordTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(IssueRecordTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(IssueRecord.class);
        while (parser.nextRecord()) {

            try {
                createResource((IssueRecord)parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(IssueRecord parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper,
                                      String version) throws Exception {

        MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder();

        CsvCell issueRecordGuid = parser.getIssueRecordGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(medicationOrderBuilder, patientGuid, issueRecordGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        medicationOrderBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            medicationOrderBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationOrderBuilder);
            return;
        }

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        medicationOrderBuilder.setDateWritten(dateTime, effectiveDate, effectiveDatePrecision);

        //cache the date against the drug record GUID, so we can pick it up when processing the DrugRecord CSV
        CsvCell drugRecordGuid = parser.getDrugRecordGuid();
        if (!drugRecordGuid.isEmpty()) {
            csvHelper.cacheDrugRecordDate(drugRecordGuid, patientGuid, new IssueRecordIssueDate(dateTime, effectiveDate, effectiveDatePrecision));
        }

        //need to handle mis-spelt column name in EMIS test pack
        //String clinicianGuid = parser.getClinicianUserInRoleGuid();
        CsvCell clinicianGuid = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            clinicianGuid = parser.getClinicanUserInRoleGuid();
        } else {
            clinicianGuid = parser.getClinicianUserInRoleGuid();
        }

        Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
        medicationOrderBuilder.setPrescriber(practitionerReference, clinicianGuid);

        CsvCell codeId = parser.getCodeId();
        EmisCodeHelper.createCodeableConcept(medicationOrderBuilder, true, codeId, CodeableConceptBuilder.Tag.Medication_Order_Drug_Code, csvHelper);

        CsvCell dose = parser.getDosage();
        medicationOrderBuilder.setDose(dose.getString(), dose);

        CsvCell cost = parser.getEstimatedNhsCost();
        if (!cost.isEmpty()) {
            medicationOrderBuilder.setNhsCost(cost.getDouble(), cost);
        }

        CsvCell quantity = parser.getQuantity();
        medicationOrderBuilder.setQuantityValue(quantity.getDouble(), quantity);

        CsvCell quantityUnit = parser.getQuantityUnit();
        medicationOrderBuilder.setQuantityUnit(quantityUnit.getString(), quantityUnit);

        CsvCell courseDuration = parser.getCourseDurationInDays();
        medicationOrderBuilder.setDurationDays(courseDuration.getInt(), courseDuration);

        //if the Medication is linked to a Problem, then use the problem's Observation as the Medication reason
        CsvCell problemObservationGuid = parser.getProblemObservationGuid();
        if (!problemObservationGuid.isEmpty()) {
            Reference conditionReference = csvHelper.createConditionReference(problemObservationGuid, patientGuid);
            medicationOrderBuilder.setReason(conditionReference, problemObservationGuid);
        }

        //specification states that there will always be a drug record GUID, but we've had a small number of cases
        //where this isn't the case. Emis haven't fixed this in eight months, so I'm changing the transform to handle this
        if (!drugRecordGuid.isEmpty()) {
            Reference medicationStatementReference = csvHelper.createMedicationStatementReference(drugRecordGuid, patientGuid);
            medicationOrderBuilder.setMedicationStatementReference(medicationStatementReference, drugRecordGuid);
        } else {
            TransformWarnings.log(LOG, fhirResourceFiler, "Emis IssueRecord {} has missing drugRecordGuid", issueRecordGuid);
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            medicationOrderBuilder.setRecordedBy(reference, enteredByGuid);
        }

        //in the earliest version of the extract, we only got the entered date and not time
        CsvCell enteredDate = parser.getEnteredDate();
        CsvCell enteredTime = null;
        if (!version.equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            enteredTime = parser.getEnteredTime();
        }
        Date enteredDateTime = CsvCell.getDateTimeFromTwoCells(enteredDate, enteredTime);
        if (enteredDateTime != null) {
            medicationOrderBuilder.setRecordedDate(enteredDateTime, enteredDate, enteredTime);
        }

        CsvCell isConfidential = parser.getIsConfidential();
        if (isConfidential.getBoolean()) {
            medicationOrderBuilder.setIsConfidential(true, isConfidential);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationOrderBuilder);
    }

    /*public static void createResource(IssueRecord parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version) throws Exception {

        MedicationOrder fhirMedication = new MedicationOrder();
        fhirMedication.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_MEDICATION_ORDER));

        String issueRecordGuid = parser.getIssueRecordGuid();
        String patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(fhirMedication, patientGuid, issueRecordGuid);

        fhirMedication.setPatient(csvHelper.createPatientReference(patientGuid));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirMedication);
            return;
        }

        Date effectiveDate = parser.getEffectiveDate();
        String effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        fhirMedication.setDateWrittenElement(dateTime);

        //cache the date against the drug record GUID, so we can pick it up when processing the DrugRecord CSV
        String drugRecordGuid = parser.getDrugRecordGuid();
        csvHelper.cacheDrugRecordDate(drugRecordGuid, patientGuid, dateTime);

        //need to handle mis-spelt column name in EMIS test pack
        //String clinicianGuid = parser.getClinicianUserInRoleGuid();
        String clinicianGuid = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            clinicianGuid = parser.getClinicanUserInRoleGuid();
        } else {
            clinicianGuid = parser.getClinicianUserInRoleGuid();
        }

        fhirMedication.setPrescriber(csvHelper.createPractitionerReference(clinicianGuid));

        Long codeId = parser.getCodeId();
        fhirMedication.setMedication(csvHelper.findMedication(codeId));

        String dose = parser.getDosage();
        MedicationOrder.MedicationOrderDosageInstructionComponent fhirDose = fhirMedication.addDosageInstruction();
        fhirDose.setText(dose);

        Double cost = parser.getEstimatedNhsCost();
        fhirMedication.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_ORDER_ESTIMATED_COST, new DecimalType(cost)));

        Double quantity = parser.getQuantity();
        String quantityUnit = parser.getQuantityUnit();
        Integer courseDuration = parser.getCourseDurationInDays();
        MedicationOrder.MedicationOrderDispenseRequestComponent fhirDispenseRequest = new MedicationOrder.MedicationOrderDispenseRequestComponent();
        fhirDispenseRequest.setQuantity(QuantityHelper.createSimpleQuantity(quantity, quantityUnit));
        fhirDispenseRequest.setExpectedSupplyDuration(QuantityHelper.createDuration(courseDuration, "days"));
        fhirMedication.setDispenseRequest(fhirDispenseRequest);

        //if the Medication is linked to a Problem, then use the problem's Observation as the Medication reason
        String problemObservationGuid = parser.getProblemObservationGuid();
        if (!Strings.isNullOrEmpty(problemObservationGuid)) {
            fhirMedication.setReason(csvHelper.createObservationReference(problemObservationGuid, patientGuid));
        }

        Reference authorisationReference = csvHelper.createMedicationStatementReference(drugRecordGuid, patientGuid);
        fhirMedication.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.MEDICATION_ORDER_AUTHORISATION, authorisationReference));

        String enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!Strings.isNullOrEmpty(enteredByGuid)) {
            Reference reference = csvHelper.createPractitionerReference(enteredByGuid);
            fhirMedication.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_BY, reference));
        }

        //in the earliest version of the extract, we only got the entered date and not time
        Date enteredDateTime = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            enteredDateTime = parser.getEnteredDate();
        } else {
            enteredDateTime = parser.getEnteredDateTime();
        }

        if (enteredDateTime != null) {
            fhirMedication.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
        }

        if (parser.getIsConfidential()) {
            fhirMedication.addExtension(ExtensionConverter.createBooleanExtension(FhirExtensionUri.IS_CONFIDENTIAL, true));
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirMedication);
    }*/

}
