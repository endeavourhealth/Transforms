package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationOrderBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.exceptions.EmisCodeNotFoundException;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.IssueRecord;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class IssueRecordTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(IssueRecordTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        IssueRecord parser = (IssueRecord)parsers.get(IssueRecord.class);
        while (parser != null && parser.nextRecord()) {

            try {
                if (csvHelper.shouldProcessRecord(parser)) {
                    createResource(parser, fhirResourceFiler, csvHelper);
                }

            } catch (EmisCodeNotFoundException ex) {
                csvHelper.logMissingCode(ex, parser.getPatientGuid(), parser.getIssueRecordGuid(), parser);

            } catch (Exception ex) {
                //log any record-level exception and carry on to the next record
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(IssueRecord parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper) throws Exception {

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

        CsvCell courseDurationCell = parser.getCourseDurationInDays();
        Integer courseDuration = courseDurationCell.getInt();
        medicationOrderBuilder.setDurationDays(courseDuration, courseDurationCell);

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianGuid);
            medicationOrderBuilder.setPrescriber(practitionerReference, clinicianGuid);
        }

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

        //if the Medication is linked to a Problem, then use the problem's Observation as the Medication reason
        CsvCell problemObservationGuid = parser.getProblemObservationGuid();
        if (!problemObservationGuid.isEmpty()) {
            Reference conditionReference = csvHelper.createConditionReference(problemObservationGuid, patientGuid);
            medicationOrderBuilder.setReason(conditionReference, problemObservationGuid);
        }

        //specification states that there will always be a drug record GUID, but we've had a small number of cases
        //where this isn't the case. Emis haven't fixed this in eight months, so I'm changing the transform to handle this
        CsvCell drugRecordGuid = parser.getDrugRecordGuid();
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
        if (!parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
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

}
