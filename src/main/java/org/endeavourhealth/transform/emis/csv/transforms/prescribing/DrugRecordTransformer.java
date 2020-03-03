package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.CodeNotFoundException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationOrderBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.*;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.DrugRecord;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DrugRecordTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DrugRecordTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(DrugRecord.class);
        while (parser != null && parser.nextRecord()) {
            try {
                createResource((DrugRecord) parser, fhirResourceFiler, csvHelper);
            } catch (CodeNotFoundException ex) {
                String errorRecClsName = Thread.currentThread().getStackTrace()[1].getClassName();
                csvHelper.logErrorRecord(ex, ((DrugRecord) parser).getPatientGuid(), ((DrugRecord) parser).getDrugRecordGuid(), errorRecClsName);
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

        CsvCell isActiveCell = parser.getIsActive();
        if (isActiveCell.getBoolean()) {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE, isActiveCell);

            //there are a very small number of DrugRecords that are active but have a cancellation date. All known
            //cases have had issues past the cancellation date, so to avoid ambiguity the cancellation date is ignored
            medicationStatementBuilder.setCancellationDate(null);

        } else {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED, isActiveCell);

            CsvCell cancellationDateCell = parser.getCancellationDate();
            if (!cancellationDateCell.isEmpty()) {
                medicationStatementBuilder.setCancellationDate(cancellationDateCell.getDate(), cancellationDateCell);
            } else {
                //there are a number of DrugRecords that are non-active but do not have a cancellation date. For consistency
                //of data, the cancellation date is inferred from the last IssueRecord of the DrugRecord
                Date inferredEndDate = calculateEndDate(parser, mostRecentDate, medicationStatementBuilder);
                medicationStatementBuilder.setCancellationDate(inferredEndDate);
            }
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

    /**
     * infers the medication end date from the last issue record
     */
    private static Date calculateEndDate(DrugRecord parser, IssueRecordIssueDate mostRecentDate, MedicationStatementBuilder medicationStatementBuilder) throws Exception {

        //if not in memory, we need to hit the DB
        if (mostRecentDate == null) {

            //work out UUID for MedicationStatement
            String sourceDrugRecordId = medicationStatementBuilder.getResourceId();
            UUID medicationStatementUuid = IdHelper.getOrCreateEdsResourceId(parser.getServiceId(), ResourceType.MedicationStatement, sourceDrugRecordId);
            Reference medicationStatementReference = ReferenceHelper.createReference(ResourceType.MedicationStatement, medicationStatementUuid.toString());

            //convert patient GUID to DDS UUID
            CsvCell patientGuid = parser.getPatientGuid();
            String sourcePatientId = EmisCsvHelper.createUniqueId(patientGuid, null);
            UUID patientUuid = IdHelper.getEdsResourceId(parser.getServiceId(), ResourceType.Patient, sourcePatientId);

            //get medication orders
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> resources = resourceDal.getResourcesByPatient(parser.getServiceId(), patientUuid, ResourceType.MedicationOrder.toString());
            for (ResourceWrapper wrapper: resources) {
                if (wrapper.isDeleted()) {
                    continue;
                }

                MedicationOrder order = (MedicationOrder)wrapper.getResource();
                MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder(order);

                Reference reference = medicationOrderBuilder.getMedicationStatementReference();
                if (reference != null
                        && ReferenceHelper.equals(reference, medicationStatementReference)) {

                    DateTimeType started = medicationOrderBuilder.getDateWritten();
                    Integer duration = medicationOrderBuilder.getDurationDays();

                    IssueRecordIssueDate obj = new IssueRecordIssueDate(started, duration);
                    if (obj.afterOrOtherIsNull(mostRecentDate)) {
                        mostRecentDate = obj;
                    }
                }
            }

            //if no issues exist for it, use the start date of the DrugRecord
            if (mostRecentDate == null) {
                Date d = parser.getEffectiveDate().getDate();
                mostRecentDate = new IssueRecordIssueDate(new DateTimeType(d), new Integer(0));
            }
        }

        Date d = mostRecentDate.getIssueDateType().getValue();

        int duration = 0;
        Integer intObj = mostRecentDate.getIssueDuration();
        if (intObj != null) {
            duration = intObj.intValue();
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.add(Calendar.DAY_OF_YEAR, duration);

        return cal.getTime();
    }


}