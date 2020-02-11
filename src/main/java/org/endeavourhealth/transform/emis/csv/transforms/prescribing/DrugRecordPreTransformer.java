package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationOrderBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.IssueRecordIssueDate;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.DrugRecord;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.MedicationOrder;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DrugRecordPreTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        AbstractCsvParser parser = parsers.get(DrugRecord.class);
        while (parser != null && parser.nextRecord()) {

            try {
                processLine((DrugRecord)parser, fhirResourceFiler, csvHelper);

            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);

            } finally {
                //make sure all tasks are complete before returning out
                csvHelper.waitUntilThreadPoolIsEmpty();
            }
        }
    }


    private static void processLine(DrugRecord parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper) throws Exception {

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            return;
        }

        //if this record is linked to a problem, store this relationship in the helper
        CsvCurrentState state = parser.getCurrentState();
        CsvCell patientGuid = parser.getPatientGuid();
        CsvCell drugRecordGuid = parser.getDrugRecordGuid();
        CsvCell problemGuid = parser.getProblemObservationGuid();
        CsvCell isActiveCell = parser.getIsActive();
        CsvCell cancellationDateCell = parser.getCancellationDate();
        CsvCell effectiveDateCell = parser.getEffectiveDate();

        Task task = new Task(state, csvHelper, patientGuid, drugRecordGuid, problemGuid, isActiveCell, cancellationDateCell, effectiveDateCell);
        csvHelper.submitToThreadPool(task);
    }

    static class Task extends AbstractCsvCallable {

        private EmisCsvHelper csvHelper;
        private CsvCell patientGuid;
        private CsvCell drugRecordGuid;
        private CsvCell problemGuid;
        private CsvCell isActiveCell;
        private CsvCell cancellationDateCell;
        private CsvCell effectiveDateCell;

        public Task(CsvCurrentState parserState, EmisCsvHelper csvHelper, CsvCell patientGuid, CsvCell drugRecordGuid,
                    CsvCell problemGuid, CsvCell isActiveCell, CsvCell cancellationDateCell, CsvCell effectiveDateCell) {
            super(parserState);
            this.csvHelper = csvHelper;
            this.patientGuid = patientGuid;
            this.drugRecordGuid = drugRecordGuid;
            this.problemGuid = problemGuid;
            this.isActiveCell = isActiveCell;
            this.cancellationDateCell = cancellationDateCell;
            this.effectiveDateCell = effectiveDateCell;
        }

        @Override
        public Object call() throws Exception {

            //if this record is linked to a problem, store this relationship in the helper
            if (!problemGuid.isEmpty()) {

                csvHelper.cacheProblemRelationship(problemGuid,
                        patientGuid,
                        drugRecordGuid,
                        ResourceType.MedicationStatement);
            }

            //if this drug record is inactive but there's no cancellation date, then we need to work out
            //the cancellation date and cache it for our proper DrugRecordTransformer
            if (!isActiveCell.getBoolean()
                && cancellationDateCell.isEmpty()) {

                //see if the IssueRecordPreTransformer has already cached the a new most recent issue date for this DrugRecord
                if (!csvHelper.hasNewDrugRecordLastIssueDate(drugRecordGuid, patientGuid)) {

                    IssueRecordIssueDate lastIssueDate = null;

                    //work out UUID for MedicationStatement
                    String sourceDrugRecordId = EmisCsvHelper.createUniqueId(patientGuid, drugRecordGuid);
                    UUID medicationStatementUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.MedicationStatement, sourceDrugRecordId);
                    if (medicationStatementUuid != null) {
                        Reference medicationStatementReference = ReferenceHelper.createReference(ResourceType.MedicationStatement, medicationStatementUuid.toString());

                        //convert patient GUID to DDS UUID
                        String sourcePatientId = EmisCsvHelper.createUniqueId(patientGuid, null);
                        UUID patientUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, sourcePatientId);

                        //get medication orders
                        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
                        List<ResourceWrapper> resources = resourceDal.getResourcesByPatient(csvHelper.getServiceId(), patientUuid, ResourceType.MedicationOrder.toString());
                        for (ResourceWrapper wrapper : resources) {
                            if (wrapper.isDeleted()) {
                                continue;
                            }

                            MedicationOrder order = (MedicationOrder) wrapper.getResource();
                            MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder(order);

                            Reference reference = medicationOrderBuilder.getMedicationStatementReference();
                            if (reference != null
                                    && ReferenceHelper.equals(reference, medicationStatementReference)) {

                                DateTimeType started = medicationOrderBuilder.getDateWritten();
                                Integer duration = medicationOrderBuilder.getDurationDays();

                                IssueRecordIssueDate obj = new IssueRecordIssueDate(started, duration);
                                if (obj.afterOrOtherIsNull(lastIssueDate)) {
                                    lastIssueDate = obj;
                                }
                            }
                        }
                    }

                    //if no issues exist for it, use the start date of the DrugRecord
                    if (lastIssueDate == null) {
                        Date d = effectiveDateCell.getDate();
                        lastIssueDate = new IssueRecordIssueDate(new DateTimeType(d), new Integer(0));
                    }

                    csvHelper.cacheExistingDrugRecordDate(drugRecordGuid, patientGuid, lastIssueDate);
                }
            }

            return null;
        }
    }
}