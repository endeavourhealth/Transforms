package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationOrderBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRPrimaryCareMedication;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRRepeatTemplate;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Reference;

import java.util.Map;

public class SRRepeatTemplateTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRRepeatTemplate.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRRepeatTemplate)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRRepeatTemplate parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        // create a medication statement
        createMedicationStatement(parser, fhirResourceFiler, csvHelper);

        // TODO: save the reference so the medication order can point to it as part of SRPrimaryCareMedication transform

    }

    private static void createMedicationStatement(SRRepeatTemplate parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper) throws Exception {

        CsvCell medicationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();
        TppCsvHelper.setUniqueId(medicationStatementBuilder, patientId, medicationId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        medicationStatementBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            medicationStatementBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        DateTimeType date = EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), "YMD");
        if (date != null) {

            medicationStatementBuilder.setAssertedDate(date, effectiveDate);
        }

        CsvCell recordedById = parser.getIDProfileEnteredBy();
        if (!recordedById.isEmpty()) {

            String staffMemberId = csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                    recordedById.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            medicationStatementBuilder.setRecordedBy(staffReference, recordedById);
        }

        CsvCell doneByClinicianId = parser.getIDDoneBy();
        if (!doneByClinicianId.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(doneByClinicianId);
            medicationStatementBuilder.setInformationSource(staffReference, recordedById);
        }

        if (!parser.getDateMedicationTemplateEnd().isEmpty()) {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE);
        } else {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationStatementBuilder, null);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
        CsvCell dmdId = parser.getIDMultiLexDMD();
        if (!dmdId.isEmpty()) {
            codeableConceptBuilder.setCodingCode(dmdId.getString(), dmdId);
        }
        CsvCell term = parser.getNameOfMedication();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
        }

        // quantity is both value and units
        CsvCell quantity = parser.getMedicationQuantity();
        if (!quantity.isEmpty()) {

            String qty = quantity.getString().substring(0, quantity.getString().indexOf(" "));
            String units = quantity.getString().substring(quantity.getString().indexOf(" ")+1);

            medicationStatementBuilder.setQuantityValue(Double.valueOf(qty), quantity);
            medicationStatementBuilder.setQuantityUnit(units, quantity);
        }

        CsvCell dose = parser.getMedicationDosage();
        if (!dose.isEmpty()) {
            medicationStatementBuilder.setDose(dose.getString(), dose);
        }

        // A Repeat by default
        medicationStatementBuilder.setAuthorisationType(MedicationAuthorisationType.REPEAT);

        CsvCell numMaxIssues = parser.getMaxIssues();
        if (!numMaxIssues.isEmpty()) {
            medicationStatementBuilder.setNumberIssuesAuthorised(numMaxIssues.getInt(), numMaxIssues);
        }


        //TODO: establish first and last issue dates

//        DateType firstIssueDate = csvHelper.getDrugRecordFirstIssueDate(drugRecordID, patientID);
//        if (firstIssueDate != null) {
//            medicationStatementBuilder.setFirstIssueDate(firstIssueDate); //, firstIssueDate.getSourceCells());
//        }
//
//        DateType mostRecentDate = csvHelper.getDrugRecordLastIssueDate(drugRecordID, patientID);
//        if (mostRecentDate != null) {
//            medicationStatementBuilder.setLastIssueDate(mostRecentDate); //, mostRecentDate.getSourceCells());
//        }

//        if (!Strings.isNullOrEmpty(consultationID)) {
//            Reference reference = csvHelper.createEncounterReference(consultationID, patientID.getString());
//            medicationStatementBuilder.setEncounter(reference, parser.getLinks());
//        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }

    private static void createOrDeleteMedicationOrder  (SRPrimaryCareMedication parser,
                                                        FhirResourceFiler fhirResourceFiler,
                                                        TppCsvHelper csvHelper) throws Exception {

        CsvCell medicationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder();
        TppCsvHelper.setUniqueId(medicationOrderBuilder, patientId, medicationId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        medicationOrderBuilder.setPatient(patientReference, patientId);

        // set the medication statement reference link - in TPP, use the same medicationId for statement and order
        //TODO: if it is a repeat, the stement is the reference to the RepeatTemplate
        Reference medicationStatementReference = csvHelper.createMedicationStatementReference(medicationId, patientId);
        medicationOrderBuilder.setMedicationStatementReference(medicationStatementReference, medicationId);

        CsvCell doneByClinicianId = parser.getIDDoneBy();
        if (!doneByClinicianId.isEmpty()) {

            medicationOrderBuilder.setPrescriber(csvHelper.createPractitionerReference(doneByClinicianId.getString()));
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            medicationOrderBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell recordedById = parser.getIDProfileEnteredBy();
        if (!recordedById.isEmpty()) {

            String staffMemberId = csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                    recordedById.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            medicationOrderBuilder.setRecordedBy(staffReference, recordedById);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        DateTimeType date = EmisDateTimeHelper.createDateTimeType(effectiveDate.getDate(), "YMD");
        if (date != null) {

            medicationOrderBuilder.setDateWritten(date, effectiveDate);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationOrderBuilder, null);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
        CsvCell dmdId = parser.getIDMultiLexDMD();
        if (!dmdId.isEmpty()) {
            codeableConceptBuilder.setCodingCode(dmdId.getString(), dmdId);
        }
        CsvCell term = parser.getNameOfMedication();
        if (!term.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(term.getString(), term);
        }

        // quantity is both value and units
        CsvCell quantity = parser.getMedicationQuantity();
        if (!quantity.isEmpty()) {

            String qty = quantity.getString().substring(0, quantity.getString().indexOf(" "));
            String units = quantity.getString().substring(quantity.getString().indexOf(" ")+1);

            medicationOrderBuilder.setQuantityValue(Double.valueOf(qty), quantity);
            medicationOrderBuilder.setQuantityUnit(units, quantity);
        }


        CsvCell dose = parser.getMedicationDosage();
        if (!dose.isEmpty()) {
            medicationOrderBuilder.setDose(dose.getString(), dose);
        }


//        if (!Strings.isNullOrEmpty(consultationID)) {
//            Reference reference = csvHelper.createEncounterReference(consultationID, patientID.getString());
//            medicationOrderBuilder.setEncounter(reference, parser.getLinks());
//        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationOrderBuilder);
    }
}
