package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationOrderBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRPrimaryCareMedication;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SRPrimaryCareMedicationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPrimaryCareMedicationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPrimaryCareMedication.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRPrimaryCareMedication) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRPrimaryCareMedication parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        // Is it a medication statement or an order (issue?)
        CsvCell isRepeatMedication = parser.getIsRepeatMedication();
        CsvCell isOtherMedication = parser.getIsOtherMedication();
        CsvCell isHospitalMedication = parser.getIsHospitalMedication();
        CsvCell isDentalMedication = parser.getIsDentalMedication();

        if (!isRepeatMedication.getBoolean()) {

            // create a medication statement for an acute
            createMedicationStatement(parser, fhirResourceFiler, csvHelper);

            if (!isDentalMedication.getBoolean() &&
                    !isHospitalMedication.getBoolean() &&
                    !isOtherMedication.getBoolean()) {

                // create an order (issue) record for non dental, hospital and other acutes
                createMedicationOrder(parser, fhirResourceFiler, csvHelper);
            }
        } else {

            // create a repeat order record.
            // the initial medication statement will have been created by the repeat template transform
            createMedicationOrder(parser, fhirResourceFiler, csvHelper);
        }
    }

    private static void createMedicationStatement(SRPrimaryCareMedication parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper) throws Exception {

        CsvCell medicationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            MedicationStatement medicationStatement = (MedicationStatement) csvHelper.retrieveResource(medicationId.getString(), ResourceType.MedicationStatement);
            if (medicationStatement != null) {
                MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder(medicationStatement);
                medicationStatementBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, medicationStatementBuilder);
            }
            return;
        }

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();
        medicationStatementBuilder.setId(medicationId.getString(), medicationId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        medicationStatementBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecorded = parser.getDateEventRecorded();
        if (!dateRecorded.isEmpty()) {
            medicationStatementBuilder.setRecordedDate(dateRecorded.getDateTime(), dateRecorded);
        }

        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        Reference recordedByReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedByCell);
        if (recordedByReference != null) {
            medicationStatementBuilder.setRecordedBy(recordedByReference, profileIdRecordedByCell);
        }

        CsvCell staffMemberIdDoneByCell = parser.getIDDoneBy();
        CsvCell doneAtCell = parser.getIDOrganisationDoneAt();
        Reference doneByReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneByCell, doneAtCell);
        if (doneByReference != null) {
            medicationStatementBuilder.setInformationSource(doneByReference, profileIdRecordedByCell, doneAtCell);
        }

        //TPP acute medication can be future-dated, so use the start date as the clinically relevant date, rather than
        //the date it was prescribed (DateEvent)
        CsvCell startDateCell = parser.getDateMedicationStart();
        if (!startDateCell.isEmpty()) {
            Date startDate = startDateCell.getDate();
            medicationStatementBuilder.setAssertedDate(new DateTimeType(startDate), startDateCell);
        }

        //with TPP acute medication, the end date is always present, being the expected end date (in the future).
        //We can safely carry that end date over to the MedicationStatement, but CANNOT safely infer the active status
        //from it, because no further update is typically sent by TPP when this ends, so there's nothing to trigger
        //the change of active state to false. Active state should be inferred from the end date.
        CsvCell endDateCell = parser.getDateMedicationEnd();
        if (!endDateCell.isEmpty()) {
            medicationStatementBuilder.setCancellationDate(endDateCell.getDate(), endDateCell);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationStatementBuilder, CodeableConceptBuilder.Tag.Medication_Statement_Drug_Code);

        CsvCell dmdId = parser.getIDMultiLexDMD();
        CsvCell term = parser.getNameOfMedication();
        if (!dmdId.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(dmdId.getString(), dmdId);
            if (!term.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            }
        }

        // the item may not be coded, but has a rubric, so set as text
        if (!term.isEmpty()) {
            codeableConceptBuilder.setText(term.getString(), term);
        }

        // quantity is both value and units
        CsvCell quantity = parser.getMedicationQuantity();
        if (!quantity.isEmpty()) {
            Pattern pattern = Pattern.compile("\\d+");
            Matcher match = pattern.matcher(quantity.getString());
            if (match.find()) {

                String qty = match.group();
                String units = quantity.getString().substring(match.end(), quantity.getString().length()).trim();

                if (StringUtils.isNumeric(qty)) {
                    medicationStatementBuilder.setQuantityValue(Double.valueOf(qty), quantity);
                }
                medicationStatementBuilder.setQuantityUnit(units, quantity);
            }
        }

        CsvCell dose = parser.getMedicationDosage();
        if (!dose.isEmpty()) {
            medicationStatementBuilder.setDose(dose.getString(), dose);
        }

        CsvCell isRepeat = parser.getIsRepeatMedication();
        if (isRepeat.getBoolean()) {
            medicationStatementBuilder.setAuthorisationType(MedicationAuthorisationType.REPEAT, isRepeat);
        } else {
            medicationStatementBuilder.setAuthorisationType(MedicationAuthorisationType.ACUTE, isRepeat);
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId);
            medicationStatementBuilder.setEncounter(eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }

    private static void createMedicationOrder(SRPrimaryCareMedication parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              TppCsvHelper csvHelper) throws Exception {

        CsvCell medicationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            MedicationOrder medicationOrder = (MedicationOrder) csvHelper.retrieveResource(medicationId.getString(), ResourceType.MedicationOrder);
            if (medicationOrder != null) {
                MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder(medicationOrder);
                medicationOrderBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, medicationOrderBuilder);
            }
            return;
        }

        MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder();
        medicationOrderBuilder.setId(medicationId.getString(), medicationId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        medicationOrderBuilder.setPatient(patientReference, patientId);

        // set the medication statement reference link
        // in TPP, use the same medicationId for statement and order if it is an Acute
        // otherwise for a repeat order, use the previously created RepeatTemplate Id
        CsvCell repeatTemplateMedicationId = parser.getIDRepeatTemplate();
        if (!repeatTemplateMedicationId.isEmpty()) {
            String repeatLocalId = SRRepeatTemplateTransformer.REPEAT_TEMPLATE_ID_PREFIX + repeatTemplateMedicationId.getString();
            Reference statementReference = ReferenceHelper.createReference(ResourceType.MedicationStatement, repeatLocalId);
            medicationOrderBuilder.setMedicationStatementReference(statementReference, repeatTemplateMedicationId);

        } else {
            Reference statementReference = ReferenceHelper.createReference(ResourceType.MedicationStatement, medicationId.getString());
            medicationOrderBuilder.setMedicationStatementReference(statementReference, medicationId);
        }

        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        Reference recordedByReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedByCell);
        if (recordedByReference != null) {
            medicationOrderBuilder.setRecordedBy(recordedByReference, profileIdRecordedByCell);
        }

        CsvCell staffMemberIdDoneByCell = parser.getIDDoneBy();
        CsvCell orgDoneAtCell = parser.getIDOrganisationDoneAt();
        Reference doneByReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneByCell, orgDoneAtCell);
        if (doneByReference != null) {
            medicationOrderBuilder.setPrescriber(doneByReference, staffMemberIdDoneByCell, orgDoneAtCell);
        }

        CsvCell dateRecorded = parser.getDateEventRecorded();
        if (!dateRecorded.isEmpty()) {
            medicationOrderBuilder.setRecordedDate(dateRecorded.getDateTime(), dateRecorded);
        }

        //medication can be future-dated - the DateEvent would remain the same as the SREvent date, but the clinically
        //relevant date is the start of the medication, so use that
        CsvCell startDateCell = parser.getDateMedicationStart();
        if (!startDateCell.isEmpty()) {
            Date startDate = startDateCell.getDate();
            medicationOrderBuilder.setDateWritten(new DateTimeType(startDate), startDateCell);

            //although medication order does have an end date field, we use the duration instead, so calculate
            //the duration from the difference between the start and end date
            CsvCell endDateCell = parser.getDateMedicationEnd();
            if (!endDateCell.isEmpty()) {
                Date endDate = endDateCell.getDate();
                long diffMillis = Math.abs(endDate.getTime() - startDate.getTime());
                long diffDays = TimeUnit.DAYS.convert(diffMillis, TimeUnit.MILLISECONDS);
                medicationOrderBuilder.setDurationDays(new Integer((int)diffDays), startDateCell, endDateCell);
            }
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(medicationOrderBuilder, CodeableConceptBuilder.Tag.Medication_Order_Drug_Code);
        CsvCell dmdId = parser.getIDMultiLexDMD();
        CsvCell term = parser.getNameOfMedication();
        if (!dmdId.isEmpty()) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(dmdId.getString(), dmdId);
            if (!term.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(term.getString(), term);
            }
        }

        // the item may not be coded, but has a rubric, so set as text
        if (!term.isEmpty()) {
            codeableConceptBuilder.setText(term.getString(), term);
        }

        // quantity is both value and units
        CsvCell quantity = parser.getMedicationQuantity();
        if (!quantity.isEmpty()) {
            Pattern pattern = Pattern.compile("\\d+");
            Matcher match = pattern.matcher(quantity.getString());
            if (match.find()) {
                String qty = match.group();
                String units = quantity.getString().substring(match.end(), quantity.getString().length()).trim();
//                String qty = quantity.getString().substring(0, quantity.getString().indexOf(" "));
//                String units = quantity.getString().substring(quantity.getString().indexOf(" ") + 1);
                if (StringUtils.isNumeric(qty)) {
                    medicationOrderBuilder.setQuantityValue(Double.valueOf(qty), quantity);
                }
                medicationOrderBuilder.setQuantityUnit(units, quantity);
            }

        }

        CsvCell dose = parser.getMedicationDosage();
        if (!dose.isEmpty()) {

            medicationOrderBuilder.setDose(dose.getString(), dose);
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId);
            medicationOrderBuilder.setEncounter(eventReference, eventId);
        }
        // No need to set boolean as this builder was created fresh from constructor so needs to be mapped
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationOrderBuilder);
    }
}