package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationOrderBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRPrimaryCareMedication;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPrimaryCareMedicationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPrimaryCareMedicationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPrimaryCareMedication.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRPrimaryCareMedication)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
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
                createOrDeleteMedicationOrder(parser, fhirResourceFiler, csvHelper);
            }
        } else {

            // create a repeat order record.
            // the initial medication statement will have been created by the repeat template transform
            createOrDeleteMedicationOrder(parser, fhirResourceFiler, csvHelper);
        }
    }

    private static void createMedicationStatement(SRPrimaryCareMedication parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper) throws Exception {

        CsvCell medicationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();
        TppCsvHelper.setUniqueId(medicationStatementBuilder, patientId, medicationId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        medicationStatementBuilder.setPatient(patientReference, patientId);

        CsvCell deleteData = parser.getRemovedData();
        if (deleteData.getIntAsBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationStatementBuilder);
            return;
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            medicationStatementBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            medicationStatementBuilder.setAssertedDate(dateTimeType, effectiveDate);
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

        if (!parser.getDateMedicationEnd().isEmpty()) {
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

        CsvCell isRepeat = parser.getIsRepeatMedication();
        if (isRepeat.getBoolean()) {
            medicationStatementBuilder.setAuthorisationType(MedicationAuthorisationType.REPEAT, isRepeat);
        } else {
            medicationStatementBuilder.setAuthorisationType(MedicationAuthorisationType.ACUTE, isRepeat);
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            medicationStatementBuilder.setEncounter (eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }

    private static void createOrDeleteMedicationOrder  (SRPrimaryCareMedication parser,
                                                        FhirResourceFiler fhirResourceFiler,
                                                        TppCsvHelper csvHelper) throws Exception {

        CsvCell medicationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder();
        TppCsvHelper.setUniqueId(medicationOrderBuilder, patientId, medicationId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        medicationOrderBuilder.setPatient(patientReference, patientId);

        CsvCell deleteData = parser.getRemovedData();
        if (deleteData.getIntAsBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationOrderBuilder);
            return;
        }

        // set the medication statement reference link
        // in TPP, use the same medicationId for statement and order if it is an Acute
        // otherwise for a repeat order, use the previously created RepeatTemplate Id
        CsvCell repeatTemplateMedicationId = parser.getIDRepeatTemplate();
        if (!repeatTemplateMedicationId.isEmpty()) {

            Reference medicationStatementReference
                    = csvHelper.createMedicationStatementReference(repeatTemplateMedicationId, patientId);
            medicationOrderBuilder.setMedicationStatementReference(medicationStatementReference, repeatTemplateMedicationId);
        } else {

            Reference medicationStatementReference
                    = csvHelper.createMedicationStatementReference(medicationId, patientId);
            medicationOrderBuilder.setMedicationStatementReference(medicationStatementReference, medicationId);
        }

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
        if (!effectiveDate.isEmpty()) {

            DateTimeType date = new DateTimeType(effectiveDate.getDate());
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

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            medicationOrderBuilder.setEncounter (eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationOrderBuilder);
    }
}
