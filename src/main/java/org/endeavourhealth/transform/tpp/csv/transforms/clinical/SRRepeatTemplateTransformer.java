package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRRepeatTemplate;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRRepeatTemplateTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRRepeatTemplateTransformer.class);

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

        // create a medication statement - NOTE: all medication orders are created as part of SRPrimaryCareMedication
        createMedicationStatement(parser, fhirResourceFiler, csvHelper);
    }

    private static void createMedicationStatement(SRRepeatTemplate parser,
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

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            medicationStatementBuilder.setEncounter (eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }
}
