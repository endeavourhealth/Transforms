package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRRepeatTemplate;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SRRepeatTemplateTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRRepeatTemplateTransformer.class);

    //we transform this and primaryCareMedication into MedicationStatements so we use this prefix to distinguish
    public static final String REPEAT_TEMPLATE_ID_PREFIX = "RepeatTemplate:";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRRepeatTemplate.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRRepeatTemplate) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRRepeatTemplate parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell medicationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        String localId = REPEAT_TEMPLATE_ID_PREFIX + medicationId.getString();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            MedicationStatement medicationStatement = (MedicationStatement)csvHelper.retrieveResource(localId, ResourceType.MedicationStatement);
            if (medicationStatement != null) {
                MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder(medicationStatement);
                medicationStatementBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, medicationStatementBuilder);
            }
            return;
        }

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();
        medicationStatementBuilder.setId(localId, medicationId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        medicationStatementBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            medicationStatementBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            medicationStatementBuilder.setAssertedDate(dateTimeType, effectiveDate);
        }

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            medicationStatementBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            if (staffReference != null) {
                medicationStatementBuilder.setInformationSource(staffReference, profileIdRecordedBy);
            }
        }

        if (!parser.getDateMedicationTemplateEnd().isEmpty()) {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.ACTIVE);
        } else {
            medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.COMPLETED);
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
//                String qty = quantity.getString().substring(0, quantity.getString().indexOf(" "));
//                String units = quantity.getString().substring(quantity.getString().indexOf(" ") + 1);
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

        // A Repeat by default
        medicationStatementBuilder.setAuthorisationType(MedicationAuthorisationType.REPEAT);

        CsvCell numMaxIssues = parser.getMaxIssues();
        if (!numMaxIssues.isEmpty()) {
            medicationStatementBuilder.setNumberIssuesAuthorised(numMaxIssues.getInt(), numMaxIssues);
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {
            Reference eventReference = csvHelper.createEncounterReference(eventId);
            medicationStatementBuilder.setEncounter(eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }
}
