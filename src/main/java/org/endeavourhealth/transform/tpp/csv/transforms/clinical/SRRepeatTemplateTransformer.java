package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
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
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if (!deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.MedicationStatement medicationStatement
                        = (org.hl7.fhir.instance.model.MedicationStatement) csvHelper.retrieveResource(medicationId.getString(),
                        ResourceType.MedicationStatement,
                        fhirResourceFiler);

                if (medicationStatement != null) {
                    MedicationStatementBuilder medicationStatementBuilder
                            = new MedicationStatementBuilder(medicationStatement);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), medicationStatementBuilder);
                }
                return;

            }
        }

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();
        medicationStatementBuilder.setId(medicationId.getString(), medicationId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        medicationStatementBuilder.setPatient(patientReference, patientId);

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
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                medicationStatementBuilder.setRecordedBy(staffReference, recordedById);
            }
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

            Pattern pattern = Pattern.compile("\\d+");
            Matcher match = pattern.matcher(quantity.getString());
            if (match.find()) {
                String qty = match.group();
                String units = quantity.getString().substring(match.end(),quantity.getString().length()).trim();
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

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            medicationStatementBuilder.setEncounter (eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }
}
