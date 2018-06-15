package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.PRESCRIPTIONS;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PRESCRIPTIONSTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PRESCRIPTIONSTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(PRESCRIPTIONS.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((PRESCRIPTIONS) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(PRESCRIPTIONS parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();
        CsvCell consultationId = parser.getConsultationId();
        CsvCell drugName = parser.getDrugName();

        String drugId = caseId.getString()
                + ":" + consultationId.getString()
                + ":" + drugName.getString();

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();
        medicationStatementBuilder.setId(drugId);

        CsvCell patientId = csvHelper.findCasePatient(caseId.getString());
        if (!patientId.isEmpty()) {
            medicationStatementBuilder.setPatient(csvHelper.createPatientReference(patientId));
        } else {
            TransformWarnings.log(LOG, parser, "No Patient id in record for CaseId: {},  file: {}",
                    caseId.getString(), parser.getFilePath());
            return;
        }

        CsvCell effectiveDate = csvHelper.findConsultationDateTime(consultationId.getString());
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            medicationStatementBuilder.setAssertedDate(dateTimeType, effectiveDate);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(medicationStatementBuilder, CodeableConceptBuilder.Tag.Medication_Statement_Drug_Code);

        // the drugs are not be coded, but has a name, so set as text
        if (!drugName.isEmpty()) {
            codeableConceptBuilder.setText(drugName.getString(), drugName);
        }

        // quantity and preparation (ml, gram, tablet etc.)
        CsvCell quantity = parser.getQuanity();
        if (!quantity.isEmpty()) {

            medicationStatementBuilder.setQuantityValue(quantity.getDouble(), quantity);
            CsvCell prep = parser.getPreparation();
            if (!prep.isEmpty()) {
                medicationStatementBuilder.setQuantityUnit(prep.getString(), quantity);
            }
        }

        CsvCell dose = parser.getDosage();
        if (!dose.isEmpty()) {
            medicationStatementBuilder.setDose(dose.getString(), dose);
        }

        // set consultation/encounter reference
        if (!consultationId.isEmpty()) {

            Reference consultationReference = csvHelper.createEncounterReference(consultationId.getString(), patientId.getString());
            medicationStatementBuilder.setEncounter (consultationReference, consultationId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }
}