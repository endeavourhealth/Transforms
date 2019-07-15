package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.PRESCRIPTIONS;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationOrderBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.MedicationStatementBuilder;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
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
                    createResource((PRESCRIPTIONS) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(PRESCRIPTIONS parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper) throws Exception {

        //create both the medication statement and the linked order
        createMedicationStatement(parser, fhirResourceFiler, csvHelper);
        createMedicationOrder(parser, fhirResourceFiler, csvHelper);
    }

    private static void createMedicationStatement(PRESCRIPTIONS parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  AdastraCsvHelper csvHelper) throws Exception {

        CsvCell caseId = parser.getCaseId();
        CsvCell consultationId = parser.getConsultationId();

        CsvCell drugName = parser.getDrugName();
        String drugNameFirstPart = drugName.getString().substring(0, drugName.getString().indexOf(" "));
        drugNameFirstPart = drugNameFirstPart.replace("/","").trim();   //strip out slashes as used as a reference

        CsvCell quantity = parser.getQuanity();
        String drugQty = quantity.getString();

        //create a unique Id for the drug based on case : consultation : drugName + qty
        String drugId = caseId.getString()
                + ":" + consultationId.getString()
                + ":" + drugNameFirstPart.concat(drugQty);

        MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder();
        medicationStatementBuilder.setId(drugId, caseId, consultationId);

        CsvCell patientId = csvHelper.findCasePatient(caseId.getString());
        if (!patientId.isEmpty()) {

            medicationStatementBuilder.setPatient(csvHelper.createPatientReference(patientId));
        } else {
            TransformWarnings.log(LOG, parser, "No Patient Id in record for CaseId: {},  file: {}",
                    caseId.getString(), parser.getFilePath());
            return;
        }

        CsvCell effectiveDate = csvHelper.findConsultationDateTime(consultationId.getString());
        if (effectiveDate != null) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            medicationStatementBuilder.setAssertedDate(dateTimeType, effectiveDate);
        }

        //v2 userRef - get from consultation transformer
        CsvCell userRefCell = csvHelper.findConsultationUserRef(consultationId.getString());
        if (userRefCell != null) {

            Reference practitionerReference = csvHelper.createPractitionerReference(userRefCell.getString());
            medicationStatementBuilder.setInformationSource(practitionerReference, userRefCell);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(medicationStatementBuilder, CodeableConceptBuilder.Tag.Medication_Statement_Drug_Code);

        //v2 dm&d code
        CsvCell drugCode = parser.getDMDCode();
        if (drugCode != null && !drugCode.isEmpty()) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(drugCode.getString(), drugCode);
            if (!drugName.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(drugName.getString(), drugName);
            }
        }

        // the drugs are not coded in v1, but has a name, so set as text
        if (!drugName.isEmpty()) {

            codeableConceptBuilder.setText(drugName.getString(), drugName);
        }

        // quantity and preparation (ml, gram, tablet etc.)
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

        // set this as acute as we have no other medication types
        medicationStatementBuilder.setAuthorisationType(MedicationAuthorisationType.ACUTE);

        // set drug status to intended as no way of determining if active or completed
        medicationStatementBuilder.setStatus(MedicationStatement.MedicationStatementStatus.INTENDED);

        // set consultation/encounter reference
        if (!consultationId.isEmpty()) {

            Reference consultationReference = csvHelper.createEncounterReference(consultationId);
            medicationStatementBuilder.setEncounter (consultationReference, consultationId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationStatementBuilder);
    }

    private static void createMedicationOrder(PRESCRIPTIONS parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              AdastraCsvHelper csvHelper) throws Exception {

        CsvCell caseId = parser.getCaseId();
        CsvCell consultationId = parser.getConsultationId();

        CsvCell drugName = parser.getDrugName();
        String drugNameFirstPart = drugName.getString().substring(0, drugName.getString().indexOf(" "));
        drugNameFirstPart = drugNameFirstPart.replace("/","").trim();   //strip out slashes as used as a reference

        CsvCell quantity = parser.getQuanity();
        String drugQty = quantity.getString();

        //create a unique Id for the drug based on case : consultation : drugName + qty
        String drugId = caseId.getString()
                + ":" + consultationId.getString()
                + ":" + drugNameFirstPart.concat(drugQty);

        MedicationOrderBuilder medicationOrderBuilder = new MedicationOrderBuilder();
        medicationOrderBuilder.setId(drugId, caseId, consultationId);

        CsvCell patientId = csvHelper.findCasePatient(caseId.getString());
        if (!patientId.isEmpty()) {

            medicationOrderBuilder.setPatient(csvHelper.createPatientReference(patientId));
        } else {
            TransformWarnings.log(LOG, parser, "No Patient Id in record for CaseId: {},  file: {}",
                    caseId.getString(), parser.getFilePath());
            return;
        }

        // set the medication statement reference link
        // use the same medicationId for statement and order if an Acute
        Reference statementReference
                = ReferenceHelper.createReference(ResourceType.MedicationStatement, drugId);
        medicationOrderBuilder.setMedicationStatementReference(statementReference, caseId, consultationId);

        CsvCell effectiveDate = csvHelper.findConsultationDateTime(consultationId.getString());
        if (effectiveDate != null) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            medicationOrderBuilder.setDateWritten(dateTimeType, effectiveDate);
        }

        //v2 userRef - get from consultation transformer
        CsvCell userRefCell = csvHelper.findConsultationUserRef(consultationId.getString());
        if (userRefCell != null) {

            Reference practitionerReference = csvHelper.createPractitionerReference(userRefCell.getString());
            medicationOrderBuilder.setPrescriber(practitionerReference, userRefCell);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(medicationOrderBuilder, CodeableConceptBuilder.Tag.Medication_Order_Drug_Code);

        //v2 dm&d code
        CsvCell drugCode = parser.getDMDCode();
        if (drugCode != null && !drugCode.isEmpty()) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(drugCode.getString(), drugCode);
            if (!drugName.isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(drugName.getString(), drugName);
            }
        }

        // the drugs are not coded in v1, but has a name, so set as text
        if (!drugName.isEmpty()) {

            codeableConceptBuilder.setText(drugName.getString(), drugName);
        }

        // quantity and preparation (ml, gram, tablet etc.)
        if (!quantity.isEmpty()) {

            medicationOrderBuilder.setQuantityValue(quantity.getDouble(), quantity);
            CsvCell prep = parser.getPreparation();
            if (!prep.isEmpty()) {
                medicationOrderBuilder.setQuantityUnit(prep.getString(), quantity);
            }
        }

        CsvCell dose = parser.getDosage();
        if (!dose.isEmpty()) {

            medicationOrderBuilder.setDose(dose.getString(), dose);
        }

        // set consultation/encounter reference
        if (!consultationId.isEmpty()) {

            Reference consultationReference = csvHelper.createEncounterReference(consultationId);
            medicationOrderBuilder.setEncounter (consultationReference, consultationId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), medicationOrderBuilder);
    }
}
