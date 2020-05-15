package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Spells;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

//import static org.hl7.fhir.instance.model.ResourceType.Encounter;

public class SpellsTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(SpellsTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Spells.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    Spells spellsParser = (Spells) parser;

                    if (spellsParser.getLinestatus().equals("delete")) {
                        deleteResource(spellsParser, fhirResourceFiler, csvHelper, version);
                    } else {
                        createResources(spellsParser, fhirResourceFiler, csvHelper, version);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void deleteResource(Spells parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {
        //TODO delete code.
        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getId().toString());
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);
        fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
    }

    public static void createResources(Spells parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getId().getString());
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);
        Reference encounterReference = csvHelper.createEncounterReference(parser.getId().getString(), patientIdCell.getString());

        encounterBuilder.setPeriodStart(parser.getAdmissionDttm().getDateTime(), parser.getAdmissionDttm());
        encounterBuilder.setPeriodEnd(parser.getDischargeDttm().getDateTime(), parser.getDischargeDttm());
        CsvCell org = parser.getAdmissionHospitalCode();
        Reference orgReference = csvHelper.createOrganisationReference(org.getString());
        encounterBuilder.setServiceProvider(orgReference);
        CsvCell admittingConsultant = parser.getAdmissionConsultantCode();
        Reference practitioner = csvHelper.createPractitionerReference(admittingConsultant.getString());
        encounterBuilder.addParticipant(practitioner, EncounterParticipantType.CONSULTANT, admittingConsultant);
        CsvCell dischargeConsultant = parser.getDischargeConsultant();
        Reference discharger = csvHelper.createPractitionerReference(dischargeConsultant.getString());
        encounterBuilder.addParticipant(discharger, EncounterParticipantType.CONSULTANT, dischargeConsultant);
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            CsvCell primaryDiagCode = parser.getPrimaryDiagnosisCode();
            ConditionBuilder condition = new ConditionBuilder();
            Reference enc = csvHelper.createEncounterReference(encounterBuilder.getResourceId(), patientReference.getId());
            condition.setEncounter(enc, primaryDiagCode);
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(primaryDiagCode.getString(), primaryDiagCode);
            condition.setCategory("diagnosis");
        }
        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            CsvCell primaryProcCode = parser.getPrimaryProcedureCode();
            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setEncounter(encounterReference);
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(primaryProcCode.getString(), primaryProcCode);
            //TODO map to IM?
            if (!parser.getPrimaryProcedure().isEmpty()) {
                codeableConceptBuilder.setCodingDisplay(parser.getPrimaryProcedure().getString()); //don't pass in a cell as this was derived
                codeableConceptBuilder.setText(parser.getPrimaryProcedure().getString());
            }
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
        }
        //TODO How do we map these to EncounterClass?
        // Note Bhrut uses NHS Patient classification
        // https://www.datadictionary.nhs.uk/data_dictionary/attributes/p/pati/patient_classification_de.asp
        // The patient class code values don't map happily. I've used the same as ENCNTTransformer.
        if (!parser.getPatientClassCode().isEmpty()) {
            switch (parser.getPatientClassCode().getInt()) {
                case 1:
                case 2:
                case 3:
                case 4:
                    encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT, parser.getPatientClassCode());
                    break;
                case 5:
                    encounterBuilder.setClass(Encounter.EncounterClass.OTHER, parser.getPatientClassCode());
                    break;
                default:
                    TransformWarnings.log(LOG, parser, "Unknown NHS Patient class found:  {} for Spells file {}", parser.getPatientClassCode(), parser.getFilePath());
            }

        }


            fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
        }


}
