package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Spells;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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

                    if (spellsParser.getLinestatus().getString().equalsIgnoreCase("delete")) {
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
        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getId().toString());
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);
        CsvCell actionCell = parser.getLinestatus();
        encounterBuilder.setDeletedAudit(actionCell);
        deleteChildResource(parser, fhirResourceFiler, csvHelper, version);
        fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
        return;

    }

    public static void createResources(Spells parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getId().getString());
        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
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
            condition.setId(idCell.getString());
            Reference enc = csvHelper.createEncounterReference(encounterBuilder.getResourceId(), patientReference.getId());
            condition.setEncounter(enc, primaryDiagCode);
            DateTimeType dtt = new DateTimeType(parser.getAdmissionDttm().getDateTime());
            condition.setOnset(dtt, parser.getAdmissionDttm());
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(primaryDiagCode.getString(), primaryDiagCode);
            condition.setCategory("diagnosis");
        }
        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            CsvCell primaryProcCode = parser.getPrimaryProcedureCode();
            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setId(parser.getId().getString() + "PROC", parser.getId());
            procedureBuilder.setIsPrimary(true);
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

        //the class is Inpatient - no need for that additional code below as extension deals with classification
        encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        //TODO - do we need to keep this when we now have the Encounter extensions? - NO
        // Note Bhrut uses NHS Patient classification. These are coded using an Encounter. The mapping
        // below to Fhir is suboptimal.
        // https://www.datadictionary.nhs.uk/data_dictionary/attributes/p/pati/patient_classification_de.asp
//        if (!parser.getPatientClassCode().isEmpty()) {
//            switch (parser.getPatientClassCode().getInt()) {
//                case 1:
//                case 2:
//                case 3:
//                case 4:
//                    encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT, parser.getPatientClassCode());
//                    break;
//                case 5:
//                    encounterBuilder.setClass(Encounter.EncounterClass.OTHER, parser.getPatientClassCode());
//                    break;
//                default:
//                    TransformWarnings.log(LOG, parser, "Unknown NHS Patient class found:  {} for Spells file {}", parser.getPatientClassCode(), parser.getFilePath());
//            }
//        }

        if (!parser.getPatientClassCode().isEmpty()) {
            CsvCell patientClassCode = parser.getPatientClassCode();
            CsvCell patientClass = parser.getPatientClass();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Patient_Class_Other);
            cc.setText(patientClass.getString(), patientClass);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(patientClassCode.getString(), patientClassCode);
            cc.setCodingDisplay(patientClass.getString());
        }
        if (!parser.getAdmissionSourceCode().isEmpty()) {
            CsvCell adminSourceCode = parser.getAdmissionSourceCode();
            CsvCell adminSource = parser.getAdmissionSource();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admission_Source);
            cc.setText(adminSource.getString(), adminSource);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(adminSourceCode.getString(), adminSourceCode);
            cc.setCodingDisplay(adminSource.getString(), adminSource);
        }
        if (!parser.getAdmissionMethodCode().isEmpty()) {
            CsvCell admissionMethodCode = parser.getAdmissionMethodCode();
            CsvCell admissionMethod = parser.getAdmissionMethod();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admission_Method);
            cc.setText(admissionMethod.getString(), admissionMethod);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(admissionMethodCode.getString(), admissionMethodCode);
            cc.setCodingDisplay(admissionMethod.getString(), admissionMethod);
        }
        if (!parser.getAdmissionWardCode().isEmpty()) {
            CsvCell admissionWardCode = parser.getAdmissionWardCode();
            CsvCell admissionWard = parser.getAdmissionWard();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admission_Ward);
            cc.setText(admissionWard.getString(), admissionWard);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(admissionWardCode.getString(), admissionWardCode);
            cc.setCodingDisplay(admissionWard.getString(), admissionWard);
        }
        if (!parser.getDischargeWardCode().isEmpty()) {
            CsvCell dischargeWardCode = parser.getDischargeWardCode();
            CsvCell dischargeWard = parser.getDischargeWard();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Discharge_Ward);
            cc.setText(dischargeWard.getString(), dischargeWard);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(dischargeWardCode.getString(), dischargeWardCode);
            cc.setCodingDisplay(dischargeWard.getString(), dischargeWard);
        }
        if (!parser.getDischargeMethodCode().isEmpty()) {
            CsvCell dischargeMethodCode = parser.getDischargeMethodCode();
            CsvCell dischargeMethod = parser.getDischargeMethod();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Discharge_Method);
            cc.setText(dischargeMethod.getString(), dischargeMethod);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(dischargeMethodCode.getString(), dischargeMethodCode);
            cc.setCodingDisplay(dischargeMethod.getString(), dischargeMethod);
        }
        if (!parser.getDischargeDestinationCode().isEmpty()) {
            CsvCell dischargeDestCode = parser.getDischargeDestinationCode();
            CsvCell dischargeDest = parser.getDischargeDestination();
            CodeableConceptBuilder cc = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Discharge_Destination);
            cc.setText(dischargeDest.getString(), dischargeDest);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(dischargeDestCode.getString(), dischargeDestCode);
            cc.setCodingDisplay(dischargeDest.getString(), dischargeDest);
        }
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }

    private static void deleteChildResource(Spells parser,
                                            FhirResourceFiler fhirResourceFiler,
                                            BhrutCsvHelper csvHelper,
                                            String version) throws Exception {
        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        CsvCell audit = parser.getLinestatus();
        EncounterBuilder enc = new EncounterBuilder();
        enc.setId(idCell.getString());
        enc.setDeletedAudit(audit);
        fhirResourceFiler.deletePatientResource(parser.getCurrentState(), enc);
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            CsvCell primaryDiagCode = parser.getPrimaryDiagnosisCode();
            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString());
            condition.setDeletedAudit(audit);
        }
        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setId(parser.getId().getString() + "PROC", parser.getId());
            procedureBuilder.setDeletedAudit(audit);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
        }

    }

}
