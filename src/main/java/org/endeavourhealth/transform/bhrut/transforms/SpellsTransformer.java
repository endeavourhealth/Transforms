package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Spells;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
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

        //delete the encounter
        fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);

        //then, delete the linked resources
        deleteChildResources(parser, fhirResourceFiler, csvHelper, version);
    }

    public static void createResources(Spells parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        CsvCell idCell = parser.getId();

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(idCell.getString());

        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);
        encounterBuilder.setPeriodStart(parser.getAdmissionDttm().getDateTime(), parser.getAdmissionDttm());
        encounterBuilder.setPeriodEnd(parser.getDischargeDttm().getDateTime(), parser.getDischargeDttm());

        //CsvCell org = parser.getAdmissionHospitalCode();
        //Reference orgReference = csvHelper.createOrganisationReference(org.getString());
        //encounterBuilder.setServiceProvider(orgReference);

        CsvCell admissionHospitalCodeCell = parser.getAdmissionHospitalCode();
        if (!admissionHospitalCodeCell.isEmpty()) {
            Reference organisationReference = csvHelper.createOrganisationReference(admissionHospitalCodeCell.getString());
            if (encounterBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
            }
            encounterBuilder.setServiceProvider(organisationReference);
        }

        CsvCell admittingConsultant = parser.getAdmissionConsultantCode();
        Reference practitioner = csvHelper.createPractitionerReference(admittingConsultant.getString());
        encounterBuilder.addParticipant(practitioner, EncounterParticipantType.CONSULTANT, admittingConsultant);

        CsvCell dischargeConsultant = parser.getDischargeConsultant();
        Reference discharger = csvHelper.createPractitionerReference(dischargeConsultant.getString());
        encounterBuilder.addParticipant(discharger, EncounterParticipantType.DISCHARGER, dischargeConsultant);

        //create an Encounter reference for the procedures and conditions to use
        Reference thisEncounter = csvHelper.createEncounterReference(idCell.getString(), patientIdCell.getString());

        //Primary Diagnosis
        CsvCell primaryDiagnosisCodeCell = parser.getPrimaryDiagnosisCode();
        if (!primaryDiagnosisCodeCell.isEmpty()) {

            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0", idCell);
            conditionBuilder.setPatient(patientReference, patientIdCell);
            conditionBuilder.setEncounter(thisEncounter, idCell);
            conditionBuilder.setClinician(practitioner, admittingConsultant);
            DateTimeType dtt = new DateTimeType(parser.getAdmissionDttm().getDateTime());
            conditionBuilder.setOnset(dtt, parser.getAdmissionDttm());

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(primaryDiagnosisCodeCell.getString(), primaryDiagnosisCodeCell);
            String diagTerm = TerminologyService.lookupIcd10CodeDescription(primaryDiagnosisCodeCell.getString());
            if (Strings.isNullOrEmpty(diagTerm)) {
                throw new Exception("Failed to find diagnosis term for ICD 10 code " + primaryDiagnosisCodeCell.getString());
            }
            codeableConceptBuilder.setCodingDisplay(diagTerm);

            if (!parser.getPrimaryDiagnosis().isEmpty()) {
                codeableConceptBuilder.setText(parser.getPrimaryDiagnosis().getString());
            }
            conditionBuilder.setCategory("diagnosis");

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
        }
        //Primary Procedure
        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            CsvCell primaryProcCode = parser.getPrimaryProcedureCode();
            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setId(idCell.getString() + ":Procedure:0", idCell);
            procedureBuilder.setIsPrimary(true);
            procedureBuilder.setEncounter(thisEncounter, idCell);
            procedureBuilder.addPerformer(practitioner, admittingConsultant);
            DateTimeType dateTimeType = new DateTimeType(parser.getAdmissionDttm().getDateTime());
            procedureBuilder.setPerformed(dateTimeType, parser.getAdmissionDttm());

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
            codeableConceptBuilder.setCodingCode(primaryProcCode.getString(), primaryProcCode);
            String procTerm = TerminologyService.lookupOpcs4ProcedureName(primaryProcCode.getString());
            if (Strings.isNullOrEmpty(procTerm)) {
                throw new Exception("Failed to find procedure term for OPCS-4 code " + primaryProcCode.getString());
            }
            codeableConceptBuilder.setCodingDisplay(procTerm); //don't pass in a cell as this was derived

            if (!parser.getPrimaryProcedure().isEmpty()) {
                codeableConceptBuilder.setText(parser.getPrimaryProcedure().getString());
            }

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
        }

        //the class of Encounter is Inpatient
        encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        //set the extensions
        if (!parser.getPatientClassCode().isEmpty()) {
            CsvCell patientClassCode = parser.getPatientClassCode();
            CsvCell patientClass = parser.getPatientClass();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Patient_Class_Other);
            cc.setText(patientClass.getString(), patientClass);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(patientClassCode.getString(), patientClassCode);
            cc.setCodingDisplay(patientClass.getString());
        }
        if (!parser.getAdmissionSourceCode().isEmpty()) {
            CsvCell adminSourceCode = parser.getAdmissionSourceCode();
            CsvCell adminSource = parser.getAdmissionSource();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admission_Source);
            cc.setText(adminSource.getString(), adminSource);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(adminSourceCode.getString(), adminSourceCode);
            cc.setCodingDisplay(adminSource.getString(), adminSource);
        }
        if (!parser.getAdmissionMethodCode().isEmpty()) {
            CsvCell admissionMethodCode = parser.getAdmissionMethodCode();
            CsvCell admissionMethod = parser.getAdmissionMethod();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admission_Method);
            cc.setText(admissionMethod.getString(), admissionMethod);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(admissionMethodCode.getString(), admissionMethodCode);
            cc.setCodingDisplay(admissionMethod.getString(), admissionMethod);
        }
        if (!parser.getAdmissionWardCode().isEmpty()) {
            CsvCell admissionWardCode = parser.getAdmissionWardCode();
            CsvCell admissionWard = parser.getAdmissionWard();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admission_Ward);
            cc.setText(admissionWard.getString(), admissionWard);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(admissionWardCode.getString(), admissionWardCode);
            cc.setCodingDisplay(admissionWard.getString(), admissionWard);
        }
        if (!parser.getDischargeWardCode().isEmpty()) {
            CsvCell dischargeWardCode = parser.getDischargeWardCode();
            CsvCell dischargeWard = parser.getDischargeWard();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Discharge_Ward);
            cc.setText(dischargeWard.getString(), dischargeWard);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(dischargeWardCode.getString(), dischargeWardCode);
            cc.setCodingDisplay(dischargeWard.getString(), dischargeWard);
        }
        if (!parser.getDischargeMethodCode().isEmpty()) {
            CsvCell dischargeMethodCode = parser.getDischargeMethodCode();
            CsvCell dischargeMethod = parser.getDischargeMethod();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Discharge_Method);
            cc.setText(dischargeMethod.getString(), dischargeMethod);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(dischargeMethodCode.getString(), dischargeMethodCode);
            cc.setCodingDisplay(dischargeMethod.getString(), dischargeMethod);
        }
        if (!parser.getDischargeDestinationCode().isEmpty()) {
            CsvCell dischargeDestCode = parser.getDischargeDestinationCode();
            CsvCell dischargeDest = parser.getDischargeDestination();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Discharge_Destination);
            cc.setText(dischargeDest.getString(), dischargeDest);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(dischargeDestCode.getString(), dischargeDestCode);
            cc.setCodingDisplay(dischargeDest.getString(), dischargeDest);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }

    private static void deleteChildResources(Spells parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             BhrutCsvHelper csvHelper,
                                             String version) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        CsvCell audit = parser.getLinestatus();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {

            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0", idCell);

            conditionBuilder.setPatient(patientReference, patientIdCell);
            conditionBuilder.setDeletedAudit(audit);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
        }
        if (!parser.getPrimaryProcedureCode().isEmpty()) {

            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setId(idCell.getString() + ":Procedure:0", idCell);
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setDeletedAudit(audit);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
        }
    }
}
