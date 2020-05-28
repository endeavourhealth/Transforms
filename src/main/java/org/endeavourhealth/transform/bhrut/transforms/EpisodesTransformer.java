package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;

public class EpisodesTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EpisodesTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Episodes.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResources((Episodes) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResources(Episodes parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        encounterBuilder.setId(idCell.toString());
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);

        CsvCell actionCell = parser.getLinestatus();
        if (actionCell.getString().equalsIgnoreCase("Delete")) {

            encounterBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);

            deleteChildResources(parser, fhirResourceFiler, csvHelper, version);
            return;
        }

        //the class is Inpatient, i.e. Inpatient Episode
        encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
        encounterBuilder.setPeriodStart(parser.getEpisodeStartDttm().getDateTime(), parser.getEpisodeStartDttm());
        encounterBuilder.setPeriodEnd(parser.getEpisodeEndDttm().getDateTime(), parser.getEpisodeEndDttm());

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

        CsvCell episodeConsultantCell = parser.getEpisodeConsultantCode();
        Reference episodePractitioner = csvHelper.createPractitionerReference(episodeConsultantCell.getString());
        encounterBuilder.addParticipant(episodePractitioner, EncounterParticipantType.CONSULTANT, episodeConsultantCell);

        //the parent inpatient spell encounter
        Reference spellEncounter
                = csvHelper.createEncounterReference(parser.getIpSpellExternalId().getString(), patientReference.getId());
        encounterBuilder.setPartOf(spellEncounter);

        //set the Encounter extensions
        //these are usually set on the parent spell encounter, but set them here also for completeness of record
        if (!parser.getPatientClassCode().isEmpty()) {
            CsvCell patientClassCode = parser.getPatientClassCode();
            CsvCell patientClass = parser.getPatientClass();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Patient_Class_Other);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(patientClassCode.getString(), patientClassCode);
            cc.setCodingDisplay(patientClass.getString());
            cc.setText(patientClass.getString(), patientClass);
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
        if (!parser.getEpisodeStartWardCode().isEmpty()) {
            CsvCell episodeStartWardCode = parser.getEpisodeStartWardCode();
            CsvCell episodeStartWard = parser.getEpisodeStartWard();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admission_Ward);
            cc.setText(episodeStartWard.getString(), episodeStartWard);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(episodeStartWardCode.getString(), episodeStartWardCode);
            cc.setCodingDisplay(episodeStartWard.getString(), episodeStartWard);
        }
        if (!parser.getEpisodeEndWardCode().isEmpty()) {
            CsvCell episodeEndWardCode = parser.getEpisodeEndWardCode();
            CsvCell episodeEndWard = parser.getEpisodeEndWard();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Discharge_Ward);
            cc.setText(episodeEndWard.getString(), episodeEndWard);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(episodeEndWardCode.getString(), episodeEndWardCode);
            cc.setCodingDisplay(episodeEndWard.getString(), episodeEndWard);
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

        //save the encounter resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);

        //create an Encounter reference for the procedures and diagnosis
        Reference thisEncounter
                = csvHelper.createEncounterReference(parser.getId().getString(), patientReference.getId());

        //its rare that there is no primary diagnosis, but check just in case
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {

            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString() + "Condition:0");
            condition.setPatient(patientReference, patientIdCell);
            DateTimeType dtt = new DateTimeType(parser.getPrimdiagDttm().getDateTime());
            condition.setOnset(dtt, parser.getPrimdiagDttm());
            condition.setIsPrimary(true);
            condition.setAsProblem(false);
            condition.setEncounter(thisEncounter, parser.getId());
            condition.setClinician(episodePractitioner, episodeConsultantCell);

            CodeableConceptBuilder code
                    = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
            code.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            code.setCodingCode(parser.getPrimaryDiagnosisCode().getString(), parser.getPrimaryDiagnosisCode());
            String diagTerm = TerminologyService.lookupIcd10CodeDescription(parser.getPrimaryDiagnosisCode().getString());
            if (Strings.isNullOrEmpty(diagTerm)) {
                throw new Exception("Failed to find diagnosis term for ICD 10 code " + parser.getPrimaryDiagnosisCode().getString());
            }
            code.setCodingDisplay(diagTerm);
            //note: no original text to set
            condition.setCategory("diagnosis");

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), condition);

            // 0 - 12 potential secondary diagnostic codes. Only if there has been a primary
            for (int i = 1; i <= 12; i++) {
                Method method = Episodes.class.getDeclaredMethod("getDiag" + i);
                CsvCell diagCode = (CsvCell) method.invoke(parser);
                if (!diagCode.isEmpty()) {
                    ConditionBuilder cc = new ConditionBuilder((Condition) condition.getResource());
                    cc.setId(idCell.getString() + "Condition:" + i);
                    cc.setAsProblem(false);
                    cc.setIsPrimary(false);
                    method = Episodes.class.getDeclaredMethod("getDiag" + i + "Dttm");
                    CsvCell diagtime = (CsvCell) method.invoke(parser);
                    DateTimeType dtti = new DateTimeType(diagtime.getDateTime());
                    cc.setOnset(dtti, diagtime);
                    cc.removeCodeableConcept(CodeableConceptBuilder.Tag.Condition_Main_Code, null);
                    CodeableConceptBuilder codeableConceptBuilder
                            = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
                    codeableConceptBuilder.setCodingCode(diagCode.getString(), diagCode);
                    diagTerm = TerminologyService.lookupIcd10CodeDescription(diagCode.getString());
                    if (Strings.isNullOrEmpty(diagTerm)) {
                        throw new Exception("Failed to find diagnosis term for ICD 10 code " + diagCode.getString());
                    }
                    code.setCodingDisplay(diagTerm);
                    cc.setCategory("diagnosis");

                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), cc);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }

        //Primary procedure - check one exists
        if (!parser.getPrimaryProcedureCode().isEmpty()) {

            ProcedureBuilder proc = new ProcedureBuilder();
            proc.setIsPrimary(true);
            proc.setId(idCell.getString() + ":Procedure:0", idCell);
            proc.setPatient(patientReference, patientIdCell);
            proc.setIsPrimary(true);
            proc.setEncounter(thisEncounter, idCell);
            if (!parser.getPrimaryProcedureDate().isEmpty()) {
                DateTimeType dttp = new DateTimeType(parser.getPrimaryProcedureDate().getDateTime());
                proc.setPerformed(dttp, parser.getPrimaryProcedureDate());
            }
            if (!episodeConsultantCell.isEmpty()) {
                proc.addPerformer(episodePractitioner, episodeConsultantCell);
            }

            CodeableConceptBuilder code
                    = new CodeableConceptBuilder(proc, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            code.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
            code.setCodingCode(parser.getPrimaryProcedureCode().getString(),
                    parser.getPrimaryProcedureCode());
            String procTerm = TerminologyService.lookupOpcs4ProcedureName(parser.getPrimaryProcedureCode().getString());
            if (Strings.isNullOrEmpty(procTerm)) {
                throw new Exception("Failed to find procedure term for OPCS-4 code " + parser.getPrimaryProcedureCode().getString());
            }
            code.setCodingDisplay(procTerm); //don't pass in a cell as this was derived

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), proc);

            //ProcedureBuilder 1-12
            for (int i = 1; i <= 12; i++) {
                Method method = Episodes.class.getDeclaredMethod("getProc" + i);
                CsvCell procCode = (CsvCell) method.invoke(parser);
                if (!procCode.isEmpty()) {
                    ProcedureBuilder procedureBuilder = new ProcedureBuilder((Procedure) proc.getResource());
                    procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                    procedureBuilder.setIsPrimary(false);
                    procedureBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Procedure_Main_Code, null);
                    CodeableConceptBuilder codeableConceptBuilder
                            = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
                    codeableConceptBuilder.setCodingCode(procCode.getString(), procCode);
                    procTerm = TerminologyService.lookupOpcs4ProcedureName(procCode.getString());
                    if (Strings.isNullOrEmpty(procTerm)) {
                        throw new Exception("Failed to find procedure term for OPCS-4 code " + procCode.getString());
                    }
                    codeableConceptBuilder.setCodingDisplay(procTerm); //don't pass in a cell as this was derived

                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }

    private static void deleteChildResources(Episodes parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             BhrutCsvHelper csvHelper,
                                             String version) throws Exception {
        CsvCell idCell = parser.getId();
        CsvCell actionCell = parser.getLinestatus();
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        //delete primary diagnosis and secondaries
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString() + "Condition:0");
            condition.setPatient(patientReference, patientIdCell);
            condition.setDeletedAudit(actionCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), condition);

            for (int i = 1; i <= 12; i++) {
                Method method = Episodes.class.getDeclaredMethod("getDiag" + i);
                CsvCell diagCode = (CsvCell) method.invoke(parser);
                if (!diagCode.isEmpty()) {
                    ConditionBuilder conditionBuilder = new ConditionBuilder();
                    conditionBuilder.setId(idCell.getString() + "Condition:" + i);
                    conditionBuilder.setPatient(patientReference, patientIdCell);
                    conditionBuilder.setDeletedAudit(actionCell);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
        //delete primary procedures and secondaries
        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            ProcedureBuilder proc = new ProcedureBuilder();
            proc.setId(idCell.getString() + ":Procedure:0", idCell);
            proc.setPatient(patientReference, patientIdCell);
            proc.setDeletedAudit(actionCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), proc);

            for (int i = 1; i <= 12; i++) {
                Method method = Episodes.class.getDeclaredMethod("getProc" + i);
                CsvCell procCode = (CsvCell) method.invoke(parser);
                if (!procCode.isEmpty()) {
                    ProcedureBuilder procedureBuilder = new ProcedureBuilder();
                    procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                    procedureBuilder.setPatient(patientReference, patientIdCell);
                    procedureBuilder.setDeletedAudit(actionCell);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }
}
