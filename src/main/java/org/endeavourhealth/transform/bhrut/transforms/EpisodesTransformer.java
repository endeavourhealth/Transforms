package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
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

        CsvCell org = parser.getAdmissionHospitalCode();
        Reference orgReference = csvHelper.createOrganisationReference(org.getString());
        encounterBuilder.setServiceProvider(orgReference);

        CsvCell episodeConsultantCell = parser.getEpisodeConsultantCode();
        Reference episodePractitioner = csvHelper.createPractitionerReference(episodeConsultantCell.getString());
        encounterBuilder.addParticipant(episodePractitioner, EncounterParticipantType.CONSULTANT, episodeConsultantCell);

        //the parent inpatient spell encounter
        Reference spellEncounter
                = csvHelper.createEncounterReference(parser.getIpSpellExternalId().getString(), patientReference.getId());
        encounterBuilder.setPartOf(spellEncounter);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);

        Reference thisEncounter
                = csvHelper.createEncounterReference(parser.getId().getString(), patientReference.getId());
        ConditionBuilder condition = new ConditionBuilder();
        condition.setId(idCell.getString() + "Condition:0");
        condition.setPatient(patientReference, patientIdCell);
        DateTimeType dtt = new DateTimeType(parser.getPrimdiagDttm().getDateTime());
        condition.setOnset(dtt, parser.getPrimdiagDttm());
        condition.setIsPrimary(true);
        condition.setAsProblem(false);
        condition.setEncounter(thisEncounter, parser.getId());
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            CodeableConceptBuilder code = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
            code.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            code.setCodingCode(parser.getPrimaryDiagnosisCode().getString(), parser.getPrimaryDiagnosisCode());
            condition.setCode(code.getCodeableConcept(), parser.getPrimaryDiagnosisCode());
        }
        condition.setRecordedDate(parser.getPrimdiagDttm().getDate(), parser.getPrimdiagDttm());
        condition.setClinician(episodePractitioner, episodeConsultantCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder, condition);

        // 0 - 12 potential secondary diagnostic codes.
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
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
                codeableConceptBuilder.setCodingCode(diagCode.getString(), diagCode);

                fhirResourceFiler.savePatientResource(parser.getCurrentState(), cc);
            } else {
                break;  //No point parsing empty cells. Assume non-empty cells are sequential.
            }
        }
        //Primary procedure
        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            ProcedureBuilder proc = new ProcedureBuilder();
            proc.setIsPrimary(true);
            proc.setId(idCell.getString() + ":Procedure:0", idCell);
            proc.setIsPrimary(true);
            if (!parser.getPrimaryProcedureDate().isEmpty()) {
                DateTimeType dttp = new DateTimeType(parser.getPrimaryProcedureDate().getDateTime());
                proc.setPerformed(dttp, parser.getPrimaryProcedureDate());
            }
            if (!episodeConsultantCell.isEmpty()) {
                proc.addPerformer(episodePractitioner, episodeConsultantCell);
            }

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(proc, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
            codeableConceptBuilder.setCodingCode(parser.getPrimaryProcedureCode().getString(),
                    parser.getPrimaryProcedureCode());

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), proc);
        }

        //ProcedureBuilder 1-12
        for (int i = 1; i <= 12; i++) {
            Method method = Episodes.class.getDeclaredMethod("getProc" + i);
            CsvCell procCode = (CsvCell) method.invoke(parser);
            if (!procCode.isEmpty()) {
                ProcedureBuilder procedureBuilder = new ProcedureBuilder((Procedure) condition.getResource());
                procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                procedureBuilder.setIsPrimary(false);
                procedureBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Procedure_Main_Code, null);
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition,
                        CodeableConceptBuilder.Tag.Procedure_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
                codeableConceptBuilder.setCodingCode(procCode.getString(), procCode);

                fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
            } else {
                break;  //No point parsing empty cells. Assume non-empty cells are sequential.
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

        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            ProcedureBuilder proc = new ProcedureBuilder();
            proc.setId(parser.getId().getString(), parser.getId());
            Reference patientReference = csvHelper.createPatientReference(patientIdCell);
            proc.setPatient(patientReference, patientIdCell);
            proc.setDeletedAudit(actionCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), proc);
        }
        for (int i = 1; i <= 12; i++) {
            Method method = Episodes.class.getDeclaredMethod("getProc" + i);
            CsvCell procCode = (CsvCell) method.invoke(parser);
            if (!procCode.isEmpty()) {
                ProcedureBuilder procedureBuilder = new ProcedureBuilder();
                procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                Reference patientReference = csvHelper.createPatientReference(patientIdCell);
                procedureBuilder.setPatient(patientReference, patientIdCell);
                procedureBuilder.setDeletedAudit(actionCell);

                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
            } else {
                break;  //No point parsing empty cells. Assume non-empty cells are sequential.
            }
        }

        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString() + "Condition:0");
            Reference patientReference = csvHelper.createPatientReference(patientIdCell);
            condition.setPatient(patientReference, patientIdCell);
            condition.setDeletedAudit(actionCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), condition);
        }
        for (int i = 1; i <= 12; i++) {
            Method method = Episodes.class.getDeclaredMethod("getDiag" + i);
            CsvCell diagCode = (CsvCell) method.invoke(parser);
            if (!diagCode.isEmpty()) {
                ConditionBuilder conditionBuilder = new ConditionBuilder();
                conditionBuilder.setId(idCell.getString() + "Condition:" + i);
                Reference patientReference = csvHelper.createPatientReference(patientIdCell);
                conditionBuilder.setPatient(patientReference, patientIdCell);
                conditionBuilder.setDeletedAudit(actionCell);

                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            } else {
                break;  //No point parsing empty cells. Assume non-empty cells are sequential.
            }
        }
    }
}
