package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
import org.endeavourhealth.transform.bhrut.schema.Outpatients;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.Reference;
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
        CsvCell idCell  = parser.getId();
        encounterBuilder.setId(idCell.toString());
        CsvCell actionCell = parser.getLinestatus();
        if (actionCell.getString().equalsIgnoreCase("Delete")) {
            encounterBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
            //TODO delete Conditions and Procedures?
            return;
        }
        CsvCell staffIdCell  = parser.getEpisodeConsultantCode();
        Reference staffReference = csvHelper.createPractitionerReference(staffIdCell.getString());
        org.hl7.fhir.instance.model.Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);

        Reference spellEncounter = csvHelper.createEncounterReference(parser.getIpSpellExternalId().getString(), patientReference.getId());
        encounterBuilder.setPartOf(spellEncounter);
        CsvCell admittingConsultant = parser.getEpisodeConsultantCode();
        Reference practitioner = csvHelper.createPractitionerReference(admittingConsultant.getString());
        encounterBuilder.addParticipant(practitioner, EncounterParticipantType.CONSULTANT, admittingConsultant);
       //
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder, encounterBuilder);


        Reference thisEncounter = csvHelper.createEncounterReference(parser.getId().getString(), patientReference.getId());
        ConditionBuilder condition = new ConditionBuilder();
        condition.setId(idCell.getString() + "Condition:0");
        condition.setPatient(patientReference,patientIdCell);
        condition.setEncounter(thisEncounter,parser.getId());
        CodeableConceptBuilder code = new CodeableConceptBuilder(condition,CodeableConceptBuilder.Tag.Condition_Main_Code);
        code.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
        code.setCodingCode(parser.getPrimaryDiagnosisCode().getString(), parser.getPrimaryDiagnosisCode());
        condition.setCode(code.getCodeableConcept(),parser.getPrimaryDiagnosisCode());
        condition.setRecordedDate(parser.getPrimdiagDttm().getDate(),parser.getPrimdiagDttm());
        condition.setClinician(staffReference, staffIdCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder, condition);

        // 0 - 12 potential secondary diagnostic codes.
        for ( int i =1; i<=12; i++){
            Method method = Outpatients.class.getDeclaredMethod("getDiag" + i);
            CsvCell diagCode = (CsvCell) method.invoke(parser);
            if (!diagCode.isEmpty()) {
                ConditionBuilder cc = new ConditionBuilder((Condition) condition.getResource());
                cc.setId(idCell.getString() + "Condition:" + i);
                cc.setAsProblem(false);

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
            proc.setId(parser.getId().getString(), parser.getId());
            if (!parser.getPrimaryProcedureDate().isEmpty()) {
                DateTimeType dtt = new DateTimeType(parser.getPrimaryProcedureDate().getDateTime());
                proc.setPerformed(dtt, parser.getPrimaryProcedureDate());
            }
           if (!parser.getEpisodeConsultantCode().isEmpty()) {
               proc.addPerformer(staffReference, parser.getEpisodeConsultantCode());
           }

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(condition,
                    CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
            codeableConceptBuilder.setCodingCode(parser.getPrimaryProcedureCode().getString(),
                    parser.getPrimaryProcedureCode());
        }

        //ProcedureBuilder 1-12
        for ( int i =1; i<=12; i++){
            Method method = Outpatients.class.getDeclaredMethod("getProc" + i);
            CsvCell procCode = (CsvCell) method.invoke(parser);
            if (!procCode.isEmpty()) {
                ProcedureBuilder procedureBuilder = new ProcedureBuilder((Procedure) condition.getResource());
                procedureBuilder.setId(idCell.getString() + ":episode:" + i);
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
}
