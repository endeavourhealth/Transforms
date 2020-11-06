package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
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

        Episodes parser = (Episodes) parsers.get(Episodes.class);

        if (parser != null) {
            while (parser.nextRecord()) {
                if (!csvHelper.processRecordFilteringOnPatientId(parser)) {
                    continue;
                }
                try {

                    CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
                    if (dataUpdateStatusCell.getString().equalsIgnoreCase("Deleted")) {

                        deleteResources(parser, fhirResourceFiler, csvHelper);
                    } else {

                        createResources(parser, fhirResourceFiler, csvHelper);
                    }
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
                                       BhrutCsvHelper csvHelper) throws Exception {

        //retrieve the parent hospital spell encounter created previously
        CsvCell getIpSpellExternalIdRawCell = parser.getIpSpellExternalIdRaw();  //the raw id without BHRUT concatenation
        if (!getIpSpellExternalIdRawCell.isEmpty()) {

            CsvCell spellExternalIdCell = parser.getIpSpellExternalId();
            Encounter spellEncounter
                    = (Encounter) csvHelper.retrieveResource(spellExternalIdCell.getString(), ResourceType.Encounter);

            if (spellEncounter != null) {
                EncounterBuilder spellEncounterBuilder = new EncounterBuilder(spellEncounter);

                //create the episode encounter
                createEpisodeEncounters(parser, spellEncounterBuilder, fhirResourceFiler, csvHelper);
            } else {

                //if we have received a spell encounter id reference that we cannot find resource for in the db, throw an exception
                throw new TransformRuntimeException("Cannot find spell encounter for id: "+spellExternalIdCell.getString());
            }
        } else {

            //if the spell encounter Id is missing then this record should follow in a later extract
            //with a valid spell encounter Id to link to a spell record.  Simply log the fact here.
            TransformWarnings.log(LOG, csvHelper, "Missing external spell id for episode encounter id: {} ", parser.getId().getString());
        }
    }


    private static void createProcedures(EncounterBuilder episodeEncounterBuilder,
                                         Episodes parser,
                                         FhirResourceFiler fhirResourceFiler,
                                         BhrutCsvHelper csvHelper) throws Exception {

        CsvCell primaryProcedureCodeCell = parser.getPrimaryProcedureCode();
        if (!primaryProcedureCodeCell.isEmpty()) {

            ProcedureBuilder procedurePrimaryBuilder = new ProcedureBuilder();

            CsvCell idCell = parser.getId();
            procedurePrimaryBuilder.setId(idCell.getString() + ":Procedure:0", idCell);

            CsvCell patientIdCell = parser.getPasId();
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            procedurePrimaryBuilder.setPatient(newPatientReference, patientIdCell);
            procedurePrimaryBuilder.setIsPrimary(true);

            //create an Encounter reference for the procedures and conditions to use
            String episodeEncounterId = idCell.getString() + ":" + parser.getEpiNum().getInt() + ":IP:Episode";
            Reference thisEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, episodeEncounterId);
            if (episodeEncounterBuilder.isIdMapped()) {
                thisEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(thisEncounter, csvHelper);
            }
            procedurePrimaryBuilder.setEncounter(thisEncounter, idCell);

            CsvCell primaryProcedureDateCell = parser.getPrimaryProcedureDate();
            if (!primaryProcedureDateCell.isEmpty()) {

                DateTimeType dttp = new DateTimeType(primaryProcedureDateCell.getDateTime());
                procedurePrimaryBuilder.setPerformed(dttp, primaryProcedureDateCell);
            }
            CsvCell episodeConsultantCodeCell = parser.getEpisodeConsultantCode();
            if (!episodeConsultantCodeCell.isEmpty()) {

                Reference practitionerReference2 = csvHelper.createPractitionerReference(episodeConsultantCodeCell.getString());
                procedurePrimaryBuilder.addPerformer(practitionerReference2, episodeConsultantCodeCell);
            }

            CodeableConceptBuilder codeableConceptBuilderPrimary
                    = new CodeableConceptBuilder(procedurePrimaryBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilderPrimary.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);

            String primaryProcCodeString = TerminologyService.standardiseOpcs4Code(primaryProcedureCodeCell.getString());
            codeableConceptBuilderPrimary.setCodingCode(primaryProcCodeString, primaryProcedureCodeCell);

            String procTerm = TerminologyService.lookupOpcs4ProcedureName(primaryProcCodeString);
            if (Strings.isNullOrEmpty(procTerm)) {
                throw new Exception("Failed to find procedure term for OPCS-4 code " + primaryProcCodeString);
            }
            codeableConceptBuilderPrimary.setCodingDisplay(procTerm); //don't pass in a cell as this was derived

            CsvCell primaryProcedureTextCell = parser.getPrimaryProcedure();
            if (!primaryProcedureTextCell.isEmpty()) {
                codeableConceptBuilderPrimary.setText(primaryProcedureTextCell.getString(), primaryProcedureTextCell);
            }

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedurePrimaryBuilder);

            //ProcedureBuilder 1-12
            for (int i = 1; i <= 12; i++) {
                Method method = Episodes.class.getDeclaredMethod("getProc" + i);
                CsvCell procCodeCell = (CsvCell) method.invoke(parser);
                if (!procCodeCell.isEmpty()) {

                    ProcedureBuilder procedureBuilder = new ProcedureBuilder();
                    procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);

                    Reference newPatientReference2 = csvHelper.createPatientReference(patientIdCell);
                    procedureBuilder.setPatient(newPatientReference2, patientIdCell);
                    procedureBuilder.setIsPrimary(false);

                    Reference procEncReference
                            = ReferenceHelper.createReference(ResourceType.Encounter, episodeEncounterId);
                    if (episodeEncounterBuilder.isIdMapped()) {
                        procEncReference
                                = IdHelper.convertLocallyUniqueReferenceToEdsReference(procEncReference, csvHelper);
                    }
                    procedureBuilder.setEncounter(procEncReference, idCell);

                    procedureBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Procedure_Main_Code, null);
                    CodeableConceptBuilder codeableConceptBuilder
                            = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);

                    String procCodeString = TerminologyService.standardiseOpcs4Code(procCodeCell.getString());
                    codeableConceptBuilder.setCodingCode(procCodeString, procCodeCell);

                    procTerm = TerminologyService.lookupOpcs4ProcedureName(procCodeString);
                    if (Strings.isNullOrEmpty(procTerm)) {
                        throw new Exception("Failed to find procedure term for OPCS-4 code " + procCodeString);
                    }
                    codeableConceptBuilder.setCodingDisplay(procTerm); //don't pass in a cell as this was derived

                    //consultant and procedure date are inferred from the primary procedure date and consultant
                    if (!primaryProcedureDateCell.isEmpty()) {
                        DateTimeType dttp = new DateTimeType(primaryProcedureDateCell.getDateTime());
                        procedureBuilder.setPerformed(dttp, primaryProcedureDateCell);
                    }
                    if (!episodeConsultantCodeCell.isEmpty()) {
                        Reference practitionerReference2
                                = csvHelper.createPractitionerReference(episodeConsultantCodeCell.getString());
                        procedureBuilder.addPerformer(practitionerReference2, episodeConsultantCodeCell);
                    }

                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }

    private static void createConditions(EncounterBuilder episodeEncounterBuilder,
                                         Episodes parser,
                                         FhirResourceFiler fhirResourceFiler,
                                         BhrutCsvHelper csvHelper) throws Exception {

        CsvCell primaryDiagnosisCodeCell = parser.getPrimaryDiagnosisCode();
        if (!primaryDiagnosisCodeCell.isEmpty()) {

            ConditionBuilder condition = new ConditionBuilder();
            CsvCell idCell = parser.getId();
            condition.setId(idCell.getString() + "Condition:0", idCell);

            CsvCell patientIdCell = parser.getPasId();
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            condition.setPatient(newPatientReference, patientIdCell);

            DateTimeType dtt = new DateTimeType(parser.getPrimdiagDttm().getDateTime());
            condition.setOnset(dtt, parser.getPrimdiagDttm());
            condition.setIsPrimary(true);
            condition.setAsProblem(false);

            //create an Encounter reference for the conditions to use
            String episodeEncounterId = idCell.getString() + ":" + parser.getEpiNum().getInt() + ":IP:Episode";
            Reference thisEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, episodeEncounterId);
            if (episodeEncounterBuilder.isIdMapped()) {
                thisEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(thisEncounter, csvHelper);
            }
            condition.setEncounter(thisEncounter, idCell);

            CsvCell episodeConsultantCodeCell = parser.getEpisodeConsultantCode();
            if (!episodeConsultantCodeCell.isEmpty()) {
                Reference practitionerReference2
                        = csvHelper.createPractitionerReference(episodeConsultantCodeCell.getString());
                condition.setClinician(practitionerReference2, episodeConsultantCodeCell);
            }
            CodeableConceptBuilder code
                    = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
            code.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);

            String icd10 = TerminologyService.standardiseIcd10Code(primaryDiagnosisCodeCell.getString().trim());
            if (icd10.endsWith("X") || icd10.endsWith("D") || icd10.endsWith("A")) {
                icd10 = icd10.substring(0, 3);
            }
            code.setCodingCode(icd10, primaryDiagnosisCodeCell);
            String diagTerm = TerminologyService.lookupIcd10CodeDescription(icd10);
            if (Strings.isNullOrEmpty(diagTerm)) {
                throw new Exception("Failed to find diagnosis term for ICD 10 code " + icd10);
            }
            code.setCodingDisplay(diagTerm);
            //note: no original text to set
            condition.setCategory("diagnosis");

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), condition);

            // 0 - 12 potential secondary diagnostic codes. Only if there has been a primary
            for (int i = 1; i <= 12; i++) {
                Method method = Episodes.class.getDeclaredMethod("getDiag" + i);
                CsvCell diagCodeCell = (CsvCell) method.invoke(parser);
                if (!diagCodeCell.isEmpty()) {
                    ConditionBuilder conditionOther = new ConditionBuilder();
                    conditionOther.setId(idCell.getString() + "Condition:" + i);
                    conditionOther.setAsProblem(false);
                    Reference condEncReference
                            = ReferenceHelper.createReference(ResourceType.Encounter, episodeEncounterId);
                    if (episodeEncounterBuilder.isIdMapped()) {
                        condEncReference
                                = IdHelper.convertLocallyUniqueReferenceToEdsReference(condEncReference, csvHelper);
                    }
                    conditionOther.setEncounter(condEncReference, idCell);

                    conditionOther.setPatient(csvHelper.createPatientReference(patientIdCell), patientIdCell);
                    method = Episodes.class.getDeclaredMethod("getDiag" + i + "Dttm");
                    CsvCell diagtime = (CsvCell) method.invoke(parser);
                    DateTimeType dtti = new DateTimeType(diagtime.getDateTime());
                    conditionOther.setOnset(dtti, diagtime);
                    conditionOther.removeCodeableConcept(CodeableConceptBuilder.Tag.Condition_Main_Code, null);
                    CodeableConceptBuilder codeableConceptBuilder
                            = new CodeableConceptBuilder(conditionOther, CodeableConceptBuilder.Tag.Condition_Main_Code);
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);

                    String diagCodeString = TerminologyService.standardiseIcd10Code(diagCodeCell.getString());
                    if (diagCodeString.endsWith("X") || diagCodeString.endsWith("D") || diagCodeString.endsWith("A")) {
                        diagCodeString = diagCodeString.substring(0, 3);
                    }
                    String diagTerm2 = TerminologyService.lookupIcd10CodeDescription(diagCodeString);
                    if (Strings.isNullOrEmpty(diagTerm2)) {
                        throw new Exception("Failed to find diagnosis term for ICD 10 code " + diagCodeString + ".");
                    }
                    codeableConceptBuilder.setCodingCode(diagCodeString, diagCodeCell);
                    codeableConceptBuilder.setCodingDisplay(diagTerm2);
                    conditionOther.setCategory("diagnosis");
                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionOther);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }

    private static void deleteResources(Episodes parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        //this is a single Encounter record (no children)
        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(parser.getId().getString());

        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);

        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        encounterBuilder.setDeletedAudit(dataUpdateStatusCell);
        fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, encounterBuilder);

        //then delete the linked clinical resources
        deleteClinicalResources(parser, fhirResourceFiler, csvHelper);
    }

    private static void createEpisodeEncounters(Episodes parser, EncounterBuilder existingParentEncounterBuilder, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        //if this is episode 1, retrieve the admission encounter using the spell encounter Id and set it's end date
        CsvCell epiNumCell = parser.getEpiNum();
        if (!epiNumCell.isEmpty() && epiNumCell.getInt() == 1) {

            CsvCell spellExternalIdCell = parser.getIpSpellExternalId();
            String spellAdmissionEncounterId = spellExternalIdCell.getString() + ":01:IP:Admission";
            Encounter spellAdmissionEncounter
                    = (Encounter) csvHelper.retrieveResource(spellAdmissionEncounterId, ResourceType.Encounter);
            EncounterBuilder spellAdmissionEncounterBuilder = new EncounterBuilder(spellAdmissionEncounter);

            //update the end date and status of the admission
            CsvCell episodeStartDateCell = parser.getEpisodeStartDttm();
            spellAdmissionEncounterBuilder.setPeriodEnd(episodeStartDateCell.getDateTime(), episodeStartDateCell);
            spellAdmissionEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

            //save the update spell admission encounter
            fhirResourceFiler.savePatientResource(parser.getCurrentState(),
                    !spellAdmissionEncounterBuilder.isIdMapped(),
                    spellAdmissionEncounterBuilder);
        }

        //these are the 1, 2, 3, 4 subsequent episodes where activity happens, conditions, procedures, wards change etc.
        EncounterBuilder episodeEncounterBuilder = new EncounterBuilder();
        episodeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        String episodeEncounterId = parser.getId().getString() + ":" + parser.getEpiNum().getInt() + ":IP:Episode";
        episodeEncounterBuilder.setId(episodeEncounterId);

        //spell episode encounters have their own start and end date/times which occur within a spell
        CsvCell episodeStartDateCell = parser.getEpisodeStartDttm();
        if (!episodeStartDateCell.isEmpty()) {
            episodeEncounterBuilder.setPeriodStart(episodeStartDateCell.getDateTime(), episodeStartDateCell);
        }
        CsvCell episodeEndDateCell = parser.getEpisodeEndDttm();
        if (!episodeEndDateCell.isEmpty()) {

            episodeEncounterBuilder.setPeriodEnd(episodeEndDateCell.getDateTime(), episodeEndDateCell);
            episodeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            episodeEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(episodeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient Episode");

        setCommonEncounterAttributes(episodeEncounterBuilder, parser, csvHelper, true);

        //and link the parent to this new child encounter
        ContainedListBuilder existingParentEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);
        Reference childEpisodeRef = ReferenceHelper.createReference(ResourceType.Encounter, episodeEncounterId);
        if (existingParentEncounterBuilder.isIdMapped()) {

            childEpisodeRef
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(childEpisodeRef, csvHelper);
        }
        existingParentEncounterList.addReference(childEpisodeRef);

        //add in additional extended data as Parameters resource with additional extension
        ContainedParametersBuilder containedParametersBuilder
                = new ContainedParametersBuilder(episodeEncounterBuilder);
        containedParametersBuilder.removeContainedParameters();

        CsvCell adminCategoryCodeCe = parser.getAdministrativeCategoryCode();
        if (!adminCategoryCodeCe.isEmpty()) {
            BhrutCsvHelper.addParmIfNotNullNhsdd( "ADMINISTRATIVE_CATEGORY_CODE",
                    adminCategoryCodeCe.getString(),
                    adminCategoryCodeCe,
                    containedParametersBuilder,
                    BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
        }

        String episodeStartWardCode = parser.getEpisodeStartWardCode().getString();
        String episodeEndWardCode = parser.getEpisodeEndWardCode().getString();
        if (!Strings.isNullOrEmpty(episodeStartWardCode) || !Strings.isNullOrEmpty(episodeEndWardCode)) {
            // JsonObject episodeWardsObjs = new JsonObject();
            if (!Strings.isNullOrEmpty(episodeStartWardCode)) {
                String columnName =  "EPISODE_START_WARD_CODE";
                JsonObject episodeWardsObjs = new JsonObject();
                episodeWardsObjs.addProperty(columnName, episodeStartWardCode);
                MapColumnRequest propertyRequest = new MapColumnRequest(
                        BhrutCsvToFhirTransformer.IM_PROVIDER_CONCEPT_ID,
                        BhrutCsvToFhirTransformer.IM_SYSTEM_CONCEPT_ID,
                        BhrutCsvToFhirTransformer.IM_SCHEMA,
                        BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME,
                        columnName
                );
                MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);
                String propertyCode = propertyResponse.getConcept().getCode();
                String propertyName = "JSON_"+propertyCode;
                containedParametersBuilder.addParameter(propertyName, episodeWardsObjs.toString());
            }
            if (!Strings.isNullOrEmpty(episodeEndWardCode)) {
                String columnName =  "EPISODE_END_WARD_CODE";
                JsonObject episodeWardsObjs = new JsonObject();
                episodeWardsObjs.addProperty(columnName, episodeEndWardCode);

                MapColumnRequest propertyRequest = new MapColumnRequest(
                        BhrutCsvToFhirTransformer.IM_PROVIDER_CONCEPT_ID,
                        BhrutCsvToFhirTransformer.IM_SYSTEM_CONCEPT_ID,
                        BhrutCsvToFhirTransformer.IM_SCHEMA,
                        BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME,
                        columnName
                );
                MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);
                String propertyCode = propertyResponse.getConcept().getCode();
                String propertyName = "JSON_"+propertyCode;
                containedParametersBuilder.addParameter(propertyName, episodeWardsObjs.toString());
            }
        }

        //save the existing parent encounter here with the updated child refs added during this method, then the sub encounters
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);

        //create the conditions and procedures for the episode encounter
        createConditions(episodeEncounterBuilder, parser, fhirResourceFiler, csvHelper);
        createProcedures(episodeEncounterBuilder, parser, fhirResourceFiler, csvHelper);

        //finally, save the episode encounter which always exists
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), !episodeEncounterBuilder.isIdMapped(), episodeEncounterBuilder);
     }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     Episodes parser,
                                                     BhrutCsvHelper csvHelper,
                                                     boolean isChildEncounter) throws Exception {

        //every encounter has the following common attributes
        CsvCell patientIdCell = parser.getPasId();
        if (!patientIdCell.isEmpty()) {
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, patientIdCell.getString());
            if (builder.isIdMapped()) {
                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }
            builder.setPatient(patientReference, patientIdCell);
        }

        //these episode encounters are part of the same episode of care as the parent spell encounter
        CsvCell spellExternalIdCell = parser.getIpSpellExternalId();
        if (!spellExternalIdCell.isEmpty()) {

            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, spellExternalIdCell.getString());
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, csvHelper);
            }
            builder.setEpisodeOfCare(episodeReference, spellExternalIdCell);
        }
        CsvCell episodeConsultantCodeCell = parser.getEpisodeConsultantCode();
        if (!episodeConsultantCodeCell.isEmpty()) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, episodeConsultantCodeCell.getString());
            if (builder.isIdMapped()) {
                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, episodeConsultantCodeCell);
        }
        CsvCell admissionHospitalCode = parser.getAdmissionHospitalCode();
        if (!admissionHospitalCode.isEmpty()) {
            Reference organizationReference
                    = csvHelper.createOrganisationReference(admissionHospitalCode.getString());
            if (builder.isIdMapped()) {
                organizationReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, csvHelper);
            }
            builder.setServiceProvider(organizationReference, admissionHospitalCode);
        }
        if (isChildEncounter) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, spellExternalIdCell.getString());
            parentEncounter
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);

            builder.setPartOf(parentEncounter, spellExternalIdCell);
        }
    }

    private static void deleteClinicalResources(Episodes parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             BhrutCsvHelper csvHelper) throws Exception {

        CsvCell idCell = parser.getId();
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        //delete primary diagnosis and secondaries
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString() + "Condition:0");
            condition.setPatient(patientReference, patientIdCell);
            condition.setDeletedAudit(dataUpdateStatusCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), condition);

            for (int i = 1; i <= 12; i++) {
                Method method = Episodes.class.getDeclaredMethod("getDiag" + i);
                CsvCell diagCode = (CsvCell) method.invoke(parser);
                if (!diagCode.isEmpty()) {
                    ConditionBuilder conditionBuilder = new ConditionBuilder();
                    conditionBuilder.setId(idCell.getString() + "Condition:" + i);
                    conditionBuilder.setPatient(patientReference, patientIdCell);
                    conditionBuilder.setDeletedAudit(dataUpdateStatusCell);
                    conditionBuilder.setAsProblem(false);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, conditionBuilder);
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
            proc.setDeletedAudit(dataUpdateStatusCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), proc);

            for (int i = 1; i <= 12; i++) {
                Method method = Episodes.class.getDeclaredMethod("getProc" + i);
                CsvCell procCode = (CsvCell) method.invoke(parser);
                if (!procCode.isEmpty()) {
                    ProcedureBuilder procedureBuilder = new ProcedureBuilder();
                    procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                    Reference loopPatientReference = csvHelper.createPatientReference(patientIdCell);
                    procedureBuilder.setPatient(loopPatientReference, patientIdCell);
                    procedureBuilder.setDeletedAudit(dataUpdateStatusCell);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }
}
