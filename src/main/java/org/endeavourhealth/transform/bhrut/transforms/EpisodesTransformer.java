package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
                    if (parser.getDataUpdateStatus().getString().equalsIgnoreCase("Deleted")) {
                        deleteEncounterAndChildren(parser, fhirResourceFiler, csvHelper);
                    } else {
                        createResources(parser, fhirResourceFiler, csvHelper, version);
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
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        CsvCell patientIdCell = parser.getPasId();

        Encounter spellEncounter = (Encounter) csvHelper.retrieveResource(parser.getIpSpellExternalId().getString(), ResourceType.Encounter);
        EncounterBuilder spellEncounterBuilder = new EncounterBuilder(spellEncounter);
        String localId = parser.getIpSpellExternalId().getString();
        UUID uuid= IdHelper.getOrCreateEdsResourceId(parser.getServiceId(),ResourceType.Encounter, localId);
        spellEncounterBuilder.setId(uuid.toString());
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        patientReference =    IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        spellEncounterBuilder.setPatient(patientReference, patientIdCell);


        createSubEncounters(parser, spellEncounterBuilder, fhirResourceFiler, csvHelper);

        CsvCell admissionHospitalCodeCell = parser.getAdmissionHospitalCode();
        Reference organisationReference;
        if (!admissionHospitalCodeCell.isEmpty()) {
            if (Strings.isNullOrEmpty(csvHelper.findOdsCode(admissionHospitalCodeCell.getString()))) {
                organisationReference = csvHelper.createOrganisationReference(admissionHospitalCodeCell.getString());
               } else {
                organisationReference = csvHelper.createOrganisationReference(csvHelper.findOdsCode(admissionHospitalCodeCell.getString()));
            }

        } else {  //Default to BHRUT
            organisationReference = csvHelper.createOrganisationReference(BhrutCsvToFhirTransformer.BHRUT_ORG_ODS_CODE);
        }
        organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
        spellEncounterBuilder.setServiceProvider(organisationReference);
        createConditions(parser, fhirResourceFiler, csvHelper);

        createProcedures(parser, fhirResourceFiler, csvHelper);


    }


    private static void createProcedures(Episodes parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {
        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        if (!parser.getPrimaryProcedureCode().isEmpty()) {

            ProcedureBuilder proc = new ProcedureBuilder();
            proc.setIsPrimary(true);
            proc.setId(idCell.getString() + ":Procedure:0", idCell);
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            proc.setPatient(newPatientReference, patientIdCell);
            Reference encounterReference = csvHelper.createEncounterReference(idCell.getString(), patientIdCell.getString());
            String json = FhirSerializationHelper.serializeResource(proc.getResource());
            proc.setIsPrimary(true);
            proc.setEncounter(encounterReference, idCell);
            if (!parser.getPrimaryProcedureDate().isEmpty()) {
                DateTimeType dttp = new DateTimeType(parser.getPrimaryProcedureDate().getDateTime());
                proc.setPerformed(dttp, parser.getPrimaryProcedureDate());
            }
            CsvCell episodeConsultantCodeCell = parser.getEpisodeConsultantCode();
            if (!episodeConsultantCodeCell.isEmpty()) {
                Reference practitionerReference2 = csvHelper.createPractitionerReference(episodeConsultantCodeCell.getString());
                proc.addPerformer(practitionerReference2, episodeConsultantCodeCell);
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
                    //ProcedureBuilder procedureBuilder = new ProcedureBuilder((Procedure) proc.getResource());
                    ProcedureBuilder procedureBuilder = new ProcedureBuilder((Procedure) FhirSerializationHelper.deserializeResource(json));
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

    private static void createConditions(Episodes parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {
        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {

            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString() + "Condition:0");
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            condition.setPatient(newPatientReference, patientIdCell);

            DateTimeType dtt = new DateTimeType(parser.getPrimdiagDttm().getDateTime());
            condition.setOnset(dtt, parser.getPrimdiagDttm());
            String json = FhirSerializationHelper.serializeResource(condition.getResource());

            condition.setIsPrimary(true);
            condition.setAsProblem(false);
            Reference encounterReference = csvHelper.createEncounterReference(idCell.getString(), patientIdCell.getString());
            condition.setEncounter(encounterReference, parser.getId());
            CsvCell episodeConsultantCodeCell = parser.getEpisodeConsultantCode();
            if (!episodeConsultantCodeCell.isEmpty()) {
                Reference practitionerReference2 = csvHelper.createPractitionerReference(episodeConsultantCodeCell.getString());
                condition.setClinician(practitionerReference2, episodeConsultantCodeCell);
            }
            CodeableConceptBuilder code
                    = new CodeableConceptBuilder(condition, CodeableConceptBuilder.Tag.Condition_Main_Code);
            code.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);

            String icd10 = TerminologyService.standardiseIcd10Code(parser.getPrimaryDiagnosisCode().getString().trim());
            if (icd10.endsWith("X") || icd10.endsWith("D") || icd10.endsWith("A")) {
                icd10 = icd10.substring(0, 3);
            }
            code.setCodingCode(icd10, parser.getPrimaryDiagnosisCode());
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
                CsvCell diagCode = (CsvCell) method.invoke(parser);
                if (!diagCode.isEmpty()) {
                    ConditionBuilder cc = new ConditionBuilder();
                    cc.setId(idCell.getString() + "Condition:" + i);
                    cc.setAsProblem(false);
                    cc.setPatient(csvHelper.createPatientReference(patientIdCell), patientIdCell);
                    method = Episodes.class.getDeclaredMethod("getDiag" + i + "Dttm");
                    CsvCell diagtime = (CsvCell) method.invoke(parser);
                    DateTimeType dtti = new DateTimeType(diagtime.getDateTime());
                    cc.setOnset(dtti, diagtime);
                    cc.removeCodeableConcept(CodeableConceptBuilder.Tag.Condition_Main_Code, null);
                    CodeableConceptBuilder codeableConceptBuilder
                            = new CodeableConceptBuilder(cc, CodeableConceptBuilder.Tag.Condition_Main_Code);
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
                    String diagcodeString = diagCode.getString();
                    if (diagcodeString.endsWith("X") || diagcodeString.endsWith("D") || diagcodeString.endsWith("A")) {
                        diagcodeString = diagcodeString.substring(0, 3);
                    }
                    String diagTerm2 = TerminologyService.lookupIcd10CodeDescription(diagcodeString);
                    if (Strings.isNullOrEmpty(diagTerm2)) {
                        throw new Exception("Failed to find diagnosis term for ICD 10 code " + diagcodeString + ".");
                    }
                    codeableConceptBuilder.setCodingCode(diagcodeString, diagCode);
                    codeableConceptBuilder.setCodingDisplay(diagTerm2);
                    cc.setCategory("diagnosis");
                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), cc);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }

    private static void deleteEncounterAndChildren(Episodes parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        CsvCell spellExternalIdCell = parser.getIpSpellExternalId();

        if (!spellExternalIdCell.isEmpty()) {

            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, spellExternalIdCell.getString());

            if (existingParentEncounter != null) {

                EncounterBuilder parentEncounterBuilder
                        = new EncounterBuilder(existingParentEncounter);

                //has this encounter got child encounters?
                if (existingParentEncounter.hasContained()) {

                    ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
                    ResourceDalI resourceDal = DalProvider.factoryResourceDal();

                    for (List_.ListEntryComponent item : listBuilder.getContainedListItems()) {
                        Reference ref = item.getItem();
                        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                        if (comps.getResourceType() != ResourceType.Encounter) {
                            continue;
                        }
                        Encounter childEncounter
                                = (Encounter) resourceDal.getCurrentVersionAsResource(csvHelper.getServiceId(), ResourceType.Encounter, comps.getId());
                        if (childEncounter != null) {
                            LOG.debug("Deleting child encounter " + childEncounter.getId());

                            fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                        } else {

                            TransformWarnings.log(LOG, csvHelper, "Cannot find existing child Encounter ref: {} for deletion", comps.getId());
                        }
                    }
                }

                //finally, delete the top level parent
                //fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

            } else {
                TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", spellExternalIdCell);
            }
        }


    }

    private static void createSubEncounters(Episodes parser, EncounterBuilder existingParentEncounterBuilder, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        existingParentEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        //fhirResourceFiler.savePatientResource(null,existingParentEncounterBuilder);


        EncounterBuilder admissionEncounterBuilder = null;
        EncounterBuilder dischargeEncounterBuilder = null;
        CsvCell epiNumCell = parser.getEpiNum();

        if (!epiNumCell.isEmpty() && epiNumCell.getInt() == 1) {
            //epiNumCell.getString().equalsIgnoreCase("1")) {
            admissionEncounterBuilder = new EncounterBuilder();
            admissionEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
            String admissionEncounterId = parser.getId().getString() + ":01:IP:Admission";
            admissionEncounterBuilder.setId(admissionEncounterId);
            CsvCell spellStartDateCell = parser.getEpisodeStartDttm();
            admissionEncounterBuilder.setPeriodStart(spellStartDateCell.getDateTime());
            admissionEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderAdmission
                    = new CodeableConceptBuilder(admissionEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAdmission.setText("Inpatient Admission");

            setCommonEncounterAttributes(admissionEncounterBuilder, parser, csvHelper, true);

            //add in additional extended data as Parameters resource with additional extension
            ContainedParametersBuilder containedParametersBuilderAdmission
                    = new ContainedParametersBuilder(admissionEncounterBuilder);
            containedParametersBuilderAdmission.removeContainedParameters();

            CsvCell adminCategoryCodeCe = parser.getAdministrativeCategoryCode();
            if (!adminCategoryCodeCe.isEmpty()) {
               csvHelper.addParmIfNotNull("AdministrativeCategoryCode", "ADMINISTRATIVE_CATEGORY_CODE",
                      adminCategoryCodeCe.getString(), adminCategoryCodeCe, containedParametersBuilderAdmission, BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
            }
            CsvCell admissionMethodCodeCell = parser.getAdmissionMethodCode();
            if (!admissionMethodCodeCell.isEmpty()) {
                csvHelper.addParmIfNotNull("ip_admission_method", "ADMISSION_METHOD_CODE",
                        admissionMethodCodeCell.getString(), admissionMethodCodeCell, containedParametersBuilderAdmission, BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
            }
            CsvCell admissionSourceCodeCell = parser.getAdmissionSourceCode();
            if (!admissionSourceCodeCell.isEmpty()) {
                csvHelper.addParmIfNotNull("ip_admission_source", "ADMISSION_SOURCE_CODE",
                        admissionSourceCodeCell.getString(), admissionSourceCodeCell,containedParametersBuilderAdmission, BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
            }
            CsvCell patientClassCodeCell = parser.getPatientClassCode();
            if (!patientClassCodeCell.isEmpty()) {
                csvHelper.addParmIfNotNull("patient_classification", "PATIENT_CLASS_CODE",
                        patientClassCodeCell.getString(), patientClassCodeCell, containedParametersBuilderAdmission, BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
            }
            //if the 01 episode has an episode end date, set the admission end date
            CsvCell episodeEndDateCell = parser.getEpisodeEndDttm();
            if (!episodeEndDateCell.isEmpty()) {
                admissionEncounterBuilder.setPeriodEnd(episodeEndDateCell.getDateTime());
                admissionEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
            //and link the parent to this new child encounter
            Reference childAdmissionRef = ReferenceHelper.createReference(ResourceType.Encounter, admissionEncounterId);
            childAdmissionRef = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAdmissionRef, csvHelper);

            //the main encounter has a discharge date so set the end date and create a linked Discharge encounter
            if (!episodeEndDateCell.isEmpty()) {
                //create new additional Discharge encounter event to link to the top level parent
                dischargeEncounterBuilder = new EncounterBuilder();
                dischargeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

                String dischargeEncounterId = parser.getId().getString() + ":01:IP:Discharge";
                dischargeEncounterBuilder.setId(dischargeEncounterId);
                dischargeEncounterBuilder.setPeriodStart(episodeEndDateCell.getDateTime());
                dischargeEncounterBuilder.setPeriodEnd(episodeEndDateCell.getDateTime());
                dischargeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

                CodeableConceptBuilder codeableConceptBuilderDischarge
                        = new CodeableConceptBuilder(dischargeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                codeableConceptBuilderDischarge.setText("Inpatient Discharge");

                setCommonEncounterAttributes(dischargeEncounterBuilder, parser, csvHelper, true);

                //add in additional extended data as Parameters resource with additional extension
                ContainedParametersBuilder containedParametersBuilderDischarge
                        = new ContainedParametersBuilder(dischargeEncounterBuilder);
                containedParametersBuilderDischarge.removeContainedParameters();

                CsvCell dischargeMethodCodeCell = parser.getDischargeMethodCode();
                if (!dischargeMethodCodeCell.isEmpty()) {
                    csvHelper.addParmIfNotNull("ip_discharge_method", "DISCHARGE_METHOD_CODE",
                            dischargeMethodCodeCell.getString(), dischargeMethodCodeCell, containedParametersBuilderDischarge,BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
                }
                CsvCell dischargeDestinationCodeCell = parser.getDischargeDestinationCode();
                if (!dischargeDestinationCodeCell.isEmpty()) {
                    csvHelper.addParmIfNotNull("ip_discharge_destination", "DISCHARGE_DESINATION_CODE",
                            dischargeDestinationCodeCell.getString(), dischargeDestinationCodeCell, containedParametersBuilderDischarge, BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
                }
                //and link the parent to this new child encounter
            }
        }
        //these are the 01, 02, 03, 04 subsequent episodes where activity happens, maternity, wards change etc.
        //also, critical care child encounters link back to these as their parents via their own transform
        EncounterBuilder episodeEncounterBuilder = new EncounterBuilder();
        episodeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        String episodeEncounterId = parser.getId().getString() + ":" + parser.getEpiNum().getInt() + ":IP:Episode";
        episodeEncounterBuilder.setId(episodeEncounterId);

        //spell episode encounter have their own start and end date/times
        CsvCell episodeStartDateCell = parser.getEpisodeStartDttm();
        if (!episodeStartDateCell.isEmpty()) {
            episodeEncounterBuilder.setPeriodStart(episodeStartDateCell.getDateTime());
        }
        CsvCell episodeEndDateCell = parser.getEpisodeEndDttm();
        if (!episodeEndDateCell.isEmpty()) {
            episodeEncounterBuilder.setPeriodEnd(episodeEndDateCell.getDateTime());
            episodeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            episodeEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(episodeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient Episode");

        setCommonEncounterAttributes(episodeEncounterBuilder, parser, csvHelper, true);

        //add in additional extended data as Parameters resource with additional extension
        ContainedParametersBuilder containedParametersBuilder
                = new ContainedParametersBuilder(episodeEncounterBuilder);
        containedParametersBuilder.removeContainedParameters();

        CsvCell episodeStartWardCodeCell = parser.getEpisodeStartWardCode();
        if (!episodeStartWardCodeCell.isEmpty()) {
            csvHelper.addParmIfNotNull("ip_episode_start_ward", "EPISODE_START_WARD_CODE",
                    episodeStartWardCodeCell.getString(), episodeStartWardCodeCell,containedParametersBuilder, BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
        }
        CsvCell episodeEndWardCodeCell = parser.getEpisodeEndWardCode();
        if (!episodeEndWardCodeCell.isEmpty()) {
            csvHelper.addParmIfNotNull("ip_episode_end_ward",  "EPISODE_END_WARD_CODE",
                    episodeEndWardCodeCell.getString(), episodeEndWardCodeCell, containedParametersBuilder, BhrutCsvToFhirTransformer.IM_EPISODES_TABLE_NAME);
        }
        String episodeStartWardCode = parser.getEpisodeStartWardCode().getString();
        String episodeEndWardCode = parser.getEpisodeEndWardCode().getString();
        if (!Strings.isNullOrEmpty(episodeStartWardCode) || !Strings.isNullOrEmpty(episodeEndWardCode)) {

            JsonObject episodeWardsObjs = new JsonObject();
            if (!Strings.isNullOrEmpty(episodeStartWardCode)) {

                episodeWardsObjs.addProperty("start_ward", episodeStartWardCode);
            }
            if (!Strings.isNullOrEmpty(episodeEndWardCode)) {

                episodeWardsObjs.addProperty("end_ward", episodeEndWardCode);
            }


            MapColumnRequest propertyRequest = new MapColumnRequest(
                    BhrutCsvToFhirTransformer.IM_PROVIDER_CONCEPT_ID,
                    BhrutCsvToFhirTransformer.IM_SYSTEM_CONCEPT_ID,
                    BhrutCsvToFhirTransformer.IM_SCHEMA,
                    "inpatient",
                    "wards"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);
            String propertyCode = propertyResponse.getConcept().getCode();
            String propertyName = "JSON_"+propertyCode;
            containedParametersBuilder.addParameter(propertyName, episodeWardsObjs.toString());
        }











        //save the existing parent encounter here with the updated child refs added during this method, then the sub encounters
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, existingParentEncounterBuilder);

        //then save the child encounter builders if they are set

        if (admissionEncounterBuilder != null) {
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), admissionEncounterBuilder);
        }
        if (dischargeEncounterBuilder != null) {
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), dischargeEncounterBuilder);
        }
//        //finally, save the episode encounter which always exists
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), episodeEncounterBuilder);


     }

    private static EncounterBuilder createEncountersParentMinimum(Episodes parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        parentTopEncounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

        CsvCell admissionDateCell = parser.getEpisodeStartDttm();
        CsvCell dischargeDateCell = parser.getEpisodeEndDttm();

        parentTopEncounterBuilder.setId(parser.getId().getString());

        if (!admissionDateCell.isEmpty()) {
            parentTopEncounterBuilder.setPeriodStart(parser.getEpisodeStartDttm().getDateTime(), parser.getEpisodeStartDttm());
        }
        if (!dischargeDateCell.isEmpty()) {
            parentTopEncounterBuilder.setPeriodEnd(parser.getEpisodeEndDttm().getDateTime(), parser.getEpisodeEndDttm());
        }
        if (!dischargeDateCell.isEmpty()) {
            parentTopEncounterBuilder.setPeriodEnd(dischargeDateCell.getDateTime());
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient Admission");

        setCommonEncounterAttributes(parentTopEncounterBuilder, parser, csvHelper, false);

        return parentTopEncounterBuilder;

    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder, Episodes parser, BhrutCsvHelper csvHelper, boolean isChildEncounter) throws Exception {

        //every encounter has the following common attributes
        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        CsvCell episodeNumCell = parser.getEpiNum();
        if (!patientIdCell.isEmpty()) {
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, patientIdCell.getString());
            if (builder.isIdMapped()) {
                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }

            builder.setPatient(patientReference);
        }
        if (!episodeNumCell.isEmpty()) {

            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeNumCell.getString());
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, csvHelper);
            }

            builder.setEpisodeOfCare(episodeReference);
        }
        CsvCell episodeConsultantCodeCell = parser.getEpisodeConsultantCode();
        if (!episodeConsultantCodeCell.isEmpty()) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, episodeConsultantCodeCell.getString());
            if (builder.isIdMapped()) {
                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }

            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }
        CsvCell admissionHospitalCode = parser.getAdmissionHospitalCode();
        if (!admissionHospitalCode.isEmpty()) {
            Reference organizationReference
                    = ReferenceHelper.createReference(ResourceType.Organization, admissionHospitalCode.getString());
            if (builder.isIdMapped()) {
                organizationReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, csvHelper);
            }

            builder.setServiceProvider(organizationReference);
        }

        if (isChildEncounter) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
            parentEncounter
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);

            builder.setPartOf(parentEncounter);
        }

    }

    private static void deleteChildResources(Episodes parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             BhrutCsvHelper csvHelper,
                                             String version) throws Exception {
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

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }
}
