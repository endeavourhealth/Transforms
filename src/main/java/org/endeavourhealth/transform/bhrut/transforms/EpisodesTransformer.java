package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
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
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }
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

        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        //Create ParentEncounterBuilder
        EncounterBuilder encounterBuilder = createEncountersParentMinimum(parser, fhirResourceFiler, csvHelper);
        createSubEncounters(parser, encounterBuilder, fhirResourceFiler, csvHelper);
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);

        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        if (dataUpdateStatusCell.getString().equalsIgnoreCase("Deleted")) {

            encounterBuilder.setDeletedAudit(dataUpdateStatusCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);

            deleteChildResources(parser, fhirResourceFiler, csvHelper, version);
            deleteEncounterAndChildren(parser, fhirResourceFiler, csvHelper);
            return;
        }

        //the class is Inpatient, i.e. Inpatient Episode
        encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
        //encounterBuilder.setPeriodStart(parser.getEpisodeStartDttm().getDateTime(), parser.getEpisodeStartDttm());
        //encounterBuilder.setPeriodEnd(parser.getEpisodeEndDttm().getDateTime(), parser.getEpisodeEndDttm());

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

        // CsvCell episodeConsultantCell = parser.getEpisodeConsultantCode();
        // Reference episodePractitioner = csvHelper.createPractitionerReference(episodeConsultantCell.getString());
        // encounterBuilder.addParticipant(episodePractitioner, EncounterParticipantType.CONSULTANT, episodeConsultantCell);

        CsvCell episodeConsultantCodeCell = parser.getEpisodeConsultantCode();
        Reference practitionerReference = null;
        if (!episodeConsultantCodeCell.isEmpty()) {
            practitionerReference = csvHelper.createPractitionerReference(episodeConsultantCodeCell.getString());
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.CONSULTANT, episodeConsultantCodeCell);
        }

        //the parent inpatient spell encounter
        // Reference spellEncounter
        //         = csvHelper.createEncounterReference(parser.getIpSpellExternalId().getString(), patientReference.getId());
        // encounterBuilder.setPartOf(spellEncounter);

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
        Reference patientEncReference = csvHelper.createPatientReference(patientIdCell);
        Reference thisEncounter
                = csvHelper.createEncounterReference(parser.getId().getString(), patientEncReference.getId());

        //its rare that there is no primary diagnosis, but check just in case
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {

            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString() + "Condition:0");
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            condition.setPatient(newPatientReference, patientIdCell);
            DateTimeType dtt = new DateTimeType(parser.getPrimdiagDttm().getDateTime());
            condition.setOnset(dtt, parser.getPrimdiagDttm());
            condition.setIsPrimary(true);
            condition.setAsProblem(false);
            condition.setEncounter(thisEncounter, parser.getId());
            if (!episodeConsultantCodeCell.isEmpty()) {
                Reference practitionerReference2 = csvHelper.createPractitionerReference(episodeConsultantCodeCell.getString());
                condition.setClinician(practitionerReference2, episodeConsultantCodeCell);
            }
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
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            proc.setPatient(newPatientReference, patientIdCell);
            proc.setIsPrimary(true);
            proc.setEncounter(thisEncounter, idCell);
            if (!parser.getPrimaryProcedureDate().isEmpty()) {
                DateTimeType dttp = new DateTimeType(parser.getPrimaryProcedureDate().getDateTime());
                proc.setPerformed(dttp, parser.getPrimaryProcedureDate());
            }

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
                fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

            } else {
                TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", spellExternalIdCell);
            }
        }


    }

    private static void createSubEncounters(Episodes parser, EncounterBuilder existingParentEncounterBuilder, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        ContainedListBuilder existingParentEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

        EncounterBuilder admissionEncounterBuilder = null;
        EncounterBuilder dischargeEncounterBuilder = null;
        CsvCell epiNumCell = parser.getEpiNum();
        if (!epiNumCell.isEmpty() && epiNumCell.getString().equalsIgnoreCase("01")) {

            admissionEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
            if (!epiNumCell.isEmpty() && epiNumCell.getString().equalsIgnoreCase("01")) {
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
                    containedParametersBuilderAdmission.addParameter("DM_hasAdministrativeCategoryCode", "CM_AdminCat" + adminCategoryCodeCe.getString());
                }
                CsvCell admissionMethodCodeCell = parser.getAdmissionMethodCode();
                if (!admissionMethodCodeCell.isEmpty()) {
                    containedParametersBuilderAdmission.addParameter("ip_admission_method", "" + admissionMethodCodeCell.getString());
                }
                CsvCell admissionSourceCodeCell = parser.getAdmissionSourceCode();
                if (!admissionSourceCodeCell.isEmpty()) {
                    containedParametersBuilderAdmission.addParameter("ip_admission_source", "" + admissionSourceCodeCell);
                }
                CsvCell patientClassCodeCell = parser.getPatientClassCode();
                if (!patientClassCodeCell.isEmpty()) {
                    containedParametersBuilderAdmission.addParameter("patient_classification", "" + patientClassCodeCell.getString());
                }
                //if the 01 episode has an episode end date, set the admission end date
                CsvCell episodeEndDateCell = parser.getEpisodeEndDttm();
                if (!episodeEndDateCell.isEmpty()) {
                    admissionEncounterBuilder.setPeriodEnd(episodeEndDateCell.getDateTime());
                    admissionEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
                }
                //and link the parent to this new child encounter
                Reference childAdmissionRef = ReferenceHelper.createReference(ResourceType.Encounter, admissionEncounterId);
                if (existingParentEncounterBuilder.isIdMapped()) {

                    childAdmissionRef
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAdmissionRef, csvHelper);
                }
                existingParentEncounterList.addReference(childAdmissionRef);

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

                    CsvCell dischargeMethodCodeCell = parser.getDischargeMethod();
                    if (!dischargeMethodCodeCell.isEmpty()) {
                        containedParametersBuilderDischarge.addParameter("ip_discharge_method", "" + dischargeMethodCodeCell.getString());
                    }
                    CsvCell dischargeDestinationCodeCell = parser.getDischargeDestinationCode();
                    if (!dischargeDestinationCodeCell.isEmpty()) {
                        containedParametersBuilderDischarge.addParameter("ip_discharge_destination", "" + dischargeDestinationCodeCell.getString());
                    }
                    //and link the parent to this new child encounter
                    Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, dischargeEncounterId);
                    if (existingParentEncounterBuilder.isIdMapped()) {

                        childDischargeRef
                                = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAdmissionRef, csvHelper);
                    }

                    existingParentEncounterList.addReference(childDischargeRef);
                }
            }
            //these are the 01, 02, 03, 04 subsequent episodes where activity happens, maternity, wards change etc.
            //also, critical care child encounters link back to these as their parents via their own transform
            EncounterBuilder episodeEncounterBuilder = new EncounterBuilder();
            episodeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

            String episodeEncounterId = parser.getId().getString() + ":" + parser.getEpiNum() + ":IP:Episode";
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

            //and link the parent to this new child encounter
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

            CsvCell episodeStartWardCodeCell = parser.getEpisodeStartWardCode();
            if (!episodeStartWardCodeCell.isEmpty()) {
                containedParametersBuilder.addParameter("ip_episode_start_ward", "" + episodeStartWardCodeCell.getString());
            }
            CsvCell episodeEndWardCodeCell = parser.getEpisodeEndWardCode();
            if (!episodeEndWardCodeCell.isEmpty()) {
                containedParametersBuilder.addParameter("ip_episode_end_ward", "" + episodeEndWardCodeCell.getString());
            }
            //save the existing parent encounter here with the updated child refs added during this method, then the sub encounters
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);

            //then save the child encounter builders if they are set
            if (admissionEncounterBuilder != null) {
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), admissionEncounterBuilder);
            }
            if (dischargeEncounterBuilder != null) {
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), dischargeEncounterBuilder);
            }
            //finally, save the episode encounter which always exists
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), episodeEncounterBuilder);
        }

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

        CsvCell spellExternalIdCell = parser.getIpSpellExternalId();
        if (isChildEncounter) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, spellExternalIdCell.getString());
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
                    procedureBuilder.setPatient(patientReference, patientIdCell);
                    procedureBuilder.setDeletedAudit(dataUpdateStatusCell);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }
}
