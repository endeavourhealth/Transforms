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
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.bhrut.schema.Spells;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class SpellsTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SpellsTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Spells.class);

        if (parser != null) {
            while (parser.nextRecord()) {
                if (!csvHelper.processRecordFilteringOnPatientId(parser)) {
                    continue;
                }
                try {
                    Spells spellsParser = (Spells) parser;

                    CsvCell dataUpdateStatusCell = spellsParser.getDataUpdateStatus();
                    if (dataUpdateStatusCell.getString().equalsIgnoreCase("Deleted")) {

                        deleteResources(spellsParser, fhirResourceFiler, csvHelper);
                    } else {

                        createResources(spellsParser, fhirResourceFiler, csvHelper);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void deleteResources(Spells parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper) throws Exception {

        CsvCell idCell = parser.getId();
        Encounter existingParentSpellEncounter
                = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, idCell.getString());

        if (existingParentSpellEncounter != null) {

            EncounterBuilder parentEncounterBuilder = new EncounterBuilder(existingParentSpellEncounter);
            CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();

            //has this encounter got child encounters, i.e. admission and discharge and any episode encounters?
            if (existingParentSpellEncounter.hasContained()) {

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

                        EncounterBuilder childEncounterBuilder = new EncounterBuilder(childEncounter);
                        childEncounterBuilder.setDeletedAudit(dataUpdateStatusCell);
                        fhirResourceFiler.deletePatientResource(null, false, childEncounterBuilder);
                    } else {

                        TransformWarnings.log(LOG, csvHelper, "Cannot find existing child Encounter: {} for deletion", comps.getId());
                    }
                }
            }
            //finally, delete the top level parent spell encounter
            parentEncounterBuilder.setDeletedAudit(dataUpdateStatusCell);
            fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

        } else {
            TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", idCell);
        }

        //then delete the linked clinical resources
        deleteClinicalResources(parser, fhirResourceFiler, csvHelper);
    }

    private static void deleteClinicalResources(Spells parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                BhrutCsvHelper csvHelper) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();

        CsvCell primaryDiagnosisCode = parser.getPrimaryDiagnosisCode();
        if (!primaryDiagnosisCode.isEmpty()) {

            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0", idCell);
            Reference patientReference = csvHelper.createPatientReference(patientIdCell);
            conditionBuilder.setPatient(patientReference, patientIdCell);
            conditionBuilder.setDeletedAudit(dataUpdateStatusCell);
            conditionBuilder.setAsProblem(false);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, conditionBuilder);
        }

        CsvCell primaryProcedureCode = parser.getPrimaryProcedureCode();
        if (!primaryProcedureCode.isEmpty()) {

            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setId(idCell.getString() + ":Procedure:0", idCell);
            Reference patientReference = csvHelper.createPatientReference(patientIdCell);
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setDeletedAudit(dataUpdateStatusCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
        }
    }

    public static void createResources(Spells parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper) throws Exception {

        CsvCell idCell = parser.getId();

        //the episode of care is for the hospital spell
        createEpisodeOfCare(parser, fhirResourceFiler, csvHelper);

        //Create Top level Parent Spell Encounter
        EncounterBuilder parentEncounterBuilder
                = createEncountersParentMinimum(parser, fhirResourceFiler, csvHelper);

        ContainedListBuilder existingParentEncounterList = new ContainedListBuilder(parentEncounterBuilder);

        EncounterBuilder admissionEncounterBuilder = new EncounterBuilder();
        admissionEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
        String admissionEncounterId = idCell.getString() + ":01:IP:Admission";
        admissionEncounterBuilder.setId(admissionEncounterId);

        //Note: the admission encounter finished status and end date are set during episodes transform
        CsvCell spellAdmissionDateCell = parser.getAdmissionDttm();
        admissionEncounterBuilder.setPeriodStart(spellAdmissionDateCell.getDateTime());
        admissionEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

        CodeableConceptBuilder codeableConceptBuilderAdmission
                = new CodeableConceptBuilder(admissionEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilderAdmission.setText("Inpatient Admission");

        setCommonEncounterAttributes(admissionEncounterBuilder, parser, csvHelper, true);

        //add in additional extended data as Parameters resource with additional extension
        ContainedParametersBuilder containedParametersBuilderAdmission
                = new ContainedParametersBuilder(admissionEncounterBuilder);
        containedParametersBuilderAdmission.removeContainedParameters();

        CsvCell patientClassCodeCell = parser.getPatientClassCode();
        if (!patientClassCodeCell.isEmpty()) {

            BhrutCsvHelper.addParmIfNotNullNhsdd(BhrutCsvToFhirTransformer.IM_PATIENT_CLASS,
                    patientClassCodeCell.getString(),
                    patientClassCodeCell,
                    containedParametersBuilderAdmission,
                    BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
        }
        CsvCell admissionSourceCodeCell = parser.getAdmissionSourceCode();
        if (!admissionSourceCodeCell.isEmpty()) {
                BhrutCsvHelper.addParmIfNotNullNhsdd( BhrutCsvToFhirTransformer.IM_ADMISSION_SOURCE_CODE,
                        admissionSourceCodeCell.getString(),
                        admissionSourceCodeCell,
                        containedParametersBuilderAdmission,
                        BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
        }
        CsvCell admissionMethodCodeCell = parser.getAdmissionMethodCode();
        if (!admissionMethodCodeCell.isEmpty()) {

            BhrutCsvHelper.addParmIfNotNullNhsdd( BhrutCsvToFhirTransformer.IM_ADMISSION_METHOD_CODE,
                    admissionMethodCodeCell.getString(),
                    admissionMethodCodeCell,
                    containedParametersBuilderAdmission,
                    BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
        }
        CsvCell admissionWardCodeCell = parser.getAdmissionWardCode();
        if (!admissionWardCodeCell.isEmpty()) {

            BhrutCsvHelper.addParmIfNotNullJson(BhrutCsvToFhirTransformer.IM_ADMISSION_WARD_CODE,
                    admissionWardCodeCell.getString(),
                    admissionWardCodeCell,
                    containedParametersBuilderAdmission,
                    BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
        }

        //and link the parent to this new child admissions encounter
        Reference childAdmissionRef = ReferenceHelper.createReference(ResourceType.Encounter, admissionEncounterId);
        childAdmissionRef = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAdmissionRef, csvHelper);
        existingParentEncounterList.addReference(childAdmissionRef);

        //if the main encounter has a discharge date create a linked Discharge encounter
        EncounterBuilder dischargeEncounterBuilder = null;
        CsvCell dischargeDateCell = parser.getDischargeDttm();
        if (!dischargeDateCell.isEmpty()) {
            //create new additional Discharge encounter event to link to the top level parent
            dischargeEncounterBuilder = new EncounterBuilder();
            dischargeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

            String dischargeEncounterId = idCell.getString() + ":01:IP:Discharge";
            dischargeEncounterBuilder.setId(dischargeEncounterId);
            dischargeEncounterBuilder.setPeriodStart(dischargeDateCell.getDateTime());
            dischargeEncounterBuilder.setPeriodEnd(dischargeDateCell.getDateTime());
            dischargeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

            CodeableConceptBuilder codeableConceptBuilderDischarge
                    = new CodeableConceptBuilder(dischargeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderDischarge.setText("Inpatient Discharge");

            setCommonEncounterAttributes(dischargeEncounterBuilder, parser, csvHelper, true);

            //replace the primary performer participant with the discharger is it exists
            CsvCell dischargeConsultantCodeCell = parser.getDischargeConsultantCode();
            if (!dischargeConsultantCodeCell.isEmpty()) {

                Reference discharger = csvHelper.createPractitionerReference(dischargeConsultantCodeCell.getString());
                if (dischargeEncounterBuilder.isIdMapped()) {
                    discharger = IdHelper.convertLocallyUniqueReferenceToEdsReference(discharger, csvHelper);
                }
                dischargeEncounterBuilder.addParticipant(discharger,
                        EncounterParticipantType.PRIMARY_PERFORMER,
                        true,
                        dischargeConsultantCodeCell);
            }

            //add in additional extended data as Parameters resource with additional extension
            ContainedParametersBuilder containedParametersBuilderDischarge
                    = new ContainedParametersBuilder(dischargeEncounterBuilder);
            containedParametersBuilderDischarge.removeContainedParameters();

            CsvCell dischargeWardCodeCell = parser.getDischargeWardCode();
            if (!dischargeWardCodeCell.isEmpty()) {

                BhrutCsvHelper.addParmIfNotNullJson(BhrutCsvToFhirTransformer.IM_DISCHARGE_WARD_CODE,
                        dischargeWardCodeCell.getString(),
                        dischargeWardCodeCell,
                        containedParametersBuilderDischarge,
                        BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }
            CsvCell dischargeMethodCodeCell = parser.getDischargeMethodCode();
            if (!dischargeMethodCodeCell.isEmpty()) {

                BhrutCsvHelper.addParmIfNotNullNhsdd(BhrutCsvToFhirTransformer.IM_DISCHARGE_METHOD_CODE,
                        dischargeMethodCodeCell.getString(),
                        dischargeMethodCodeCell,
                        containedParametersBuilderDischarge,
                        BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }
            CsvCell dischargeDestCodeCell = parser.getDischargeDestinationCode();
            if (!dischargeDestCodeCell.isEmpty()) {

                BhrutCsvHelper.addParmIfNotNullNhsdd(BhrutCsvToFhirTransformer.IM_DISCHARGE_DEST_CODE,
                        dischargeDestCodeCell.getString(),
                        dischargeDestCodeCell,
                        containedParametersBuilderDischarge,
                        BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }

            //and link the parent to this new child discharge encounter
            Reference childDischargeRef
                    = ReferenceHelper.createReference(ResourceType.Encounter, dischargeEncounterId);
            childDischargeRef = IdHelper.convertLocallyUniqueReferenceToEdsReference(childDischargeRef, csvHelper);
            existingParentEncounterList.addReference(childDischargeRef);
        }

        //Primary Diagnosis
        CsvCell primaryDiagnosisCodeCell = parser.getPrimaryDiagnosisCode();
        if (!primaryDiagnosisCodeCell.isEmpty()) {

            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0", idCell);

            CsvCell patientIdCell = parser.getPasId();
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            conditionBuilder.setPatient(newPatientReference, patientIdCell);
            conditionBuilder.setAsProblem(false);

            Reference thisParentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
            if (parentEncounterBuilder.isIdMapped()) {
                thisParentEncounter
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(thisParentEncounter, csvHelper);
            }
            conditionBuilder.setEncounter(thisParentEncounter, idCell);

            CsvCell admissionConsultantCodeCell = parser.getAdmissionConsultantCode();
            if (!admissionConsultantCodeCell.isEmpty()) {
                Reference practitionerReference2
                        = csvHelper.createPractitionerReference(admissionConsultantCodeCell.getString());
                conditionBuilder.setClinician(practitionerReference2, admissionConsultantCodeCell);
            }
            DateTimeType dtt = new DateTimeType(parser.getAdmissionDttm().getDateTime());
            conditionBuilder.setOnset(dtt, parser.getAdmissionDttm());

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            String icd10 = primaryDiagnosisCodeCell.getString().trim();
            codeableConceptBuilder.setCodingCode(icd10, primaryDiagnosisCodeCell);
            if (icd10.endsWith("X") || icd10.endsWith("D") || icd10.endsWith("A")) {  //X being a wildcard
                icd10 = icd10.substring(0, 3);
            }
            icd10 = TerminologyService.standardiseIcd10Code(icd10);
            String diagTerm = TerminologyService.lookupIcd10CodeDescription(icd10);
            if (Strings.isNullOrEmpty(diagTerm)) {

                throw new Exception("Failed to find diagnosis term for ICD 10 code " + icd10 + ".");
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

            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setId(idCell.getString() + ":Procedure:0", idCell);

            CsvCell patientIdCell = parser.getPasId();
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            procedureBuilder.setPatient(newPatientReference, patientIdCell);
            procedureBuilder.setIsPrimary(true);

            Reference thisParentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
            if (parentEncounterBuilder.isIdMapped()) {
                thisParentEncounter
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(thisParentEncounter, csvHelper);
            }
            procedureBuilder.setEncounter(thisParentEncounter, idCell);

            CsvCell admissionConsultantCodeCell = parser.getAdmissionConsultantCode();
            if (!admissionConsultantCodeCell.isEmpty()) {
                Reference practitionerReference2
                        = csvHelper.createPractitionerReference(admissionConsultantCodeCell.getString());
                procedureBuilder.addPerformer(practitionerReference2, admissionConsultantCodeCell);
            }

            DateTimeType dateTimeType = new DateTimeType(parser.getAdmissionDttm().getDateTime());
            procedureBuilder.setPerformed(dateTimeType, parser.getAdmissionDttm());

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);

            CsvCell primaryProcCode = parser.getPrimaryProcedureCode();
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

        //first, save the updated parent with links
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), parentEncounterBuilder);

        //then save the sub encounters if created,  admission is always created
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), admissionEncounterBuilder);

        if (dischargeEncounterBuilder != null) {
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), dischargeEncounterBuilder);
        }

//        //first, save the updated parent with links
//        fhirResourceFiler.savePatientResource(parser.getCurrentState(), !parentEncounterBuilder.isIdMapped(), parentEncounterBuilder);
//
//        //then save the sub encounters if created,  admission is always created
//        fhirResourceFiler.savePatientResource(parser.getCurrentState(), !admissionEncounterBuilder.isIdMapped(), admissionEncounterBuilder);
//
//        if (dischargeEncounterBuilder != null) {
//            fhirResourceFiler.savePatientResource(parser.getCurrentState(), !dischargeEncounterBuilder.isIdMapped(), dischargeEncounterBuilder);
//        }
    }

    private static void createEpisodeOfCare(Spells parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        CsvCell idCell = parser.getId();
        EpisodeOfCareBuilder episodeOfCareBuilder
                = csvHelper.getEpisodeOfCareCache().getOrCreateEpisodeOfCareBuilder(idCell, csvHelper, fhirResourceFiler);

        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        if (episodeOfCareBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        episodeOfCareBuilder.setPatient(patientReference, patientIdCell);

        CsvCell startDateTime = parser.getAdmissionDttm();
        if (!startDateTime.isEmpty()) {
            episodeOfCareBuilder.setRegistrationStartDate(startDateTime.getDateTime(), startDateTime);
        }

        CsvCell endDateTime = parser.getDischargeDttm();
        if (!endDateTime.isEmpty()) {
            episodeOfCareBuilder.setRegistrationEndDate(endDateTime.getDateTime(), endDateTime);
        }
        CsvCell odsCodeCell = parser.getAdmissionHospitalCode();
        Reference organisationReference;
        if (!odsCodeCell.isEmpty()) {
                organisationReference = csvHelper.createOrganisationReference(odsCodeCell.getString());
                     // if episode already ID mapped, get the mapped ID for the org
            if (episodeOfCareBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeOfCareBuilder.setManagingOrganisation(organisationReference, odsCodeCell);
        } else {
            //v1 uses service details
            UUID serviceId = parser.getServiceId();
            organisationReference = csvHelper.createOrganisationReference(serviceId.toString());
            // if episode already ID mapped, get the mapped ID for the org
            if (episodeOfCareBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeOfCareBuilder.setManagingOrganisation(organisationReference);
        }
        CsvCell consultantCodeCell = parser.getAdmissionConsultantCode();
        if (!consultantCodeCell.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(consultantCodeCell.getString());
            if (episodeOfCareBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }
            episodeOfCareBuilder.setCareManager(practitionerReference, consultantCodeCell);
        }

        csvHelper.getEpisodeOfCareCache().cacheEpisodeOfCareBuilder(idCell, episodeOfCareBuilder);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(),!episodeOfCareBuilder.isIdMapped(),episodeOfCareBuilder);
    }

    private static EncounterBuilder createEncountersParentMinimum(Spells parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        parentTopEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
        parentTopEncounterBuilder.setId(parser.getId().getString());

        CsvCell admissionDateCell = parser.getAdmissionDttm();
        parentTopEncounterBuilder.setPeriodStart(admissionDateCell.getDateTime(), parser.getAdmissionDttm());

        CsvCell dischargeDateCell = parser.getDischargeDttm();
        if (!dischargeDateCell.isEmpty()) {

            parentTopEncounterBuilder.setPeriodEnd(dischargeDateCell.getDateTime(), parser.getDischargeDttm());
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient");

        setCommonEncounterAttributes(parentTopEncounterBuilder, parser, csvHelper, false);

        //set the discharger practitioner if it exists
        CsvCell dischargeConsultantCodeCell = parser.getDischargeConsultantCode();
        if (!dischargeConsultantCodeCell.isEmpty()) {

            Reference discharger = csvHelper.createPractitionerReference(dischargeConsultantCodeCell.getString());
            if (parentTopEncounterBuilder.isIdMapped()) {
                discharger = IdHelper.convertLocallyUniqueReferenceToEdsReference(discharger, csvHelper);
            }
            parentTopEncounterBuilder.addParticipant(discharger, EncounterParticipantType.DISCHARGER, dischargeConsultantCodeCell);
        }

        return parentTopEncounterBuilder;
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     Spells parser,
                                                     BhrutCsvHelper csvHelper, boolean isChildEncounter) throws Exception {

        //every encounter has the following common attributes
        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();

        if (!patientIdCell.isEmpty()) {
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, patientIdCell.getString());
            if (builder.isIdMapped()) {
                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }

            builder.setPatient(patientReference);
        }

        //the episode of care is created prior using the same Id as the parent inpatient spell  encounter
        if (!idCell.isEmpty()) {

            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, idCell.getString());
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, csvHelper);
            }

            builder.setEpisodeOfCare(episodeReference);
        }

        CsvCell admissionConsultantCodeCell = parser.getAdmissionConsultantCode();
        if (!admissionConsultantCodeCell.isEmpty()) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, admissionConsultantCodeCell.getString());
            if (builder.isIdMapped()) {
                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }

            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }
        CsvCell admissionHospitalCode = parser.getAdmissionHospitalCode();
        Reference organizationReference;
        if (!admissionHospitalCode.isEmpty()) {
                organizationReference
                        = csvHelper.createOrganisationReference(admissionHospitalCode.getString());

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
}
