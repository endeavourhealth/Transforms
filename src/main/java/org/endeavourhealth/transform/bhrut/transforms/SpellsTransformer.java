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
import org.endeavourhealth.transform.bhrut.schema.Spells;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

                try {
                    Spells spellsParser = (Spells) parser;

                    if (spellsParser.getDataUpdateStatus().getString().equalsIgnoreCase("Deleted")) {
                        deleteResource(spellsParser, fhirResourceFiler, csvHelper, version);
                        deleteEncounterAndChildren(spellsParser, fhirResourceFiler, csvHelper);
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

        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        encounterBuilder.setDeletedAudit(dataUpdateStatusCell);

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
        EpisodeOfCareBuilder episodeOfCareBuilder = csvHelper.getEpisodeOfCareCache().getOrCreateEpisodeOfCareBuilder(idCell, csvHelper, fhirResourceFiler);
        createEpisodeOfcare(parser, fhirResourceFiler, csvHelper, version, episodeOfCareBuilder);
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        //Create ParentEncounterBuilder
        createEncountersParentMinimum(parser, fhirResourceFiler, csvHelper);
        String parentEncounterId = idCell.getString();
        Encounter existingParentEncounter
                = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, parentEncounterId);

        //if existingParentEncounter is null a new Parent would be created.
        EncounterBuilder encounterBuilder = new EncounterBuilder(existingParentEncounter);
        createSubEncounters(parser, encounterBuilder, fhirResourceFiler, csvHelper);

        //EncounterBuilder encounterBuilder = new EncounterBuilder();
        //encounterBuilder.setId(idCell.getString());
        //encounterBuilder.setPatient(patientReference, patientIdCell);
        //encounterBuilder.setPeriodStart(parser.getAdmissionDttm().getDateTime(), parser.getAdmissionDttm());
        //encounterBuilder.setPeriodEnd(parser.getDischargeDttm().getDateTime(), parser.getDischargeDttm());

        CsvCell admissionHospitalCodeCell = parser.getAdmissionHospitalCode();
        if (!admissionHospitalCodeCell.isEmpty()) {
            Reference organisationReference = csvHelper.createOrganisationReference(admissionHospitalCodeCell.getString());
            if (encounterBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
            }
            encounterBuilder.setServiceProvider(organisationReference);
        }

        CsvCell admissionConsultantCodeCell = parser.getAdmissionConsultantCode();
        Reference practitionerReference = null;
        if (!admissionConsultantCodeCell.isEmpty()) {
            practitionerReference = csvHelper.createPractitionerReference(admissionConsultantCodeCell.getString());
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.CONSULTANT, admissionConsultantCodeCell);
        }
        //CsvCell admittingConsultant = parser.getAdmissionConsultantCode();
        //Reference practitioner = csvHelper.createPractitionerReference(admittingConsultant.getString());
        //encounterBuilder.addParticipant(practitioner, EncounterParticipantType.CONSULTANT, admittingConsultant);

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
            if (!practitionerReference.isEmpty()) {
                conditionBuilder.setClinician(practitionerReference, admissionConsultantCodeCell);
            }
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
            if (!practitionerReference.isEmpty()) {
                procedureBuilder.addPerformer(practitionerReference, admissionConsultantCodeCell);
            }
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

    private static void createEpisodeOfcare(Spells parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper, String version, EpisodeOfCareBuilder episodeOfCareBuilder) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        CsvCell id = parser.getId();

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
        if (odsCodeCell != null) {
            Reference organisationReference = csvHelper.createOrganisationReference(odsCodeCell.getString());
            // if episode already ID mapped, get the mapped ID for the org
            if (episodeOfCareBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeOfCareBuilder.setManagingOrganisation(organisationReference, odsCodeCell);
        } else {
            //v1 uses service details
            UUID serviceId = parser.getServiceId();
            Reference organisationReference = csvHelper.createOrganisationReference(serviceId.toString());
            // if episode already ID mapped, get the mapped ID for the org
            if (episodeOfCareBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeOfCareBuilder.setManagingOrganisation(organisationReference);
        }
        CsvCell consultantCodeCell = parser.getAdmissionConsultantCode();
        if (consultantCodeCell != null && !consultantCodeCell.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(consultantCodeCell.getString());
            if (episodeOfCareBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }
            episodeOfCareBuilder.setCareManager(practitionerReference, consultantCodeCell);
        }
        //Extension
        if (!parser.getPatientClassCode().isEmpty()) {
            CsvCell patientClassCode = parser.getPatientClassCode();
            CsvCell patientClass = parser.getPatientClass();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(episodeOfCareBuilder, CodeableConceptBuilder.Tag.Encounter_Patient_Class_Other);
            cc.setText(patientClass.getString(), patientClass);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(patientClassCode.getString(), patientClassCode);
            cc.setCodingDisplay(patientClass.getString());
            episodeOfCareBuilder.setPriority(patientClassCode.getString(), patientClassCode);
        }

        csvHelper.getEpisodeOfCareCache().returnEpisodeOfCareBuilder(id, episodeOfCareBuilder);

    }

    private static void deleteChildResources(Spells parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             BhrutCsvHelper csvHelper,
                                             String version) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {

            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0", idCell);

            conditionBuilder.setPatient(patientReference, patientIdCell);
            conditionBuilder.setDeletedAudit(dataUpdateStatusCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
        }
        if (!parser.getPrimaryProcedureCode().isEmpty()) {

            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setId(idCell.getString() + ":Procedure:0", idCell);
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setDeletedAudit(dataUpdateStatusCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
        }
    }

    private static void createEncountersParentMinimum(Spells parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        parentTopEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        parentTopEncounterBuilder.setId(parser.getId().toString());
        parentTopEncounterBuilder.setPeriodStart(parser.getAdmissionDttm().getDateTime(), parser.getAdmissionDttm());
        parentTopEncounterBuilder.setPeriodEnd(parser.getDischargeDttm().getDateTime(), parser.getDischargeDttm());
        Date dischargeDate = parser.getDischargeDttm().getDateTime();

        if (dischargeDate != null) {
            parentTopEncounterBuilder.setPeriodEnd(dischargeDate);
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient Admission");

        setCommonEncounterAttributes(parentTopEncounterBuilder, parser, csvHelper);
        fhirResourceFiler.savePatientResource(null, parentTopEncounterBuilder);

    }

    private static void createSubEncounters(Spells parser, EncounterBuilder parentEncounterBuilder, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        ContainedListBuilder existingEncounterList = new ContainedListBuilder(parentEncounterBuilder);

        EncounterBuilder admissionEncounterBuilder = new EncounterBuilder();
        admissionEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        String admissionEncounterId = parser.getId() + ":01:IP:Admission";
        admissionEncounterBuilder.setId(admissionEncounterId);

        admissionEncounterBuilder.setPeriodStart(parser.getAdmissionDttm().getDateTime(), parser.getAdmissionDttm());
        admissionEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        CodeableConceptBuilder codeableConceptBuilderAdmission
                = new CodeableConceptBuilder(admissionEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilderAdmission.setText("Inpatient Admission");
        setCommonEncounterAttributes(admissionEncounterBuilder, parser, csvHelper);

        //add in additional extended data as Parameters resource with additional extension
        ContainedParametersBuilder containedParametersBuilderMain
                = new ContainedParametersBuilder(admissionEncounterBuilder);
        containedParametersBuilderMain.removeContainedParameters();

        CsvCell admissionMethodCodeCell = parser.getAdmissionMethodCode();
        if (!admissionMethodCodeCell.isEmpty()) {
            containedParametersBuilderMain.addParameter("ip_admission_method", "" + admissionMethodCodeCell.getString());
        }
        CsvCell admissionSourceCodeCell = parser.getAdmissionSourceCode();
        if (!admissionSourceCodeCell.isEmpty()) {
            containedParametersBuilderMain.addParameter("ip_admission_source", "" + admissionSourceCodeCell.getString());
        }

        CsvCell patientClassCodeCell = parser.getPatientClassCode();
        if (!patientClassCodeCell.isEmpty()) {
            containedParametersBuilderMain.addParameter("patient_classification", "" + patientClassCodeCell.getString());
        }

        //and link the parent to this new child encounter
        Reference childAdmissionRef = ReferenceHelper.createReference(ResourceType.Encounter, admissionEncounterId);
        existingEncounterList.addReference(childAdmissionRef);

        //save the admission encounter
        fhirResourceFiler.savePatientResource(null, admissionEncounterBuilder);

        CsvCell spellDischargeDateCell = parser.getDischargeDttm();
        if (!spellDischargeDateCell.isEmpty()) {

            //create new additional Discharge encounter event to link to the top level parent
            EncounterBuilder dischargeEncounterBuilder = new EncounterBuilder();
            dischargeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

            String dischargeEncounterId = parser.getId() + ":01:IP:Discharge";
            dischargeEncounterBuilder.setId(dischargeEncounterId);
            dischargeEncounterBuilder.setPeriodStart(spellDischargeDateCell.getDate());
            dischargeEncounterBuilder.setPeriodEnd(spellDischargeDateCell.getDate());
            dischargeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

            CodeableConceptBuilder codeableConceptBuilderDischarge
                    = new CodeableConceptBuilder(dischargeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderDischarge.setText("Inpatient Discharge");

            setCommonEncounterAttributes(dischargeEncounterBuilder, parser, csvHelper);
            ContainedParametersBuilder containedParametersBuilderDischarge
                    = new ContainedParametersBuilder(dischargeEncounterBuilder);
            containedParametersBuilderDischarge.removeContainedParameters();


            if (!parser.getDischargeMethodCode().isEmpty()) {
                containedParametersBuilderMain.addParameter("ip_discharge_method", "" + parser.getDischargeMethodCode().getString());
            }

            if (!parser.getDischargeDestinationCode().isEmpty()) {
                CsvCell dischargeDestCode = parser.getDischargeDestinationCode();
                containedParametersBuilderMain.addParameter("ip_discharge_destination", "" + dischargeDestCode.getString());
            }

            //and link the parent to this new child encounter
            Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, dischargeEncounterId);
            existingEncounterList.addReference(childDischargeRef);

            //save the discharge encounter builder
            fhirResourceFiler.savePatientResource(null, dischargeEncounterBuilder);

        }
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     Spells parser,
                                                     BhrutCsvHelper csvHelper) throws Exception {

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


        if (patientIdCell != null) {

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
        if (!admissionHospitalCode.isEmpty()) {
            Reference organizationReference
                    = ReferenceHelper.createReference(ResourceType.Organization, admissionHospitalCode.getString());
            if (builder.isIdMapped()) {

                organizationReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, csvHelper);
            }
            builder.setServiceProvider(organizationReference);
        }

        if (!patientIdCell.isEmpty()) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
            if (builder.isIdMapped()) {

                parentEncounter
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            }
            builder.setPartOf(parentEncounter);
        }

    }

    private static void deleteEncounterAndChildren(Spells parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        Encounter existingParentEncounter
                = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, parser.getId().getString());

        EncounterBuilder parentEncounterBuilder
                = new EncounterBuilder(existingParentEncounter);

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

                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing child Encounter: {} for deletion", childEncounter.getId());
                }
            }

            //finally, delete the top level parent
            fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

        } else {
            TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", parser.getId().getString());
        }

    }

}
