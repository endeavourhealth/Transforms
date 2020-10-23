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
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }
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
        encounterBuilder.setId(parser.getId().getString());

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

        //Create ParentEncounterBuilder
        EncounterBuilder encounterBuilder = createEncountersParentMinimum(parser, fhirResourceFiler, csvHelper);
        Reference patientReference2 = csvHelper.createPatientReference(patientIdCell);
        if (encounterBuilder.isIdMapped()) {
            IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference2, fhirResourceFiler);
        }
        encounterBuilder.setPatient(patientReference2, patientIdCell);

        CsvCell admissionHospitalCodeCell = parser.getAdmissionHospitalCode();
        Reference organisationReference;
                organisationReference = csvHelper.createOrganisationReference(admissionHospitalCodeCell.getString());

        if (encounterBuilder.isIdMapped()) {
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
        }
        encounterBuilder.setServiceProvider(organisationReference);


        CsvCell admissionConsultantCodeCell = parser.getAdmissionConsultantCode();
        Reference practitionerReference = null;
        if (!admissionConsultantCodeCell.isEmpty()) {
            practitionerReference = csvHelper.createPractitionerReference(admissionConsultantCodeCell.getString());
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.CONSULTANT, admissionConsultantCodeCell);
        }

        if (!parser.getDischargeConsultantCode().isEmpty()) {
            CsvCell dischargeConsultant = parser.getDischargeConsultantCode();
            Reference discharger = csvHelper.createPractitionerReference(dischargeConsultant.getString());
            if (encounterBuilder.isIdMapped()) {
                discharger = IdHelper.convertLocallyUniqueReferenceToEdsReference(discharger, csvHelper);
            }
            encounterBuilder.addParticipant(discharger, EncounterParticipantType.DISCHARGER, dischargeConsultant);
        }
        //create an Encounter reference for the procedures and conditions to use
        Reference thisEncounter = csvHelper.createEncounterReference(idCell.getString(), patientIdCell.getString());
        if (encounterBuilder.isIdMapped()) {
            thisEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(thisEncounter, csvHelper);
        }

        //Primary Diagnosis
        CsvCell primaryDiagnosisCodeCell = parser.getPrimaryDiagnosisCode();
        if (!primaryDiagnosisCodeCell.isEmpty()) {

            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0", idCell);
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            conditionBuilder.setPatient(newPatientReference, patientIdCell);
            conditionBuilder.setAsProblem(false);
            conditionBuilder.setEncounter(thisEncounter, idCell);
            if (!admissionConsultantCodeCell.isEmpty()) {
                Reference practitionerReference2 = csvHelper.createPractitionerReference(admissionConsultantCodeCell.getString());
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
            CsvCell primaryProcCode = parser.getPrimaryProcedureCode();
            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
            procedureBuilder.setPatient(newPatientReference, patientIdCell);
            procedureBuilder.setId(idCell.getString() + ":Procedure:0", idCell);
            procedureBuilder.setIsPrimary(true);
            Reference encounterReference = csvHelper.createEncounterReference(idCell.getString(), patientIdCell.getString());
            procedureBuilder.setEncounter(encounterReference, idCell);

            if (!admissionConsultantCodeCell.isEmpty()) {
                Reference practitionerReference2 = csvHelper.createPractitionerReference(admissionConsultantCodeCell.getString());
                procedureBuilder.addPerformer(practitionerReference2, admissionConsultantCodeCell);
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
        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();
        //set the extensions
        if (!parser.getPatientClassCode().isEmpty()) {
            CsvCell patientClassCode = parser.getPatientClassCode();
            CsvCell patientClass = parser.getPatientClass();
            if (!patientClass.isEmpty()) {
                csvHelper.addParmIfNotNullNhsdd(BhrutCsvToFhirTransformer.IM_PATIENT_CLASS,
                        patientClass.getString(),patientClass,parametersBuilder, BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }
        }
        if (!parser.getAdmissionSourceCode().isEmpty()) {
            CsvCell adminSourceCode = parser.getAdmissionSourceCode();
            if (!adminSourceCode.isEmpty()) {
                csvHelper.addParmIfNotNullNhsdd( BhrutCsvToFhirTransformer.IM_ADMIN_SOURCE_CODE,
                        adminSourceCode.getString(), adminSourceCode, parametersBuilder, BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }
        }
        if (!parser.getAdmissionMethodCode().isEmpty()) {
            CsvCell admissionMethodCode = parser.getAdmissionMethodCode();
            if (!admissionMethodCode.isEmpty()) {
                csvHelper.addParmIfNotNullNhsdd( BhrutCsvToFhirTransformer.IM_ADMIN_METHOD_CODE,
                        admissionMethodCode.getString(), admissionMethodCode, parametersBuilder, BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }
        }
        if (!parser.getAdmissionWardCode().isEmpty()) {
            CsvCell admissionWardCode = parser.getAdmissionWardCode();
            csvHelper.addParmIfNotNullNhsdd( BhrutCsvToFhirTransformer.IM_ADMISSION_WARD_CODE,
                    admissionWardCode.getString(), admissionWardCode, parametersBuilder, BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }
        if (!parser.getDischargeWardCode().isEmpty()) {
            CsvCell dischargeWardCode = parser.getDischargeWardCode();
            csvHelper.addParmIfNotNullNhsdd( BhrutCsvToFhirTransformer.IM_DISCHARGE_WARD_CODE,
                    dischargeWardCode.getString(), dischargeWardCode, parametersBuilder, BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }
        if (!parser.getDischargeMethodCode().isEmpty()) {
            CsvCell dischargeMethodCode = parser.getDischargeMethodCode();
            csvHelper.addParmIfNotNullNhsdd( BhrutCsvToFhirTransformer.IM_DISCHARGE_METHOD_CODE,
                    dischargeMethodCode.getString(), dischargeMethodCode, parametersBuilder, BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);

        }
        if (!parser.getDischargeDestinationCode().isEmpty()) {
            CsvCell dischargeDestCode = parser.getDischargeDestinationCode();
            csvHelper.addParmIfNotNullNhsdd( BhrutCsvToFhirTransformer.IM_DISCHARGE_DEST_CODE,
                    dischargeDestCode.getString(), dischargeDestCode, parametersBuilder, BhrutCsvToFhirTransformer.IM_SPELLS_TABLE_NAME);
            }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), !encounterBuilder.isIdMapped(), encounterBuilder);
    }

    private static void createEpisodeOfcare(Spells parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper,
                    String version, EpisodeOfCareBuilder episodeOfCareBuilder) throws Exception {

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

        csvHelper.getEpisodeOfCareCache().cacheEpisodeOfCareBuilder(id, episodeOfCareBuilder);

    }

    private static void deleteChildResources(Spells parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             BhrutCsvHelper csvHelper,
                                             String version) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        CsvCell idCell = parser.getId();
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();


        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {

            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0", idCell);
            Reference patientReference = csvHelper.createPatientReference(patientIdCell);
            conditionBuilder.setPatient(patientReference, patientIdCell);
            conditionBuilder.setDeletedAudit(dataUpdateStatusCell);
            conditionBuilder.setAsProblem(false);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
        }
        if (!parser.getPrimaryProcedureCode().isEmpty()) {

            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setId(idCell.getString() + ":Procedure:0", idCell);
            Reference patientReference = csvHelper.createPatientReference(patientIdCell);
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setDeletedAudit(dataUpdateStatusCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
        }
    }

    private static EncounterBuilder createEncountersParentMinimum(Spells parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        parentTopEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
        CsvCell dischargeDateCell = parser.getDischargeDttm();
        CsvCell admissionDateCell = parser.getAdmissionDttm();

        parentTopEncounterBuilder.setId(parser.getId().getString());
        if (!admissionDateCell.isEmpty()) {
            parentTopEncounterBuilder.setPeriodStart(parser.getAdmissionDttm().getDateTime(), parser.getAdmissionDttm());
        }
        if (!dischargeDateCell.isEmpty()) {
            parentTopEncounterBuilder.setPeriodEnd(parser.getDischargeDttm().getDateTime(), parser.getDischargeDttm());
        }
        if (!dischargeDateCell.isEmpty()) {
            parentTopEncounterBuilder.setPeriodEnd(dischargeDateCell.getDateTime());
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient Spell");

        setCommonEncounterAttributes(parentTopEncounterBuilder, parser, csvHelper, false);

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
