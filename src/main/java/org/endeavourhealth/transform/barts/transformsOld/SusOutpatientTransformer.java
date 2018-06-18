package org.endeavourhealth.transform.barts.transformsOld;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.SusResourceMapDalI;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.BartsSusHelper;
import org.endeavourhealth.transform.barts.schema.SusOutpatient;
import org.endeavourhealth.transform.barts.schema.Tails;
import org.endeavourhealth.transform.barts.schema.TailsRecord;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SusOutpatientTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatientTransformer.class);
    private static int entryCount = 0;

    /*
     *
     */
    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID,
                                 String[] allFiles) throws Exception {

        for (ParserI parser: parsers) {

            //parse corresponding tails file first
            String fName = FilenameUtils.getName(parser.getFilePath());
            String tailFilePath = BartsCsvToFhirTransformer.findTailFile(allFiles, "tailopa_DIS." + fName.split("_")[1].split("\\.")[1]);
            TailsPreTransformer.transform(version, new Tails(parser.getServiceId(), parser.getSystemId(), parser.getExchangeId(), version, tailFilePath));

            entryCount = 0;
            while (parser.nextRecord()) {
                try {
                    entryCount++;

                    SusOutpatient susOutpatient = (SusOutpatient)parser;

                    // CDS V6-2 Type 010 - Accident and Emergency CDS
                    // CDS V6-2 Type 020 - Outpatient CDS
                    // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
                    // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
                    // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
                    // CDS V6-2 Type 160 - Admitted Patient Care - Other Delivery Event CDS
                    // CDS V6-2 Type 180 - Admitted Patient Care - Unfinished Birth Episode CDS
                    // CDS V6-2 Type 190 - Admitted Patient Care - Unfinished General Episode CDS
                    // CDS V6-2 Type 200 - Admitted Patient Care - Unfinished Delivery Episode CDS
                    if (susOutpatient.getCDSRecordType() == 10 ||
                            susOutpatient.getCDSRecordType() == 20 ||
                            susOutpatient.getCDSRecordType() == 120 ||
                            susOutpatient.getCDSRecordType() == 130 ||
                            susOutpatient.getCDSRecordType() == 140 ||
                            susOutpatient.getCDSRecordType() == 160 ||
                            susOutpatient.getCDSRecordType() == 180 ||
                            susOutpatient.getCDSRecordType() == 190 ||
                            susOutpatient.getCDSRecordType() == 200) {
                        mapFileEntry(susOutpatient, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    /*
     *
     */
    public static void mapFileEntry(SusOutpatient parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    BartsCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID) throws Exception {

        LOG.debug("Current patient:" + parser.getLocalPatientId() + "/" + parser.getCDSUniqueID());

        TailsRecord tr = TailsPreTransformer.getTailsRecord(parser.getCDSUniqueID());

        ResourceId patientResourceId = null;
        ResourceId organisationResourceId = null;
        ResourceId episodeOfCareResourceId = null;
        ResourceId encounterResourceId = null;
        ResourceId gpResourceId = null;
        ResourceId gpPracticeResourceId = null;

        if ((parser.getICDPrimaryDiagnosis().length() > 0) || (parser.getOPCSPrimaryProcedureCode().length() > 0)) {
            // GP
            if (parser.getGP() != null && parser.getGP().length() > 0) {
                gpResourceId = getGPResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getGP());
                if (gpResourceId == null) {
                    gpResourceId = createGPResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getGP());
                }
            }

            // GP Practice
            if (parser.getGPPractice() != null && parser.getGPPractice().length() > 0) {
                gpPracticeResourceId = getGlobalOrgResourceId(parser.getGPPractice());
                if (gpPracticeResourceId == null) {
                    gpPracticeResourceId = createGlobalOrgResourceId(parser.getGPPractice());
                }
            }

            // Organisation
            Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
            organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);
            // Patient
            HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(HumanName.NameUse.OFFICIAL, parser.getPatientTitle(), parser.getPatientForename(), "", parser.getPatientSurname());
            Address fhirAddress = null;
            if (parser.getAddressType().compareTo("02") == 0) {
                fhirAddress = AddressHelper.createAddress(Address.AddressUse.HOME, parser.getAddress1(), parser.getAddress2(), parser.getAddress3(), parser.getAddress4(), parser.getAddress5(), parser.getPostCode());
            }

            Identifier patientIdentifier[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()))};

            CodeableConcept ethnicGroup = null;
            if (parser.getEthnicCategory() != null && parser.getEthnicCategory().length() > 0) {
                ethnicGroup = new CodeableConcept();
                ethnicGroup.addCoding().setCode(parser.getEthnicCategory()).setSystem(FhirExtensionUri.PATIENT_ETHNICITY).setDisplay(BartsSusHelper.getSusEthnicCategoryDisplay(parser.getEthnicCategory()));
            }

            patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), parser.getNHSNo(), name, fhirAddress, BartsSusHelper.convertSusGenderToFHIR(parser.getGender()), parser.getDOB(), organisationResourceId, null, patientIdentifier, gpResourceId, gpPracticeResourceId, ethnicGroup);

            if (tr != null && tr.getEpisodeId() != null && tr.getEpisodeId().length() > 0 && tr.getEncounterId() != null && tr.getEncounterId().length() > 0) {
                // EpisodeOfCare
                episodeOfCareResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEpisodeId());
                if (episodeOfCareResourceId == null) {
                    episodeOfCareResourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEpisodeId());
                }
                EpisodeOfCare.EpisodeOfCareStatus episodeStatus;
                if (parser.getOutcomeCode().compareTo("1") == 0) {
                    episodeStatus = EpisodeOfCare.EpisodeOfCareStatus.FINISHED;
                } else {
                    episodeStatus = EpisodeOfCare.EpisodeOfCareStatus.ACTIVE;
                }
                //Identifiers
                Identifier episodeIdentifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_EPISODE_ID).setValue(tr.getEpisodeId())};

                // TODO When partial update of EpisodeOfCare is implemented then the 'end date' should probably only be set once when/if creating the resource. It should not be updated here as HL7 is likely more accurate. Same might apply to 'start date'
                createEpisodeOfCare(parser.getCurrentState(), fhirResourceFiler, episodeOfCareResourceId, patientResourceId, organisationResourceId, episodeStatus, parser.getAppointmentDateTime(), parser.getExpectedLeavingDateTime(), episodeIdentifiers);

                // Encounter
                encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId());
                if (encounterResourceId == null) {
                    encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId());
                }
                //Identifiers
                Identifier encounterIdentifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_EPISODE_ID).setValue(tr.getEpisodeId()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_ID).setValue(tr.getEncounterId()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(tr.getFINNbr())};

                createEncounter(parser.getCurrentState(), fhirResourceFiler, patientResourceId, episodeOfCareResourceId, encounterResourceId, Encounter.EncounterState.FINISHED, parser.getAppointmentDateTime(), parser.getExpectedLeavingDateTime(), encounterIdentifiers, Encounter.EncounterClass.OUTPATIENT);
            }
        }

        // Map diagnosis codes ?
        if (parser.getICDPrimaryDiagnosis().length() > 0) {
            // Diagnosis
            mapDiagnosis(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId, tr);
        }

        // Map procedure codes ?
        if (parser.getOPCSPrimaryProcedureCode().length() > 0 && tr != null && tr.getEncounterId() != null && tr.getEncounterId().length() > 0) {
            // Procedure
            mapProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId, tr);
        }

    }


    /*
    Data line is of type Inpatient
    */
    public static void mapDiagnosis(SusOutpatient parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    BartsCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID,
                                    ResourceId patientResourceId,
                                    ResourceId encounterResourceId,
                                    TailsRecord tr) throws Exception {

        HashMap<String, Integer> codeDuplicateCountList = new HashMap<String, Integer>();
        Integer currCodeDuplicateCount = 0;

        String resourceMapSourceRowId ="CDSIdValue="+parser.getCDSUniqueID();
        List<UUID> mappingsToAdd = new ArrayList<UUID>();
        LOG.debug("Mapping Diagnosis from file entry (" + entryCount + ")");

        SusResourceMapDalI database = DalProvider.factoryBartsSusResourceMapDal();
        List<UUID> currentMappings = database.getSusResourceMappings(fhirResourceFiler.getServiceId(), resourceMapSourceRowId, Enumerations.ResourceType.CONDITION);
        LOG.debug("Number of SUS multi-mappings found:" + (currentMappings == null ? "0" : currentMappings.size()));
        if (currentMappings != null) {
            Iterator it = currentMappings.iterator();
            while (it.hasNext()) {
                LOG.debug("Resource id (multi-mappings):" + ((UUID) it.next()).toString());
            }
        }

        // Turn key into Resource id
        currCodeDuplicateCount = 1;
        codeDuplicateCountList.put(parser.getICDPrimaryDiagnosis(), currCodeDuplicateCount);
        ResourceId diagnosisResourceId = getDiagnosisResourceIdFromCDSData(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCDSUniqueID(), parser.getICDPrimaryDiagnosis(), currCodeDuplicateCount);

        //Identifiers
        Identifier[] identifiers = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID())};

        CodeableConcept diagnosisCode = new CodeableConcept();
        //diagnosisCode = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDPrimaryDiagnosis(), FhirCodeUri.CODE_SYSTEM_CERNER_ICD_10, FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED, "",false);
        diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_ICD10, TerminologyService.lookupIcd10CodeDescription(parser.getICDPrimaryDiagnosis()), parser.getICDPrimaryDiagnosis());

        Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "cds coding")};

        Condition fhirCondition = new Condition();
        createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getAppointmentDateTime(), new DateTimeType(parser.getAppointmentDateTime()), diagnosisCode, null, identifiers, Condition.ConditionVerificationStatus.CONFIRMED, null, ex);

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Delete primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        } else {
            LOG.debug("Save primary Condition resource(PatId=" + parser.getLocalPatientId() + ")" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            if (currentMappings.contains(diagnosisResourceId.getResourceId())) {
                // Mapping already exists - leave as is (i.e. remove for current list to avoid deletion)
                currentMappings.remove(diagnosisResourceId.getResourceId());
            } else {
                // New add
                mappingsToAdd.add(diagnosisResourceId.getResourceId());
            }

        }

        // secondary piagnoses ?
        LOG.debug("Secondary diagnosis list=" + parser.getICDSecondaryDiagnosisList());
        LOG.debug("Secondary diagnosis count=" + parser.getICDSecondaryDiagnosisCount());
        for (int i = 0; i < parser.getICDSecondaryDiagnosisCount(); i++) {
            // Turn key into Resource id
            if (codeDuplicateCountList.containsKey(parser.getICDSecondaryDiagnosis(i))) {
                currCodeDuplicateCount = codeDuplicateCountList.get(parser.getICDSecondaryDiagnosis(i));
                currCodeDuplicateCount++;
                codeDuplicateCountList.replace(parser.getICDSecondaryDiagnosis(i), currCodeDuplicateCount);
            } else {
                currCodeDuplicateCount = 1;
                codeDuplicateCountList.put(parser.getICDSecondaryDiagnosis(i), currCodeDuplicateCount);
            }
            diagnosisResourceId = getDiagnosisResourceIdFromCDSData(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCDSUniqueID(), parser.getICDSecondaryDiagnosis(i), currCodeDuplicateCount);

            //diagnosisCode = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDSecondaryDiagnosis(i), FhirCodeUri.CODE_SYSTEM_CERNER_ICD_10, FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED, "",false);
            diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_ICD10, TerminologyService.lookupIcd10CodeDescription(parser.getICDSecondaryDiagnosis(i)), parser.getICDSecondaryDiagnosis(i));

            fhirCondition = new Condition();
            createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getAppointmentDateTime(), new DateTimeType(parser.getAppointmentDateTime()), diagnosisCode, null, identifiers, Condition.ConditionVerificationStatus.CONFIRMED, null, ex);

            // save resource
            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            } else {
                LOG.debug("Save primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
                if (currentMappings.contains(diagnosisResourceId.getResourceId())) {
                    // Mapping already exists - leave as is (i.e. remove for current list to avoid deletion)
                    currentMappings.remove(diagnosisResourceId.getResourceId());
                } else {
                    // New add
                    mappingsToAdd.add(diagnosisResourceId.getResourceId());
                }

            }
        }

        if (currentMappings.size() > 0) {
            // Any mappings left are no longer referenced
            //delete all Condition resources in the list
            Iterator it = currentMappings.iterator();
            while (it.hasNext()) {
                UUID uuid = (UUID) it.next();
                fhirCondition = new Condition();
                fhirCondition.setId(uuid.toString());
                fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            }
            //delete all multi-mappings
            LOG.debug("Remove all remaining sus_resource_map entries");
            database.deleteSusResourceMappings(fhirResourceFiler.getServiceId(), resourceMapSourceRowId, Enumerations.ResourceType.CONDITION, currentMappings);
        }
        if (mappingsToAdd.size() > 0) {
            database.saveSusResourceMappings(fhirResourceFiler.getServiceId(), resourceMapSourceRowId, Enumerations.ResourceType.CONDITION, mappingsToAdd);
        }

    }

    /*
    Data line is of type Inpatient
    */
    public static void mapProcedure(SusOutpatient parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    BartsCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID,
                                    ResourceId patientResourceId,
                                    ResourceId encounterResourceId,
                                    TailsRecord tr) throws Exception {

        HashMap<String, Integer> codeDuplicateCountList = new HashMap<String, Integer>();
        Integer currCodeDuplicateCount = 0;

        List<UUID> mappingsToAdd = new ArrayList<UUID>();
        LOG.debug("Mapping Procedure from file entry (" + entryCount + ")");
        LOG.debug("Testing-Deployment");

        SusResourceMapDalI database = DalProvider.factoryBartsSusResourceMapDal();
        List<UUID> currentMappings = database.getSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.PROCEDURE);
        LOG.debug("Number of SUS multi-mappings found:" + (currentMappings == null ? "0" : currentMappings.size()));
        if (currentMappings != null) {
            Iterator it = currentMappings.iterator();
            while (it.hasNext()) {
                LOG.debug("Resource id (multi-mappings):" + ((UUID) it.next()).toString());
            }
        }

        // Turn key into Resource id
        currCodeDuplicateCount = 1;
        LOG.debug("currCodeDuplicateCount:" + currCodeDuplicateCount);
        codeDuplicateCountList.put(parser.getOPCSPrimaryProcedureCode(), currCodeDuplicateCount);
        LOG.debug("parser.getOPCSPrimaryProcedureCode():" + parser.getOPCSPrimaryProcedureCode());
        LOG.debug("parser.getOPCSPrimaryProcedureDateAsString():" + parser.getOPCSPrimaryProcedureDateAsString());
        LOG.debug("currCodeDuplicateCount:" + currCodeDuplicateCount);
        LOG.debug("tr.getEncounterId():" + tr.getEncounterId());
        ResourceId resourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId(), parser.getOPCSPrimaryProcedureDateAsString(), parser.getOPCSPrimaryProcedureCode(), currCodeDuplicateCount);

        String code = parser.getOPCSPrimaryProcedureCode();
        Date date = parser.getOPCSPrimaryProcedureDate();
        String uniqueId = parser.getCDSUniqueID();
        ProcedureBuilder procedureBuilder = ProcedureTransformer.createProcedureResource(resourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, code, null, FhirCodeUri.CODE_SYSTEM_OPCS4, date, null, uniqueId, "cds coding");

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Save primary Procedure(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
        } else {
            LOG.debug("Save primary Procedure(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
            if (currentMappings.contains(resourceId.getResourceId())) {
                // Mapping already exists - leave as is (i.e. remove for current list to avoid deletion)
                currentMappings.remove(resourceId.getResourceId());
            } else {
                // New add
                mappingsToAdd.add(resourceId.getResourceId());
            }
        }

        // secondary procedures
        LOG.debug("Secondary procedure list=" + parser.getOPCSecondaryProcedureList());
        LOG.debug("Secondary procedure count=" + parser.getOPCSecondaryProcedureCodeCount());
        for (int i = 0; i < parser.getOPCSecondaryProcedureCodeCount(); i++) {
            if (codeDuplicateCountList.containsKey(parser.getOPCSecondaryProcedureCode(i))) {
                currCodeDuplicateCount = codeDuplicateCountList.get(parser.getOPCSecondaryProcedureCode(i));
                currCodeDuplicateCount++;
                codeDuplicateCountList.replace(parser.getOPCSecondaryProcedureCode(i), currCodeDuplicateCount);
            } else {
                currCodeDuplicateCount = 1;
                codeDuplicateCountList.put(parser.getOPCSecondaryProcedureCode(i), currCodeDuplicateCount);
            }

            // Turn key into Resource id
            resourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId(), parser.getOPCSecondaryProcedureDateAsString(i), parser.getOPCSecondaryProcedureCode(i), currCodeDuplicateCount);

            code = parser.getOPCSecondaryProcedureCode(i);
            date = parser.getOPCSecondaryProcedureDate(i);
            procedureBuilder = ProcedureTransformer.createProcedureResource(resourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, code, null, FhirCodeUri.CODE_SYSTEM_OPCS4, date, null, uniqueId, "cds coding");

            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete secondary Procedure (PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
            } else {
                LOG.debug("Save secondary Procedure (PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
                if (currentMappings.contains(resourceId.getResourceId())) {
                    // Mapping already exists - leave as is (i.e. remove for current list to avoid deletion)
                    currentMappings.remove(resourceId.getResourceId());
                } else {
                    // New add
                    mappingsToAdd.add(resourceId.getResourceId());
                }
            }
        }

        if (currentMappings.size() > 0) {
            // Any mappings left are no longer referenced
            //delete all Condition resources in the list
            Iterator it = currentMappings.iterator();
            while (it.hasNext()) {

                procedureBuilder = new ProcedureBuilder();
                procedureBuilder.setId(it.next().toString());
                procedureBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
            }
            //delete all multi-mappings
            database.deleteSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.PROCEDURE, currentMappings);
        }
        if (mappingsToAdd.size() > 0) {
            database.saveSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.PROCEDURE, mappingsToAdd);
        }

    }


}
