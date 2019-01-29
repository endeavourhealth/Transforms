package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusEmergency;
import org.endeavourhealth.transform.barts.schema.SusEmergencyTail;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SusEmergencyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyTransformer.class);

    public static void transformProcedures(List<ParserI> parsers,
                                           FhirResourceFiler fhirResourceFiler,
                                           BartsCsvHelper csvHelper,
                                           Map<String, List<ParserI>> parserMap) throws Exception {

        for (ParserI parser: parsers) {

            //parse corresponding tails file first
            Map<String, SusTailCacheEntry> tailsCache = processTailsFile(parser, parserMap);

            while (parser.nextRecord()) {
                try {
                    processRecordProcedures((SusEmergency)parser);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static Map<String, SusTailCacheEntry> processTailsFile(ParserI parser, Map<String, List<ParserI>> parserMap) throws Exception {
        SusEmergencyTail tailParser = (SusEmergencyTail)BartsCsvToFhirTransformer.findTailFile(parserMap, "SusEmergencyTail", parser.getFilePath());
        Map<String, SusTailCacheEntry> tailsCache = new HashMap<>();
        SusEmergencyTailPreTransformer.transform(tailParser, tailsCache);
        return tailsCache;
    }

    private static void processRecordProcedures(SusEmergency parser) throws Exception {


        //TODO - copy from inpatient transform
    }

    private static void processProcedure(SusEmergency parser, CsvCell code, CsvCell date, boolean isPrimary) throws Exception {

//TODO
    }

    private static void validateRecordType(SusEmergency parser) throws Exception {
        // CDS V6-2 Type 010 - Accident and Emergency CDS
        // CDS V6-2 Type 020 - Emergency CDS
        // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
        // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
        // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
        // CDS V6-2 Type 160 - Admitted Patient Care - Other Delivery Event CDS
        // CDS V6-2 Type 180 - Admitted Patient Care - Unfinished Birth Episode CDS
        // CDS V6-2 Type 190 - Admitted Patient Care - Unfinished General Episode CDS
        // CDS V6-2 Type 200 - Admitted Patient Care - Unfinished Delivery Episode CDS
        CsvCell recordTypeCell = parser.getCDSRecordType();
        int recordType = recordTypeCell.getInt();
        if (recordType != 10 &&
                recordType != 20 &&
                recordType != 120 &&
                recordType != 130 &&
                recordType != 140 &&
                recordType != 160 &&
                recordType != 180 &&
                recordType != 190 &&
                recordType != 200) {

            throw new TransformException("Unexpected CDS record type " + recordType);
        }
    }

    

    ///////////BELOW IS THE SUS EMERGENCY TRANSFORM FROM WHEN IT WAS ORIGINALLY WRITTEN FOR 2.1//////////

    /*private static int entryCount = 0;

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
            String tailFilePath = BartsCsvToFhirTransformer.findTailFile(allFiles, "tailaea_DIS." + fName.split("_")[1].split("\\.")[1]);
            TailsPreTransformer.transform(version, new Tails(parser.getServiceId(), parser.getSystemId(), parser.getExchangeId(), version, tailFilePath));

            entryCount = 0;
            while (parser.nextRecord()) {
                try {
                    entryCount++;

                    SusEmergency susEmergency = (SusEmergency)parser;

                    // CDS V6-2 Type 010 - Accident and Emergency CDS
                    // CDS V6-2 Type 020 - Emergency CDS
                    // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
                    // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
                    // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
                    // CDS V6-2 Type 160 - Admitted Patient Care - Other Delivery Event CDS
                    // CDS V6-2 Type 180 - Admitted Patient Care - Unfinished Birth Episode CDS
                    // CDS V6-2 Type 190 - Admitted Patient Care - Unfinished General Episode CDS
                    // CDS V6-2 Type 200 - Admitted Patient Care - Unfinished Delivery Episode CDS
                    if (susEmergency.getCDSRecordType() == 10 ||
                            susEmergency.getCDSRecordType() == 20 ||
                            susEmergency.getCDSRecordType() == 120 ||
                            susEmergency.getCDSRecordType() == 130 ||
                            susEmergency.getCDSRecordType() == 140 ||
                            susEmergency.getCDSRecordType() == 160 ||
                            susEmergency.getCDSRecordType() == 180 ||
                            susEmergency.getCDSRecordType() == 190 ||
                            susEmergency.getCDSRecordType() == 200) {
                        mapFileEntry(susEmergency, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void mapFileEntry(SusEmergency parser,
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
                EpisodeOfCare.EpisodeOfCareStatus fhirEpisodeOfCareStatus;
                if (parser.getCDSRecordType() == 120 || parser.getCDSRecordType() == 130 || parser.getCDSRecordType() == 140) {
                    fhirEpisodeOfCareStatus = EpisodeOfCare.EpisodeOfCareStatus.FINISHED;
                } else {
                    fhirEpisodeOfCareStatus = EpisodeOfCare.EpisodeOfCareStatus.ACTIVE;
                }
                episodeOfCareResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEpisodeId());
                if (episodeOfCareResourceId == null) {
                    episodeOfCareResourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEpisodeId());
                }
                //Identifiers
                Identifier episodeIdentifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_EPISODE_ID).setValue(tr.getEpisodeId())};

                createEpisodeOfCare(parser.getCurrentState(), fhirResourceFiler, episodeOfCareResourceId, patientResourceId, organisationResourceId, EpisodeOfCare.EpisodeOfCareStatus.FINISHED, parser.getArrivalDateTime(), parser.getDepartureDateTime(), episodeIdentifiers);
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

                createEncounter(parser.getCurrentState(), fhirResourceFiler, patientResourceId, episodeOfCareResourceId, encounterResourceId, Encounter.EncounterState.FINISHED, parser.getArrivalDateTime(), parser.getDepartureDateTime(), encounterIdentifiers, Encounter.EncounterClass.EMERGENCY);
            }
        }

        // Map diagnosis codes ?
        if (parser.getICDPrimaryDiagnosis().length() > 0) {
            // Diagnosis
            mapDiagnosis(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId, tr);
        //} else {
            //LOG.debug("No primary diagnosis present");
        }

        // Map procedure codes ?
        if (parser.getOPCSPrimaryProcedureCode().length() > 0 && tr != null && tr.getEncounterId() != null && tr.getEncounterId().length() > 0) {
            // Procedure
            mapProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId, tr);
        //} else {
           // LOG.debug("No primary procedure present");
        }

    }


 
    public static void mapDiagnosis(SusEmergency parser,
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
        LOG.debug("Mapping Diagnosis from file entry (" + entryCount + ")");

        SusResourceMapDalI database = DalProvider.factoryBartsSusResourceMapDal();
        List<UUID> currentMappings = database.getSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.CONDITION);
        LOG.debug("Number of SUS multi-mappings found:" + (currentMappings == null ? "0" : currentMappings.size()));

        currCodeDuplicateCount = 1;
        codeDuplicateCountList.put(parser.getICDPrimaryDiagnosis(), currCodeDuplicateCount);
        ResourceId diagnosisResourceId = getDiagnosisResourceIdFromCDSData(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCDSUniqueID(), parser.getICDPrimaryDiagnosis(), currCodeDuplicateCount);

        //Identifiers
        Identifier identifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID())};

        CodeableConcept diagnosisCode = new CodeableConcept();
        //cc = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDPrimaryDiagnosis(), FhirCodeUri.CODE_SYSTEM_CERNER_ICD_10, FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED, "",false);
        diagnosisCode.addCoding().setCode(parser.getICDPrimaryDiagnosis()).setSystem(FhirCodeUri.CODE_SYSTEM_ICD10).setDisplay(TerminologyService.lookupIcd10CodeDescription(parser.getICDPrimaryDiagnosis()));

        Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "cds coding")};

        Condition fhirCondition = new Condition();
        createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getArrivalDateTime(), new DateTimeType(parser.getArrivalDateTime()), diagnosisCode, null, identifiers, Condition.ConditionVerificationStatus.CONFIRMED, null, ex);

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Delete primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
            // Leave multi-mapping entry - any entry left at the end of it will be deleted
        } else {
            LOG.debug("Save primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
            if (currentMappings.contains(diagnosisResourceId.getResourceId())) {
                // Mapping already exists - leave as is (i.e. remove for current list to avoid deletion)
                currentMappings.remove(diagnosisResourceId.getResourceId());
            } else {
                // New add
                mappingsToAdd.add(diagnosisResourceId.getResourceId());
            }
        }

        // secondary piagnoses ?
        for (int i = 0; i < parser.getICDSecondaryDiagnosisCount(); i++) {
            if (codeDuplicateCountList.containsKey(parser.getICDSecondaryDiagnosis(i))) {
                currCodeDuplicateCount = codeDuplicateCountList.get(parser.getICDSecondaryDiagnosis(i));
                currCodeDuplicateCount++;
                codeDuplicateCountList.replace(parser.getICDSecondaryDiagnosis(i), currCodeDuplicateCount);
            } else {
                currCodeDuplicateCount = 1;
                codeDuplicateCountList.put(parser.getICDSecondaryDiagnosis(i), currCodeDuplicateCount);
            }

            diagnosisResourceId = getDiagnosisResourceIdFromCDSData(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCDSUniqueID(), parser.getICDSecondaryDiagnosis(i), currCodeDuplicateCount);

            diagnosisCode = new CodeableConcept();
            //cc = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDSecondaryDiagnosis(i), FhirCodeUri.CODE_SYSTEM_CERNER_ICD_10, FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED, "",false);
            diagnosisCode.addCoding().setCode(parser.getICDSecondaryDiagnosis(i)).setSystem(FhirCodeUri.CODE_SYSTEM_ICD10).setDisplay(TerminologyService.lookupIcd10CodeDescription(parser.getICDSecondaryDiagnosis(i)));

            fhirCondition = new Condition();
            createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getArrivalDateTime(), new DateTimeType(parser.getArrivalDateTime()), diagnosisCode, null, identifiers, Condition.ConditionVerificationStatus.CONFIRMED, null, ex);

            // save resource
            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
                // Leave mapping entry - any entry left at the end of it will be deleted
            } else {
                LOG.debug("Save primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
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
                fhirCondition = new Condition();
                fhirCondition.setId(it.next().toString());
                fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
            }
            //delete all multi-mappings
            database.deleteSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.CONDITION, currentMappings);
        }
        if (mappingsToAdd.size() > 0) {
            database.saveSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.CONDITION, mappingsToAdd);
        }

    }

    public static void mapProcedure(SusEmergency parser,
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

        SusResourceMapDalI database = DalProvider.factoryBartsSusResourceMapDal();
        List<UUID> currentMappings = database.getSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.PROCEDURE);
        LOG.debug("Number of SUS multi-mappings found:" + (currentMappings == null ? "0" : currentMappings.size()));

        // Turn key into Resource id
        currCodeDuplicateCount = 1;
        codeDuplicateCountList.put(parser.getOPCSPrimaryProcedureCode(), currCodeDuplicateCount);
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
        for (int i = 0; i < parser.getOPCSecondaryProcedureCodeCount(); i++) {
            if (codeDuplicateCountList.containsKey(parser.getOPCSecondaryProcedureCode(i))) {
                currCodeDuplicateCount = codeDuplicateCountList.get(parser.getOPCSecondaryProcedureCode(i));
                currCodeDuplicateCount++;
                codeDuplicateCountList.replace(parser.getOPCSecondaryProcedureCode(i), currCodeDuplicateCount);
            } else {
                currCodeDuplicateCount = 1;
                codeDuplicateCountList.put(parser.getOPCSecondaryProcedureCode(i), currCodeDuplicateCount);
            }
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

    }*/

}
