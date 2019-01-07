package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusInpatient;
import org.endeavourhealth.transform.barts.schema.SusInpatientTail;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SusInpatientTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusInpatientTransformer.class);


    public static void transformProcedures(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 Map<String, List<String>> fileMap) throws Exception {

        for (ParserI parser: parsers) {

            //parse corresponding tails file first
            Map<String, SusTailCacheEntry> tailsCache = processTails(parser, fileMap);

            while (parser.nextRecord()) {
                try {
                    processRecordProcedures(csvHelper, (SusInpatient)parser, tailsCache);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static Map<String, SusTailCacheEntry> processTails(ParserI parser, Map<String, List<String>> fileMap) throws Exception {
        String tailFilePath = BartsCsvToFhirTransformer.findTailFile(fileMap, "SusInpatientTail", parser.getFilePath());
        SusInpatientTail tailParser = new SusInpatientTail(parser.getServiceId(), parser.getSystemId(), parser.getExchangeId(), parser.getVersion(), tailFilePath);
        Map<String, SusTailCacheEntry> tailsCache = new HashMap<>();
        SusInpatientTailPreTransformer.transform(tailParser, tailsCache);
        return tailsCache;
    }

    private static void processRecordProcedures(BartsCsvHelper csvHelper, SusInpatient parser, Map<String, SusTailCacheEntry> tailsCache) throws Exception {
        
        //the old version of this parser only transformed specific record types, but I can't find
        //the rationale for that. But it looks like we only have record of the matching
        //types so just validate that this is true.
        validateRecordType(parser);

        //validate the procedure scheme is what we expect (i.e. OPCS-4)
        CsvCell procedureSchemCell = parser.getProcedureSchemeInUse();
        String procedureScheme = procedureSchemCell.getString();
        if (!procedureScheme.equals("02")) {
            throw new TransformException("Unexpected procedure scheme " + procedureScheme);
        }

        CsvCell cdsUniqueId = parser.getCdsUniqueId();
        String cdsUniqueIdStr = cdsUniqueId.getString();
        SusTailCacheEntry tailRecord = tailsCache.get(cdsUniqueIdStr);
        if (tailRecord == null) {
            throw new TransformException("Failed to find tail record for CSV unique ID " + cdsUniqueId);
        }

        CsvCell personIdCell = tailRecord.getPersonId();
        if (!csvHelper.processRecordFilteringOnPatientId(personIdCell.getString())) {
            return;
        }

        //TODO - find out previous procedures created for this record and delete any we don't find again

        CsvCell primaryCodeCell = parser.getPrimaryProcedureOPCS();
        if (!primaryCodeCell.isEmpty()) {
            CsvCell dateCell = parser.getPrimaryProcedureDate();
            CsvCell isPrimaryCell = primaryCodeCell; //to audit why we set the "is primary" property, use the fact that it's the primary cell itself

            //the SUS specification has fields for the main clinician and anaethetist but these seem to be always
            //empty for Barts data so we don't use them. The below code ensures this assertion remains valid.
            CsvCell primaryPerformer = parser.getPrimaryMainOperatingHCPRegistrationEntryIdentifier();
            CsvCell anaesthetist = parser.getPrimaryResponsibleAnaesthetistRegistrationEntryIdentifier();
            if (!primaryPerformer.isEmpty()
                    || !anaesthetist.isEmpty()) {
                throw new TransformException("Primary performer (" + primaryPerformer + ") or anaesthetist (" + anaesthetist + ") is non-empty");
            }

            processProcedure(csvHelper, parser, tailRecord, primaryCodeCell, dateCell, isPrimaryCell);
        }

        CsvCell secondaryCodeCell = parser.getSecondaryProcedureOPCS();
        if (!secondaryCodeCell.isEmpty()) {
            CsvCell dateCell = parser.getSecondaryProcedureDate();

            processProcedure(csvHelper, parser, tailRecord, secondaryCodeCell, dateCell, null);
        }

        CsvCell otherCodesCell = parser.getAdditionalecondaryProceduresOPCS();
        if (!otherCodesCell.isEmpty()) {

            // Each code-set is 40 characters and consists of 6 fields (4 for code + 8 for date + 4 further sub-fields) - only code and date are used
            String otherCodes = otherCodesCell.getString();
            int startPos = 0;
            while (startPos + 12 <= otherCodes.length()) {
                String code = otherCodes.substring(startPos, startPos + 4);
                String dateStr = otherCodes.substring(startPos + 4, startPos + 12);

                //create dummy cells for auditing purposes, that link back to the same cell
                CsvCell codeCell = new CsvCell(otherCodesCell.getPublishedFileId(), otherCodesCell.getRecordNumber(), otherCodesCell.getColIndex(), code, parser);
                CsvCell dateCell = new CsvCell(otherCodesCell.getPublishedFileId(), otherCodesCell.getRecordNumber(), otherCodesCell.getColIndex(), dateStr, parser);

                processProcedure(csvHelper, parser, tailRecord, codeCell, dateCell, null);

                startPos = startPos + 40;
            }
        }
    }

    private static void processProcedure(BartsCsvHelper csvHelper, SusInpatient parser, SusTailCacheEntry tailRecord, CsvCell codeCell, CsvCell performedDateCell, CsvCell isPrimaryCell) throws Exception {

        //TODO - work out unique ID

        /*ProcedureBuilder procedureBuilder = csvHelper.getProcedureCache().borrowProcedureBuilder(uniqueId, );

        CsvCell personIdCell = tailRecord.getPersonId();
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
        if (procedureBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
        }
        procedureBuilder.setPatient(patientReference);

        CsvCell encounterIdCell = tailRecord.getEncounterId();
        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
        if (procedureBuilder.isIdMapped()) {
            encounterReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(encounterReference, csvHelper);
        }
        procedureBuilder.setEncounter(encounterReference);

        //only set the practitioner if not already set, since we only really know the responsible consultant
        //in this file, and not actually the person who performed the procedure. The SUS spec does have fields
        //for performer and anaesthetist, but these are always empty for Barts (and we validate that above)
        if (!procedureBuilder.hasPerformer()) {
            CsvCell responsibleHcpPersonnelId = tailRecord.getResponsibleHcpPersonnelId();
            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, responsibleHcpPersonnelId.getString());
            if (procedureBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            procedureBuilder.addPerformer(practitionerReference, responsibleHcpPersonnelId);
        }

        //implicitly completed if it's in this file
        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

        //if we've got a cell telling us it's the primary procedure, set that to true
        if (isPrimaryCell != null) {
            procedureBuilder.setIsPrimary(true, isPrimaryCell);
        }

        Date performedDate = performedDateCell.getDateTime();
        procedureBuilder.setPerformed(new DateTimeType(performedDate), performedDateCell);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4); //always OPCS-4 in this file

        String code = codeCell.getString();
        code = TerminologyService.standardiseOpcs4Code(code); //ensure the dot is added to the code
        codeableConceptBuilder.setCodingCode(code, codeCell);

        String term = TerminologyService.lookupOpcs4ProcedureName(code);
        codeableConceptBuilder.setCodingDisplay(term);
        codeableConceptBuilder.setText(term);

        csvHelper.getProcedureCache().returnProcedureBuilder(uniqueId, procedureBuilder);*/
    }

    private static void validateRecordType(SusInpatient parser) throws Exception {
        // CDS V6-2 Type 010 - Accident and Emergency CDS
        // CDS V6-2 Type 020 - Outpatient CDS
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


    ///////////BELOW IS THE SUS INPATIENT TRANSFORM FROM WHEN IT WAS ORIGINALLY WRITTEN FOR 2.1//////////
    
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
            String tailFilePath = BartsCsvToFhirTransformer.findTailFile(allFiles, "tailip_DIS." + fName.split("_")[2] + "_susrnj.dat");
            TailsPreTransformer.transform(version, new Tails(parser.getServiceId(), parser.getSystemId(), parser.getExchangeId(), version, tailFilePath));

            entryCount = 0;
            while (parser.nextRecord()) {
                try {
                    entryCount++;

                    SusInpatient susInpatient = (SusInpatient)parser;

                    // CDS V6-2 Type 010 - Accident and Emergency CDS
                    // CDS V6-2 Type 020 - Outpatient CDS
                    // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
                    // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
                    // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
                    // CDS V6-2 Type 160 - Admitted Patient Care - Other Delivery Event CDS
                    // CDS V6-2 Type 180 - Admitted Patient Care - Unfinished Birth Episode CDS
                    // CDS V6-2 Type 190 - Admitted Patient Care - Unfinished General Episode CDS
                    // CDS V6-2 Type 200 - Admitted Patient Care - Unfinished Delivery Episode CDS
                    if (susInpatient.getCDSRecordType() == 10 ||
                            susInpatient.getCDSRecordType() == 20 ||
                            susInpatient.getCDSRecordType() == 120 ||
                            susInpatient.getCDSRecordType() == 130 ||
                            susInpatient.getCDSRecordType() == 140 ||
                            susInpatient.getCDSRecordType() == 160 ||
                            susInpatient.getCDSRecordType() == 180 ||
                            susInpatient.getCDSRecordType() == 190 ||
                            susInpatient.getCDSRecordType() == 200) {

                        mapFileEntry(susInpatient, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void mapFileEntry(SusInpatient parser,
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
                //LOG.debug("Ethnic group:" + parser.getEthnicCategory() + "==>" + getSusEthnicCategoryDisplay(parser.getEthnicCategory()));
            }

            patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), parser.getNHSNo(), name, fhirAddress, BartsSusHelper.convertSusGenderToFHIR(parser.getGender()), parser.getDOB(), organisationResourceId, null, patientIdentifier, gpResourceId, gpPracticeResourceId, ethnicGroup);

            if (tr != null && tr.getEpisodeId() != null && tr.getEpisodeId().length() > 0 && tr.getEncounterId() != null && tr.getEncounterId().length() > 0) {
                // EpisodeOfCare
                episodeOfCareResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEpisodeId());
                if (episodeOfCareResourceId == null) {
                    episodeOfCareResourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEpisodeId());
                }
                EpisodeOfCare.EpisodeOfCareStatus fhirEpisodeOfCareStatus;
                if (parser.getCDSRecordType() == 120 || parser.getCDSRecordType() == 130 || parser.getCDSRecordType() == 140) {
                    fhirEpisodeOfCareStatus = EpisodeOfCare.EpisodeOfCareStatus.FINISHED;
                } else {
                    fhirEpisodeOfCareStatus = EpisodeOfCare.EpisodeOfCareStatus.ACTIVE;
                }
                //Identifiers
                Identifier episodeIdentifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_EPISODE_ID).setValue(tr.getEpisodeId())};

                createEpisodeOfCare(parser.getCurrentState(), fhirResourceFiler, episodeOfCareResourceId, patientResourceId, organisationResourceId, fhirEpisodeOfCareStatus, parser.getAdmissionDateTime(), parser.getDischargeDateTime(), episodeIdentifiers);
                // Encounter
                encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId());
                if (encounterResourceId == null) {
                    encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId());
                }
                Encounter.EncounterState encounterStatus;
                if (parser.getCDSRecordType() == 120 || parser.getCDSRecordType() == 130 || parser.getCDSRecordType() == 140) {
                    encounterStatus = Encounter.EncounterState.FINISHED;
                } else {
                    encounterStatus = Encounter.EncounterState.INPROGRESS;
                }
                //Identifiers
                Identifier encounterIdentifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_EPISODE_ID).setValue(tr.getEpisodeId()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_ID).setValue(tr.getEncounterId()),
                        new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(tr.getFINNbr())};

                createEncounter(parser.getCurrentState(), fhirResourceFiler, patientResourceId, episodeOfCareResourceId, encounterResourceId, encounterStatus, parser.getAdmissionDateTime(), parser.getDischargeDateTime(), encounterIdentifiers, Encounter.EncounterClass.INPATIENT);
            }
        }

        // Map diagnosis codes ?
        if (parser.getICDPrimaryDiagnosis().length() > 0) {
            // Diagnosis
            mapDiagnosis(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId);
        }

        // Map procedure codes ?
        if (parser.getOPCSPrimaryProcedureCode().length() > 0 && tr != null && tr.getEncounterId() != null && tr.getEncounterId().length() > 0) {
            // Procedure
            mapProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId, tr);
        }

    }


    public static void mapDiagnosis(SusInpatient parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    BartsCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID,
                                    ResourceId patientResourceId,
                                    ResourceId encounterResourceId) throws Exception {

        HashMap<String, Integer> codeDuplicateCountList = new HashMap<String, Integer>();
        Integer currCodeDuplicateCount = 0;

        List<UUID> mappingsToAdd = new ArrayList<UUID>();
        LOG.debug("Mapping Diagnosis from file entry (" + entryCount + ")");

        SusResourceMapDalI database = DalProvider.factoryBartsSusResourceMapDal();
        List<UUID> currentMappings = database.getSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.CONDITION);
        LOG.debug("Number of SUS multi-mappings found:" + (currentMappings == null ? "0" : currentMappings.size()));

        currCodeDuplicateCount = 1;
        codeDuplicateCountList.put(parser.getICDPrimaryDiagnosis(), currCodeDuplicateCount);
        ResourceId resourceId = getDiagnosisResourceIdFromCDSData(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCDSUniqueID(), parser.getICDPrimaryDiagnosis(), currCodeDuplicateCount);

        Condition fhirCondition = new Condition();

        //Identifiers
        Identifier identifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID())};

        //CodeableConcept diagnosisCode = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDPrimaryDiagnosis(), FhirCodeUri.CODE_SYSTEM_CERNER_ICD_10, FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED, "", false);
        CodeableConcept diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_ICD10, TerminologyService.lookupIcd10CodeDescription(parser.getICDPrimaryDiagnosis()), parser.getICDPrimaryDiagnosis());

        Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "cds coding")};

        createDiagnosis(fhirCondition, resourceId,encounterResourceId, patientResourceId, parser.getAdmissionDateTime(), new DateTimeType(parser.getAdmissionDate()), diagnosisCode, null, identifiers, Condition.ConditionVerificationStatus.CONFIRMED, null, ex);

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Delete primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
        } else {
            LOG.debug("Save primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
            if (currentMappings.contains(resourceId.getResourceId())) {
                // Mapping already exists - leave as is (i.e. remove for current list to avoid deletion)
                currentMappings.remove(resourceId.getResourceId());
            } else {
                // New add
                mappingsToAdd.add(resourceId.getResourceId());
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

            resourceId = getDiagnosisResourceIdFromCDSData(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCDSUniqueID(), parser.getICDSecondaryDiagnosis(i), currCodeDuplicateCount);

            //diagnosisCode = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDSecondaryDiagnosis(i), FhirCodeUri.CODE_SYSTEM_CERNER_ICD_10, FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED, "", false);
            diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_ICD10, TerminologyService.lookupIcd10CodeDescription(parser.getICDSecondaryDiagnosis(i)), parser.getICDSecondaryDiagnosis(i));

            fhirCondition = new Condition();
            createDiagnosis(fhirCondition, resourceId,encounterResourceId, patientResourceId, parser.getAdmissionDateTime(), new DateTimeType(parser.getAdmissionDate()), diagnosisCode, null, identifiers, Condition.ConditionVerificationStatus.CONFIRMED, null, ex);

            // save resource
            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
            } else {
                LOG.debug("Save primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
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

    
    public static void mapProcedure(SusInpatient parser,
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

        currCodeDuplicateCount = 1;
        codeDuplicateCountList.put(parser.getOPCSPrimaryProcedureCode(), currCodeDuplicateCount);
        ResourceId resourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId(), parser.getOPCSPrimaryProcedureDateAsString(), parser.getOPCSPrimaryProcedureCode(), currCodeDuplicateCount);

        // status
        // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
        // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
        // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
        Procedure.ProcedureStatus procedureStatus;
        if (parser.getCDSRecordType() == 120 || parser.getCDSRecordType() == 130 || parser.getCDSRecordType() == 140) {
            procedureStatus =Procedure.ProcedureStatus.COMPLETED;
        } else {
            procedureStatus = Procedure.ProcedureStatus.INPROGRESS;
        }

        String code = parser.getOPCSPrimaryProcedureCode();
        Date date = parser.getOPCSPrimaryProcedureDate();
        String uniqueId = parser.getCDSUniqueID();
        ProcedureBuilder procedureBuilder = ProcedureTransformer.createProcedureResource(resourceId, encounterResourceId, patientResourceId, procedureStatus, code, null, FhirCodeUri.CODE_SYSTEM_OPCS4, date, null, uniqueId, "cds coding");

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

            // New resource id
            resourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId(), parser.getOPCSecondaryProcedureDateAsString(i), parser.getOPCSecondaryProcedureCode(i), currCodeDuplicateCount);

            // Code
            code = parser.getOPCSecondaryProcedureCode(i);
            date = parser.getOPCSecondaryProcedureDate(i);
            procedureBuilder = ProcedureTransformer.createProcedureResource(resourceId, encounterResourceId, patientResourceId, procedureStatus, code, null, FhirCodeUri.CODE_SYSTEM_OPCS4, date, null, uniqueId, "cds coding");

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
