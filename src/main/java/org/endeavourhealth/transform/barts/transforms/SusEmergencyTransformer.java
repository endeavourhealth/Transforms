package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsBartsSusResourceMapDal;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.SusEmergency;
import org.endeavourhealth.transform.barts.schema.TailsRecord;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class SusEmergencyTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyTransformer.class);
    private static int entryCount = 0;

    public static void transform(String version,
                                 SusEmergency parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        entryCount = 0;
        while (parser.nextRecord()) {
            try {
                entryCount++;
                // CDS V6-2 Type 010 - Accident and Emergency CDS
                // CDS V6-2 Type 020 - Outpatient CDS
                // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
                // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
                // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
                // CDS V6-2 Type 160 - Admitted Patient Care - Other Delivery Event CDS
                // CDS V6-2 Type 180 - Admitted Patient Care - Unfinished Birth Episode CDS
                // CDS V6-2 Type 190 - Admitted Patient Care - Unfinished General Episode CDS
                // CDS V6-2 Type 200 - Admitted Patient Care - Unfinished Delivery Episode CDS
                if (parser.getCDSRecordType() == 10 ||
                        parser.getCDSRecordType() == 20 ||
                        parser.getCDSRecordType() == 120 ||
                        parser.getCDSRecordType() == 130 ||
                        parser.getCDSRecordType() == 140 ||
                        parser.getCDSRecordType() == 160 ||
                        parser.getCDSRecordType() == 180 ||
                        parser.getCDSRecordType() == 190 ||
                        parser.getCDSRecordType() == 200) {
                    mapFileEntry(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static void mapFileEntry(SusEmergency parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
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
            Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
            organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);
            // Patient
            HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(HumanName.NameUse.OFFICIAL, parser.getPatientTitle(), parser.getPatientForename(), "", parser.getPatientSurname());
            Address fhirAddress = null;
            if (parser.getAddressType().compareTo("02") == 0) {
                fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME, parser.getAddress1(), parser.getAddress2(), parser.getAddress3(), parser.getAddress4(), parser.getAddress5(), parser.getPostCode());
            }

            Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()))};

            CodeableConcept ethnicGroup = null;
            if (parser.getEthnicCategory() != null && parser.getEthnicCategory().length() > 0) {
                ethnicGroup = new CodeableConcept();
                ethnicGroup.addCoding().setCode(parser.getEthnicCategory()).setSystem(FhirExtensionUri.PATIENT_ETHNICITY).setDisplay(getSusEthnicCategoryDisplay(parser.getEthnicCategory()));
            }

            patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), parser.getNHSNo(), name, fhirAddress, convertSusGenderToFHIR(parser.getGender()), parser.getDOB(), organisationResourceId, null, patientIdentifier, gpResourceId, gpPracticeResourceId, ethnicGroup);

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
                Identifier episodeIdentifiers[] = {new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID()),
                        new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_EPISODE_ID).setValue(tr.getEpisodeId())};

                createEpisodeOfCare(parser.getCurrentState(), fhirResourceFiler, episodeOfCareResourceId, patientResourceId, organisationResourceId, EpisodeOfCare.EpisodeOfCareStatus.FINISHED, parser.getArrivalDateTime(), parser.getDepartureDateTime(), episodeIdentifiers);
                // Encounter
                encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId());
                if (encounterResourceId == null) {
                    encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId());
                }
                //Identifiers
                Identifier encounterIdentifiers[] = {new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID()),
                        new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_EPISODE_ID).setValue(tr.getEpisodeId()),
                        new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_ENCOUNTER_ID).setValue(tr.getEncounterId()),
                        new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(tr.getFINNbr())};

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
        if (parser.getOPCSPrimaryProcedureCode().length() > 0) {
            // Procedure
            mapProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId, tr);
        //} else {
           // LOG.debug("No primary procedure present");
        }

    }


    /*
    Data line is of type Inpatient
    */
    public static void mapDiagnosis(SusEmergency parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID,
                                    ResourceId patientResourceId,
                                    ResourceId encounterResourceId,
                                    TailsRecord tr) throws Exception {

        List<UUID> mappingsToAdd = new ArrayList<UUID>();
        LOG.debug("Mapping Diagnosis from file entry (" + entryCount + ")");

        RdbmsBartsSusResourceMapDal database = DalProvider.factoryBartsSusResourceMapDal();
        List<UUID> currentMappings = database.getSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.CONDITION);
        LOG.debug("Number of SUS multi-mappings found:" + (currentMappings == null ? "0" : currentMappings.size()));

        ResourceId diagnosisResourceId = getDiagnosisResourceIdFromCDSData(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCDSUniqueID(), parser.getICDPrimaryDiagnosis());

        //Identifiers
        Identifier identifiers[] = {new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID())};

        CodeableConcept diagnosisCode = new CodeableConcept();
        //cc = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDPrimaryDiagnosis(), BartsCsvToFhirTransformer.CODE_SYSTEM_ICD_10, BartsCsvToFhirTransformer.CODE_SYSTEM_SNOMED, "",false);
        diagnosisCode.addCoding().setCode(parser.getICDPrimaryDiagnosis()).setSystem(FhirUri.CODE_SYSTEM_ICD10).setDisplay("");

        Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "cds coding")};

        Condition fhirCondition = new Condition();
        createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getArrivalDateTime(), new DateTimeType(parser.getArrivalDateTime()), diagnosisCode, null, identifiers, Condition.ConditionVerificationStatus.CONFIRMED, null, ex);

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Delete primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            // Leave multi-mapping entry - any entry left at the end of it will be deleted
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

        // secondary piagnoses ?
        LOG.debug("Secondary diagnosis list=" + parser.getICDSecondaryDiagnosisList());
        LOG.debug("Secondary diagnosis count=" + parser.getICDSecondaryDiagnosisCount());
        for (int i = 0; i < parser.getICDSecondaryDiagnosisCount(); i++) {
            diagnosisResourceId = getDiagnosisResourceIdFromCDSData(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCDSUniqueID(), parser.getICDSecondaryDiagnosis(i));

            diagnosisCode = new CodeableConcept();
            //cc = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDSecondaryDiagnosis(i), BartsCsvToFhirTransformer.CODE_SYSTEM_ICD_10, BartsCsvToFhirTransformer.CODE_SYSTEM_SNOMED, "",false);
            diagnosisCode.addCoding().setCode(parser.getICDSecondaryDiagnosis(i)).setSystem(FhirUri.CODE_SYSTEM_ICD10).setDisplay("");

            fhirCondition = new Condition();
            createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getArrivalDateTime(), new DateTimeType(parser.getArrivalDateTime()), diagnosisCode, null, identifiers, Condition.ConditionVerificationStatus.CONFIRMED, null, ex);

            // save resource
            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete primary Condition resource(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
                // Leave mapping entry - any entry left at the end of it will be deleted
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
                fhirCondition = new Condition();
                fhirCondition.setId(it.next().toString());
                fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            }
            //delete all multi-mappings
            database.deleteSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.CONDITION, currentMappings);
        }
        if (mappingsToAdd.size() > 0) {
            database.saveSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.CONDITION, mappingsToAdd);
        }

    }

    /*
Data line is of type Inpatient
*/
    public static void mapProcedure(SusEmergency parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID,
                                    ResourceId patientResourceId,
                                    ResourceId encounterResourceId,
                                    TailsRecord tr) throws Exception {

        //CodeableConcept cc = null;
        //Date d = null;
        //String uniqueId;
        List<UUID> mappingsToAdd = new ArrayList<UUID>();
        LOG.debug("Mapping Procedure from file entry (" + entryCount + ")");

        RdbmsBartsSusResourceMapDal database = DalProvider.factoryBartsSusResourceMapDal();
        List<UUID> currentMappings = database.getSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.PROCEDURE);
        LOG.debug("Number of SUS multi-mappings found:" + (currentMappings == null ? "0" : currentMappings.size()));

        // Turn key into Resource id
        ResourceId resourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId(), parser.getOPCSPrimaryProcedureDateAsString(), parser.getOPCSPrimaryProcedureCode());

        //Identifiers
        Identifier identifiers[] = {new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_CDS_UNIQUE_ID).setValue(parser.getCDSUniqueID())};

        // Code
        CodeableConcept procedureCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_OPCS4, "", parser.getOPCSPrimaryProcedureCode());

        Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "cds coding")};

        Procedure fhirProcedure = new Procedure ();
        ProcedureTransformer.createProcedureResource(fhirProcedure, resourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, procedureCode, parser.getOPCSPrimaryProcedureDate(), null, identifiers, null, ex);

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Save primary Procedure(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
        } else {
            LOG.debug("Save primary Procedure(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
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
            resourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, tr.getEncounterId(), parser.getOPCSecondaryProcedureDateAsString(i), parser.getOPCSecondaryProcedureCode(i));

            // Code
            procedureCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_OPCS4, "", parser.getOPCSecondaryProcedureCode(i));

            fhirProcedure = new Procedure ();
            ProcedureTransformer.createProcedureResource(fhirProcedure, resourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, procedureCode, parser.getOPCSecondaryProcedureDate(i), null, identifiers, null, ex);

            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete secondary Procedure (PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
            } else {
                LOG.debug("Save secondary Procedure (PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
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
                fhirProcedure = new Procedure();
                fhirProcedure.setId(it.next().toString());
                fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirProcedure);
            }
            //delete all multi-mappings
            database.deleteSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.PROCEDURE, currentMappings);
        }
        if (mappingsToAdd.size() > 0) {
            database.saveSusResourceMappings(fhirResourceFiler.getServiceId(), "CDSIdValue="+parser.getCDSUniqueID(), Enumerations.ResourceType.PROCEDURE, mappingsToAdd);
        }

    }

}
