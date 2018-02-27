package org.endeavourhealth.transform.common;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceMergeDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

public class BasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BasisTransformer.class);

    private static Connection hl7receiverConnection = null;
    private static PreparedStatement resourceIdSelectStatement;
    private static PreparedStatement resourceIdInsertStatement;
    private static PreparedStatement mappingSelectStatement;
    private static PreparedStatement getAllCodeSystemsSelectStatement;
    private static PreparedStatement getCodeSystemsSelectStatement;
    private static HashMap<Integer, String> codeSystemCache = new HashMap<Integer, String>();
    private static int lastLookupCodeSystemId = 0;
    private static String lastLookupCodeSystemIdentifier = "";
    private static ResourceMergeDalI mergeDAL = null;

    /*
     * Example: ResourceId resourceId = ResourceIdHelper.getResourceId("B", "Condition", uniqueId);
     */
    public static ResourceId getResourceId(String scope, String resourceType, String uniqueId) throws SQLException, ClassNotFoundException, IOException {

        //Try to find the resourceId in the cache first
        String resourceIdLookup = scope + "|" + resourceType + "|" + uniqueId;
        ResourceId resourceId  = BartsCsvHelper.getResourceIdFromCache (resourceIdLookup);
        if (resourceId != null) {
            return resourceId;
        }

        //otherwise, hit the DB
        if (hl7receiverConnection == null) {
            prepareJDBCConnection();
        }

        resourceIdSelectStatement.setString(1, scope);
        resourceIdSelectStatement.setString(2, resourceType);
        resourceIdSelectStatement.setString(3, uniqueId);

        ResultSet rs = resourceIdSelectStatement.executeQuery();
        if (rs.next()) {
            resourceId = new ResourceId();
            resourceId.setScopeId(scope);
            resourceId.setResourceType(resourceType);
            resourceId.setResourceId((UUID) rs.getObject(1));
            resourceId.setUniqueId(uniqueId);

            // Add to the cache
            BartsCsvHelper.addResourceIdToCache(resourceId);
        }
        rs.close();

        return resourceId;
    }


    public static void saveResourceId(ResourceId resourceId) throws SQLException, ClassNotFoundException, IOException {
        if (hl7receiverConnection == null) {
            prepareJDBCConnection();
        }

        resourceIdInsertStatement.setString(1, resourceId.getScopeId());
        resourceIdInsertStatement.setString(2, resourceId.getResourceType());
        resourceIdInsertStatement.setString(3, resourceId.getUniqueId());
        resourceIdInsertStatement.setObject(4, resourceId.getResourceId());

        if (resourceIdInsertStatement.executeUpdate() != 1) {
            throw new SQLException("Could not create ResourceId:"
                    + resourceId.getScopeId()
                    + ":" + resourceId.getResourceType() + ":"
                    + resourceId.getUniqueId()
                    + ":" + resourceId.getResourceId().toString());
        } else {
            // Add to the cache
            BartsCsvHelper.addResourceIdToCache(resourceId);
        }
    }

    /*
        set sourceCodeSystemId to -1 if no system is defined
     */
    public static CodeableConcept mapToCodeableConceptDONOTUSEFORNOW(String scope, String sourceContextName, String sourceCodeValue, int sourceSystemId, int targetSystemId, String displayText, boolean throwErrors) throws TransformException, SQLException, IOException, ClassNotFoundException {
        String sourceCodeSystem = null;
        String targetCodeSystem = null;
        String searchKey = "scope=" + scope + ":sourceContextName=" + sourceContextName + ":sourceCodeValue=" + sourceCodeValue + ":sourceCodeSystemId=" + sourceSystemId + ":targetSystemId=" + targetSystemId;
        LOG.trace("Looking for:" + searchKey);

        if (hl7receiverConnection == null) {
            prepareJDBCConnection();
        }

        CodeableConcept ret = null;

        mappingSelectStatement.setString(1, scope);
        mappingSelectStatement.setString(2, sourceContextName);
        mappingSelectStatement.setString(3, sourceCodeValue);
        mappingSelectStatement.setInt(4, sourceSystemId);
        mappingSelectStatement.setInt(5, targetSystemId);

        ResultSet rs = mappingSelectStatement.executeQuery();
        if (rs.next()) {
            sourceCodeSystem = getCodeSystemName(sourceSystemId);
            targetCodeSystem = getCodeSystemName(targetSystemId);

            ret = new CodeableConcept();
            ret.addCoding().setCode(rs.getString(1)).setSystem(targetCodeSystem).setDisplay(rs.getString(3));
            if (rs.getString(2).length() > 0) {
                ret.addCoding().setCode(sourceCodeValue).setSystem(rs.getString(2)).setDisplay(rs.getString(2));
            } else {
                ret.addCoding().setCode(sourceCodeValue).setSystem(sourceCodeSystem).setDisplay(displayText);
            }
            if (rs.next()) {
                if (throwErrors) {
                    throw new TransformException("Mapping entry not unique:" + searchKey);
                } else {
                    LOG.error("Mapping entry not unique:" + searchKey);
                }
            }
        } else {
            // No entry found
            if (throwErrors) {
                throw new TransformException("Mapping entry not found:" + searchKey);
            } else {
                // Use original code
                sourceCodeSystem = getCodeSystemName(sourceSystemId);
                ret.addCoding().setCode(sourceCodeValue).setSystem(sourceCodeSystem).setDisplay(displayText);
            }
        }
        rs.close();

        return ret;
    }

    /*
     *
     */
    public static String getCodeSystemName(int codeSystemId) throws SQLException {
        LOG.trace("Looking for Code Systems:" + codeSystemId);
        String ret = "UNKNOWN:" + codeSystemId;

        if (codeSystemCache.containsKey(codeSystemId)) {
            ret = codeSystemCache.get(codeSystemId);
        } else {
            LOG.trace("Code System not found:" + codeSystemId);
            if (lastLookupCodeSystemId == codeSystemId) {
                LOG.trace("Same as last time");
                ret = lastLookupCodeSystemIdentifier;
            }
            ResultSet rs = getAllCodeSystemsSelectStatement.executeQuery();
            while (rs.next()) {
                if (!codeSystemCache.containsKey(codeSystemId)) {
                    LOG.trace("Adding:" + rs.getInt(1) + "==>" + rs.getString(2));
                    codeSystemCache.put(rs.getInt(1), rs.getString(2));
                    if (codeSystemId == rs.getInt(1)) {
                        LOG.trace("FOUND");
                        lastLookupCodeSystemId = codeSystemId;
                        lastLookupCodeSystemIdentifier = rs.getString(2);
                        ret = rs.getString(2);
                    }
                }
            }
            rs.close();
        }
        return ret;
    }


    public static void prepareJDBCConnection() throws ClassNotFoundException, SQLException, IOException {
        JsonNode json = ConfigManager.getConfigurationAsJson("hl7receiver_db");

        Class.forName(json.get("drivername").asText());

        Properties connectionProps = new Properties();
        connectionProps.put("user", json.get("username").asText());
        connectionProps.put("password", json.get("password").asText());
        hl7receiverConnection = DriverManager.getConnection(json.get("url").asText(), connectionProps);

        resourceIdSelectStatement = hl7receiverConnection.prepareStatement("SELECT resource_uuid FROM mapping.resource_uuid where scope_id=? and resource_type=? and unique_identifier=?");
        resourceIdInsertStatement = hl7receiverConnection.prepareStatement("insert into mapping.resource_uuid (scope_id, resource_type, unique_identifier, resource_uuid) values (?, ?, ?, ?)");
        mappingSelectStatement = hl7receiverConnection.prepareStatement("SELECT target_code, source_term, target_term FROM mapping.code a INNER JOIN  mapping.code_context d on a.source_code_context_id = d.code_context_id where scope_id=? and d.code_context_name=? and source_code=? and source_code_system_id=? and target_code_system_id=? and is_mapped=true");
        getAllCodeSystemsSelectStatement = hl7receiverConnection.prepareStatement("SELECT code_system_id, code_system_identifier from mapping.code_system order by code_system_id asc");
        getCodeSystemsSelectStatement = hl7receiverConnection.prepareStatement("SELECT code_system_identifier from mapping.code_system where code_system_id=?");
    }

    /*
    Encounter resources are not maintained by this feed. They are only created if missing. Encounter status etc. is maintained by the HL7 feed
 */
    public static ResourceId resolveOrganisationResource(CsvCurrentState currentParserState, String primaryOrgOdsCode, FhirResourceFiler fhirResourceFiler, String name, Address fhirAddress) throws Exception {
        ResourceId resourceId = null;
        String uniqueId = "OdsCode=" + primaryOrgOdsCode;
        resourceId = getResourceId("G", "Organization", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId("G");
            resourceId.setResourceType("Organization");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());

            LOG.trace("Create new Organisation:" + resourceId.getUniqueId() + " resource:" + resourceId.getResourceId());
            saveResourceId(resourceId);

            // Save place-holder EpisodeOfCare
            Organization fhirOrganization = new Organization();

            fhirOrganization.setId(resourceId.getResourceId().toString());

            fhirOrganization.addIdentifier().setSystem("http://fhir.nhs.net/Id/ods-organization-code").setValue(primaryOrgOdsCode);

            CodeableConcept cc = new CodeableConcept();
            cc.addCoding().setSystem("http://endeavourhealth.org/fhir/ValueSet/primarycare-organization-type").setCode("TR").setDisplay("NHS Trust");
            fhirOrganization.setType(cc);

            fhirOrganization.setName(name);

            fhirOrganization.addAddress(fhirAddress);

            LOG.trace("Save Organization:" + FhirSerializationHelper.serializeResource(fhirOrganization));
            saveAdminResource(fhirResourceFiler, currentParserState, fhirOrganization);
        }
        return resourceId;
    }

    /*
        Method must receive either an EncounterId or a CDSUniqueId (to find EncounterId via Tails)
     */
    public static ResourceId getEpisodeOfCareResourceId(String scope, String episodeId) throws Exception {
        String uniqueId = "EpisodeId=" + episodeId;
        return getResourceId(scope, "EpisodeOfCare", uniqueId);
    }

    public static ResourceId createEpisodeOfCareResourceId(String scope, String episodeId) throws Exception {
        String uniqueId = "EpisodeId=" + episodeId;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(scope);
        resourceId.setResourceType("EpisodeOfCare");
        resourceId.setUniqueId(uniqueId);
        resourceId.setResourceId(UUID.randomUUID());
        LOG.trace("Create EpisodeOfCare resourceId:" + resourceId.getUniqueId() + "==>" + resourceId.getResourceId());
        saveResourceId(resourceId);
        return resourceId;
    }

    public static EpisodeOfCare createEpisodeOfCare(CsvCurrentState currentParserState, FhirResourceFiler fhirResourceFiler, ResourceId episodeResourceId, ResourceId patientResourceId, ResourceId organisationResourceId, EpisodeOfCare.EpisodeOfCareStatus status, Date episodeStartDate, Date episodeEndDate, Identifier identifiers[]) throws Exception {
        EpisodeOfCare fhirEpisodeOfCare = new EpisodeOfCare();

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                LOG.trace("Adding identifier to episode:" + identifiers[i].getSystem() + "==>" + identifiers[i].getValue());
                fhirEpisodeOfCare.addIdentifier(identifiers[i]);
            }
        }

        fhirEpisodeOfCare.setId(episodeResourceId.getResourceId().toString());

        fhirEpisodeOfCare.setStatus(status);

        fhirEpisodeOfCare.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        fhirEpisodeOfCare.setManagingOrganization(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));

        Period p = new Period();
        if (episodeStartDate != null) {
            p.setStart(episodeStartDate);
        }
        if (episodeEndDate != null) {
            p.setEnd(episodeEndDate);
        }
        fhirEpisodeOfCare.setPeriod(p);

        LOG.trace("Save fhirEpisodeOfCare:" + FhirSerializationHelper.serializeResource(fhirEpisodeOfCare));
        savePatientResource(fhirResourceFiler, currentParserState, fhirEpisodeOfCare.getId().toString(), fhirEpisodeOfCare);

        return fhirEpisodeOfCare;
    }

    /*
        Method must receive either an EncounterId or a CDSUniqueId (to find EncounterId via Tails)
     */
    public static ResourceId getEncounterResourceId(String scope, String encounterId) throws Exception {
        String uniqueId = "EncounterId=" + encounterId;
        return getResourceId(scope, "Encounter", uniqueId);
    }

    public static ResourceId createEncounterResourceId(String scope, String encounterId) throws Exception {
        String uniqueId = "EncounterId=" + encounterId;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(scope);
        resourceId.setResourceType("Encounter");
        resourceId.setUniqueId(uniqueId);
        resourceId.setResourceId(UUID.randomUUID());
        LOG.trace("Create new Encounter:" + resourceId.getUniqueId() + " resource:" + resourceId.getResourceId());
        saveResourceId(resourceId);
        return resourceId;
    }

    public static Encounter createEncounter(CsvCurrentState currentParserState, FhirResourceFiler fhirResourceFiler, ResourceId patientResourceId, ResourceId episodeOfCareResourceId, ResourceId encounterResourceId, Encounter.EncounterState status, Date periodStart, Date periodEnd, Identifier identifiers[], Encounter.EncounterClass encounterClass) throws Exception {
        // Save place-holder Encounter
        Encounter fhirEncounter = new Encounter();

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirEncounter.addIdentifier(identifiers[i]);
            }
        }

        fhirEncounter.setId(encounterResourceId.getResourceId().toString());

        fhirEncounter.setClass_(encounterClass);

        fhirEncounter.setStatus(status);

        // Period
        Period p = new Period();
        if (periodStart != null) {
            p.setStart(periodStart);
        }
        if (periodEnd != null) {
            p.setEnd(periodEnd);
        }
        fhirEncounter.setPeriod(p);

        // Patient reference
        fhirEncounter.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // EpisodeOfCare reference
        if (episodeOfCareResourceId != null) {
            fhirEncounter.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareResourceId.getResourceId().toString()));
        }

        LOG.trace("Save Encounter:" + FhirSerializationHelper.serializeResource(fhirEncounter));
        savePatientResource(fhirResourceFiler, currentParserState, fhirEncounter.getId().toString(), fhirEncounter);

        return fhirEncounter;
    }

    /*
     *
     */
    public static void createDiagnosis(Condition fhirCondition, ResourceId diagnosisResourceId, ResourceId encounterResourceId, ResourceId patientResourceId, Date dateRecorded, DateTimeType onsetDate, CodeableConcept diagnosisCode, String notes, Identifier identifiers[], Condition.ConditionVerificationStatus cvs, String[] metaUri, Extension[] ex) throws Exception {
        fhirCondition.setId(diagnosisResourceId.getResourceId().toString());

        // Extensions
        if (ex != null) {
            for (int i = 0; i < ex.length; i++) {
                fhirCondition.addExtension(ex[i]);
            }
        }

        // Meta
        Meta meta = new Meta();
        meta.addProfile(FhirProfileUri.PROFILE_URI_CONDITION); // This should always be added to make it compatible with EMIS data for viewing purposes
        if (metaUri != null) {
            for (int i = 0; i < metaUri.length; i++) {
                meta.addProfile(metaUri[i]);
            }
        }
        fhirCondition.setMeta(meta);

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirCondition.addIdentifier(identifiers[i]);
            }
        }

        fhirCondition.setVerificationStatus(cvs);

        // set patient reference
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // Encounter
        if (encounterResourceId != null) {
            fhirCondition.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }

        // Date recorded
        fhirCondition.setDateRecorded(dateRecorded);

        fhirCondition.setOnset(onsetDate);

        if (diagnosisCode.getText() == null || diagnosisCode.getText().length() == 0) {
            diagnosisCode.setText(diagnosisCode.getCoding().get(0).getDisplay());
        }
        fhirCondition.setCode(diagnosisCode);

        // set category to 'diagnosis'
        CodeableConcept cc = new CodeableConcept();
        cc.addCoding().setSystem("http://hl7.org/fhir/condition-category").setCode("diagnosis");
        fhirCondition.setCategory(cc);

        // set verificationStatus - to field 8. Confirmed if value is 'Confirmed' otherwise ????
        //fhirCondition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        // set notes
        if (notes != null) {
            fhirCondition.setNotes(notes);
        }


    }

    /*
     *
     */
    public static void createProcedureResource(Procedure fhirProcedure, ResourceId procedureResourceId, ResourceId encounterResourceId, ResourceId patientResourceId, Procedure.ProcedureStatus status, CodeableConcept procedureCode, Date procedureDate, String notes, Identifier identifiers[], String[] metaUri, Extension[] ex) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Turn key into Resource id
        fhirProcedure.setId(procedureResourceId.getResourceId().toString());

        // Extensions
        if (ex != null) {
            for (int i = 0; i < ex.length; i++) {
                fhirProcedure.addExtension(ex[i]);
            }
        }

        // Meta
        if (metaUri != null) {
            Meta meta = new Meta();
            for (int i = 0; i < metaUri.length; i++) {
                meta.addProfile(metaUri[i]);
            }
            fhirProcedure.setMeta(meta);
        }

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirProcedure.addIdentifier(identifiers[i]);
            }
        }

        // Encounter
        if (encounterResourceId != null) {
            fhirProcedure.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }

        // set patient reference
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // status
        fhirProcedure.setStatus(status);

        // Code
        if (procedureCode.getText() == null || procedureCode.getText().length() == 0) {
            procedureCode.setText(procedureCode.getCoding().get(0).getDisplay());
        }
        fhirProcedure.setCode(procedureCode);

        // Performed date/time
        if (procedureDate != null) {
            //Timing t = new Timing().addEvent(procedureDate);
            DateTimeType dateDt = new DateTimeType(procedureDate);
            fhirProcedure.setPerformed(dateDt);
        }

        // set notes
        if (notes != null) {
            fhirProcedure.addNotes(new Annotation().setText(notes));
        }

    }

    /*
     *
     */
    public static ResourceId resolvePatientResource(String scope, String uniqueId, CsvCurrentState currentParserState, String primaryOrgHL7OrgOID, FhirResourceFiler fhirResourceFiler, String mrn, String nhsno, HumanName name, Address fhirAddress, Enumerations.AdministrativeGender gender, Date dob, ResourceId organisationResourceId, CodeableConcept maritalStatus, Identifier identifiers[], ResourceId gp, ResourceId gpPractice, CodeableConcept ethnicGroup) throws Exception {
        if (uniqueId == null) {
            // Default format is for Barts
            uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + mrn;
        }
        ResourceId patientResourceId = getResourceId(scope, "Patient", uniqueId);
        if (patientResourceId == null) {
            patientResourceId = new ResourceId();
            patientResourceId.setScopeId(scope);
            patientResourceId.setResourceType("Patient");
            patientResourceId.setUniqueId(uniqueId);
            patientResourceId.setResourceId(UUID.randomUUID());

            LOG.trace("Create new Patient:" + patientResourceId.getUniqueId() + " resource:" + patientResourceId.getResourceId());

            saveResourceId(patientResourceId);

            // Create patient
            Patient fhirPatient = new Patient();

            fhirPatient.setId(patientResourceId.getResourceId().toString());

            if (ethnicGroup != null) {
                Extension ex = new Extension();
                ex.setUrl(FhirExtensionUri.PATIENT_ETHNICITY);
                ex.setValue(ethnicGroup);
                fhirPatient.addExtension(ex);
            }

            if (identifiers != null) {
                for (int i = 0; i < identifiers.length; i++) {
                    fhirPatient.addIdentifier(identifiers[i]);
                }
            }

            if (nhsno != null && nhsno.length() > 0) {
                Identifier patientIdentifier = new Identifier()
                        .setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER)
                        .setValue(nhsno.replaceAll("\\-", ""));
                fhirPatient.addIdentifier(patientIdentifier);

            }

            //Example: HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(HumanName.NameUse.OFFICIAL, parser.getPatientTitle(), parser.getPatientForename(), "", parser.getPatientSurname());
            if (name != null) {
                fhirPatient.addName(name);
            }

            //Example: Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME, parser.getAddress1(), parser.getAddress2(), parser.getAddress3(), parser.getAddress4(), parser.getAddress5(), parser.getPostCode());
            if (fhirAddress != null) {
                fhirPatient.addAddress(fhirAddress);
            }

            // Gender
            // Enumerations.AdministrativeGender
            if (gender != null) {
                fhirPatient.setGender(gender);
            }

            if (dob != null) {
                fhirPatient.setBirthDate(dob);
            }

            if (maritalStatus != null) {
                fhirPatient.setMaritalStatus(maritalStatus);
            }

            if (organisationResourceId != null) {
                fhirPatient.setManagingOrganization(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));
            }

            if (gp != null) {
                fhirPatient.addCareProvider(ReferenceHelper.createReference(ResourceType.Practitioner, gp.getResourceId().toString()));
            }

            if (gpPractice != null) {
                fhirPatient.addCareProvider(ReferenceHelper.createReference(ResourceType.Organization, gpPractice.getResourceId().toString()));
            }

            LOG.trace("Save Patient:" + FhirSerializationHelper.serializeResource(fhirPatient));
            savePatientResource(fhirResourceFiler, currentParserState, patientResourceId.getResourceId().toString(), fhirPatient);
        } else {
            // Check merge history
            if (mergeDAL == null) {
                mergeDAL = DalProvider.factoryResourceMergeDal();
            }
            UUID newResourceId = mergeDAL.resolveMergeUUID(fhirResourceFiler.getServiceId(),"Patient", patientResourceId.getResourceId());
            patientResourceId.setResourceId(newResourceId);
        }
        return patientResourceId;
    }

    public static ResourceId getPatientResourceId(String scope, String primaryOrgHL7OrgOID, String mrn) throws Exception {
        String uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + mrn;
        return getResourceId(scope, "Patient", uniqueId);
    }

    public static ResourceId getProblemResourceId(String scope, String patientId, String onsetDate, String problem) throws Exception {
        String uniqueId = "PatientId=" + patientId + "-OnsetDate=" + onsetDate + "-ProblemCode=" + problem;
        return getConditionResourceId(scope, uniqueId);
    }

    public static ResourceId readProblemResourceId(String scope, String patientId, String onsetDate, String problem) throws Exception {
        String uniqueId = "PatientId=" + patientId + "-OnsetDate=" + onsetDate + "-ProblemCode=" + problem;
        return getResourceId(scope, "Condition", uniqueId);
    }

    public static ResourceId getDiagnosisResourceIdFromCDSData(String scope, String CDSUniqueID, String diagnosis, Integer repetition) throws Exception {
        String uniqueId = "CDSIdValue=" + CDSUniqueID + "-DiagnosisCode=" + diagnosis + "-Repetition=" + repetition.toString();
        return getConditionResourceId(scope, uniqueId);
    }

    public static ResourceId getDiagnosisResourceId(String scope, String patientId, String diagnosisDate, String diagnosis) throws Exception {
        String uniqueId = "PatientId=" + patientId + "-DiagnosisDate=" + diagnosisDate + "-DiagnosisCode=" + diagnosis;
        return getConditionResourceId(scope, uniqueId);
    }

    public static ResourceId readDiagnosisResourceId(String scope, String patientId, String diagnosisDate, String diagnosis) throws Exception {
        String uniqueId = "PatientId=" + patientId + "-DiagnosisDate=" + diagnosisDate + "-DiagnosisCode=" + diagnosis;
        return getResourceId(scope, "Condition", uniqueId);
    }

    public static ResourceId getObservationResourceId(String scope, String patientId, String observationDate, String observationCode) throws Exception {
        String uniqueId = "PatientId=" + patientId + "-ObservationDate=" + observationDate + "-ObservationCode=" + observationCode;
        return getObservationResourceId(scope, uniqueId);
    }

    public static ResourceId getConditionResourceId(String scope, String uniqueId) throws Exception {
        ResourceId resourceId = getResourceId(scope, "Condition", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(scope);
            resourceId.setResourceType("Condition");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }
        return resourceId;
    }


    public static ResourceId getProcedureResourceId(String scope, String encounterId, String procedureDateTime, String procedureCode, Integer repetition) throws Exception {
        String uniqueId = null;
        if (repetition.intValue() == 0) {
            uniqueId = "EncounterId=" + encounterId + "-ProcedureDateTime=" + procedureDateTime + "-ProcedureCode=" + procedureCode;
        } else {
            uniqueId = "EncounterId=" + encounterId + "-ProcedureDateTime=" + procedureDateTime + "-ProcedureCode=" + procedureCode + "-Repetition=" + repetition.toString();
        }
        ResourceId resourceId = getResourceId(scope, "Procedure", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(scope);
            resourceId.setResourceType("Procedure");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }
        return resourceId;
    }

    public static ResourceId getObservationResourceId(String scope, String uniqueId) throws Exception {
        ResourceId resourceId = getResourceId(scope, "Observation", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(scope);
            resourceId.setResourceType("Observation");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }
        return resourceId;
    }

    public static ResourceId readProcedureResourceId(String scope, String encounterId, String procedureDateTime, String procedureCode) throws Exception {
        String uniqueId = "EncounterId=" + encounterId + "-ProcedureDateTime=" + procedureDateTime + "-ProcedureCode=" + procedureCode;
        return getResourceId(scope, "Procedure", uniqueId);
    }

    /*
     *
     */
    public static ResourceId getGPResourceId(String scope, String gmcCode) throws Exception {
        String uniqueId = "GmcCode=" + gmcCode;
        return getResourceId(scope, "Practitioner", uniqueId);
    }

    /*
     *
     */
    public static ResourceId createGPResourceId(String scope, String gmcCode) throws Exception {
        String uniqueId = "GmcCode=" + gmcCode;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(scope);
        resourceId.setResourceType("Practitioner");
        resourceId.setUniqueId(uniqueId);
        resourceId.setResourceId(UUID.randomUUID());
        saveResourceId(resourceId);
        return resourceId;
    }

    public static ResourceId getPractitionerResourceId(String scope, CsvCell personnelIdCell) throws Exception {

        String uniqueId = "PersonnelId=" + personnelIdCell.getString();
        ResourceId resourceId = getResourceId(scope, "Practitioner", uniqueId);

        //if we failed to find a UUID for our personnelID, generate one and return it
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(scope);
            resourceId.setResourceType("Practitioner");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }
        return resourceId;
    }


    /*public static ResourceId getPractitionerResourceId(String scope, String personnelId) throws Exception {
        String uniqueId = "PersonnelId=" + personnelId;
        //if we failed to find a UUID for our personnelID, generate one and return it
        //return getResourceId(scope, "Practitioner", uniqueId);
        ResourceId ret = getResourceId(scope, "Practitioner", uniqueId);
        if (ret == null) {
            ret = createPractitionerResourceId(scope, personnelId);
        }
    }

    public static ResourceId createPractitionerResourceId(String scope, String personnelId) throws Exception {
        String uniqueId = "PersonnelId=" + personnelId;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(scope);
        resourceId.setResourceType("Practitioner");
        resourceId.setUniqueId(uniqueId);
        resourceId.setResourceId(UUID.randomUUID());
        saveResourceId(resourceId);
        return resourceId;
    }*/

    public static ResourceId createPatientResourceId(String scope, String primaryOrgHL7OrgOID, String mrn) throws Exception {
        String uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + mrn;
        ResourceId patientResourceId = new ResourceId();
        patientResourceId.setScopeId(scope);
        patientResourceId.setResourceType("Patient");
        patientResourceId.setUniqueId(uniqueId);
        patientResourceId.setResourceId(UUID.randomUUID());
        saveResourceId(patientResourceId);
        return patientResourceId;
    }

    /*
     *
     */
    public static ResourceId getLocationResourceId(String scope, String locationId) throws Exception {
        String uniqueId = "LocationId=" + locationId;
        return getResourceId(scope, "Location", uniqueId);
    }

    /*
     *
     */
    public static ResourceId createLocationResourceId(String scope, String locationId) throws Exception {
        String uniqueId = "LocationId=" + locationId;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(scope);
        resourceId.setResourceType("Location");
        resourceId.setUniqueId(uniqueId);
        resourceId.setResourceId(UUID.randomUUID());
        saveResourceId(resourceId);
        return resourceId;
    }

    /*
 *
 */
    public static ResourceId getGlobalOrgResourceId(String odsCode) throws Exception {
        String uniqueId = "OdsCode=" + odsCode;
        return getResourceId("G", "Organization", uniqueId);
    }

    /*
     *
     */
    public static ResourceId createGlobalOrgResourceId(String odsCode) throws Exception {
        String uniqueId = "OdsCode=" + odsCode;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId("G");
        resourceId.setResourceType("Organization");
        resourceId.setUniqueId(uniqueId);
        resourceId.setResourceId(UUID.randomUUID());
        saveResourceId(resourceId);
        return resourceId;
    }

    /*
    *
    */
    public static void createProblemResource(Condition fhirCondition, ResourceId problemResourceId, ResourceId patientResourceId, ResourceId encounterResourceId, Date dateRecorded, CodeableConcept problemCode, DateTimeType onsetDate, String notes, Identifier identifiers[], Extension[] ex, Condition.ConditionVerificationStatus cvs) throws Exception {
        fhirCondition.setId(problemResourceId.getResourceId().toString());

        fhirCondition.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PROBLEM));

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirCondition.addIdentifier(identifiers[i]);
            }
        }

        // Extensions
        if (ex != null) {
            for (int i = 0; i < ex.length; i++) {
                fhirCondition.addExtension(ex[i]);
            }
        }

        if (encounterResourceId != null) {
            fhirCondition.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }
        // set patient reference
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // Date recorded
        fhirCondition.setDateRecorded(dateRecorded);

        // set code to coded problem
        if (problemCode.getText() == null || problemCode.getText().length() == 0) {
            problemCode.setText(problemCode.getCoding().get(0).getDisplay());
        }
        fhirCondition.setCode(problemCode);

        // set category to 'complaint'
        CodeableConcept cc = new CodeableConcept();
        cc.addCoding().setSystem(FhirValueSetUri.VALUE_SET_CONDITION_CATEGORY).setCode("complaint");
        fhirCondition.setCategory(cc);

        // set onset to field  to field 10 + 11
        fhirCondition.setOnset(onsetDate);

        fhirCondition.setVerificationStatus(cvs);

        // set notes
        if (notes != null) {
            fhirCondition.setNotes(notes);
        }
    }

    public static ResourceId getOrCreateDiagnosticReportResourceId(String scope, CsvCell observationIdCell) throws Exception {
        return getOrCreateResourceId(scope, ResourceType.DiagnosticReport, observationIdCell);
    }

    public static ResourceId getOrCreateObservationResourceId(String scope, CsvCell observationIdCell) throws Exception {
        return getOrCreateResourceId(scope, ResourceType.Observation, observationIdCell);
    }

    public static ResourceId getOrCreateProcedureResourceId(String scope, CsvCell procedureIdCell) throws Exception {
        return getOrCreateResourceId(scope, ResourceType.Procedure, procedureIdCell);
    }

    public static ResourceId getOrCreateConditionResourceId(String scope, CsvCell conditionIdCell) throws Exception {
        return getOrCreateResourceId(scope, ResourceType.Condition, conditionIdCell);
    }

    private static ResourceId getOrCreateResourceId(String scope, ResourceType resourceType, CsvCell uniqueIdCell) throws Exception {
        ResourceId resourceId = getResourceId(scope, resourceType.toString(), uniqueIdCell.getString());
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(scope);
            resourceId.setResourceType(resourceType.toString());
            resourceId.setUniqueId(uniqueIdCell.getString());
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }
        return resourceId;
    }


    public static Enumerations.AdministrativeGender convertSusGenderToFHIR(int gender) {
        if (gender == 1) {
            return Enumerations.AdministrativeGender.MALE;
        } else {
            if (gender == 2) {
                return Enumerations.AdministrativeGender.FEMALE;
            } else {
                if (gender == 9) {
                    return Enumerations.AdministrativeGender.NULL;
                } else {
                    return Enumerations.AdministrativeGender.UNKNOWN;
                }
            }
        }
    }

    public static void deletePatientResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, ResourceBuilderBase... resourceBuilders) throws Exception {
        fhirResourceFiler.deletePatientResource(parserState, false, resourceBuilders);
    }

    public static void deletePatientResourceMapIds(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, ResourceBuilderBase... resourceBuilders) throws Exception {
        fhirResourceFiler.deletePatientResource(parserState, true, resourceBuilders);
    }

    public static void savePatientResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, ResourceBuilderBase... resourceBuilders) throws Exception {
        fhirResourceFiler.savePatientResource(parserState, false, resourceBuilders);
    }

    public static void savePatientResourceMapIds(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, ResourceBuilderBase... resourceBuilders) throws Exception {
        fhirResourceFiler.savePatientResource(parserState, true, resourceBuilders);
    }

    public static void saveAdminResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, ResourceBuilderBase... resourceBuilders) throws Exception {
        fhirResourceFiler.saveAdminResource(parserState, false, resourceBuilders);
    }

    public static void saveAdminResourceMapIds(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, ResourceBuilderBase... resourceBuilders) throws Exception {
        fhirResourceFiler.saveAdminResource(parserState, true, resourceBuilders);
    }

    //TO DELETE AFTER CHANGING TRANSFORMS TO USE BUILDERS

    public static void deletePatientResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, String groupId, Resource... resources) throws Exception {
        fhirResourceFiler.deletePatientResource(parserState, false, resources);
    }

    public static void deletePatientResourceMapIds(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, String groupId, Resource... resources) throws Exception {
        fhirResourceFiler.deletePatientResource(parserState, true, resources);
    }

    public static void savePatientResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, String groupId, Resource... resources) throws Exception {
        fhirResourceFiler.savePatientResource(parserState, false, resources);
    }

    public static void saveAdminResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, Resource... resources) throws Exception {
        fhirResourceFiler.saveAdminResource(parserState, false, resources);

    }

    public static void savePatientResourceMapIds(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, String groupId, Resource... resources) throws Exception {
        fhirResourceFiler.savePatientResource(parserState, true, resources);
    }

    public static void saveAdminResourceMapIds(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, Resource... resources) throws Exception {
        fhirResourceFiler.saveAdminResource(parserState, true, resources);
    }

}
