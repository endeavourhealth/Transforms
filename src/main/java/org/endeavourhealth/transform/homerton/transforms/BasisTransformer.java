package org.endeavourhealth.transform.homerton.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.CsvCurrentState;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Enumerations;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    public static ResourceId getResourceId(String scope, String resourceType, String uniqueId) throws SQLException, ClassNotFoundException, IOException {
        ResourceId ret = null;
        if (hl7receiverConnection == null) {
            prepareJDBCConnection();
        }

        resourceIdSelectStatement.setString(1, scope);
        resourceIdSelectStatement.setString(2, resourceType);
        resourceIdSelectStatement.setString(3, uniqueId);

        ResultSet rs = resourceIdSelectStatement.executeQuery();
        if (rs.next()) {
            ret = new ResourceId();
            ret.setScopeId(scope);
            ret.setResourceType(resourceType);
            ret.setResourceId((UUID) rs.getObject(1));
        }
        rs.close();

        return ret;
    }


    public static void saveResourceId(ResourceId r) throws SQLException, ClassNotFoundException, IOException {
        if (hl7receiverConnection == null) {
            prepareJDBCConnection();
        }

        resourceIdInsertStatement.setString(1, r.getScopeId());
        resourceIdInsertStatement.setString(2, r.getResourceType());
        resourceIdInsertStatement.setString(3, r.getUniqueId());
        resourceIdInsertStatement.setObject(4, r.getResourceId());

        if (resourceIdInsertStatement.executeUpdate() != 1) {
            throw new SQLException("Could not create ResourceId:" + r.getScopeId() + ":" + r.getResourceType() + ":" + r.getUniqueId() + ":" + r.getResourceId().toString());
        }
    }

    /*
        set sourceCodeSystemId to -1 if no system is defined
     */
    public static CodeableConcept mapToCodeableConcept(String scope, String sourceContextName, String sourceCodeValue, int sourceSystemId, int targetSystemId, String displayText, boolean throwErrors) throws TransformException, SQLException, IOException, ClassNotFoundException {
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
    public static ResourceId resolveOrganisationResource(CsvCurrentState currentParserState, String primaryOrgOdsCode, FhirResourceFiler fhirResourceFiler) throws Exception {
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

            fhirOrganization.setName("Homerton University Hospital NHS Foundation Trust");

            Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
            fhirOrganization.addAddress(fhirAddress);

            LOG.debug("Save Organization:" + FhirSerializationHelper.serializeResource(fhirOrganization));
            saveAdminResource(fhirResourceFiler, currentParserState, fhirOrganization);
        }
        return resourceId;
    }

    /*
        Method must receive either an EncounterId or a CDSUniqueId (to find EncounterId via Tails)
     */
    public static ResourceId getEpisodeOfCareResourceId(String episodeId) throws Exception {
        String uniqueId = "EpisodeId=" + episodeId;
        return getResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, "EpisodeOfCare", uniqueId);
    }

    public static ResourceId createEpisodeOfCareResourceId(String episodeId) throws Exception {
        String uniqueId = "EpisodeId=" + episodeId;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE);
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
                LOG.debug("Adding identifier to episode:" + identifiers[i].getSystem() + "==>" + identifiers[i].getValue());
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

        LOG.debug("Save fhirEpisodeOfCare:" + FhirSerializationHelper.serializeResource(fhirEpisodeOfCare));
        savePatientResource(fhirResourceFiler, currentParserState, fhirEpisodeOfCare.getId().toString(), fhirEpisodeOfCare);

        return fhirEpisodeOfCare;
    }

    /*
        Method must receive either an EncounterId or a CDSUniqueId (to find EncounterId via Tails)
     */
    public static ResourceId getEncounterResourceId(String encounterId) throws Exception {
        String uniqueId = "EncounterId=" + encounterId;
        return getResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, "Encounter", uniqueId);
    }

    public static ResourceId createEncounterResourceId(String encounterId) throws Exception {
        String uniqueId = "EncounterId=" + encounterId;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE);
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

        LOG.debug("Save Encounter:" + FhirSerializationHelper.serializeResource(fhirEncounter));
        savePatientResource(fhirResourceFiler, currentParserState, fhirEncounter.getId().toString(), fhirEncounter);

        return fhirEncounter;
    }

    public static ResourceId resolvePatientResource(CsvCurrentState currentParserState, String primaryOrgHL7OrgOID, FhirResourceFiler fhirResourceFiler, String mrn, String nhsno, HumanName name, Address fhirAddress, Enumerations.AdministrativeGender gender, Date dob, ResourceId organisationResourceId, CodeableConcept maritalStatus) throws Exception {
        String uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + mrn;
        ResourceId patientResourceId = getResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, "Patient", uniqueId);
        if (patientResourceId == null) {
            patientResourceId = new ResourceId();
            patientResourceId.setScopeId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE);
            patientResourceId.setResourceType("Patient");
            patientResourceId.setUniqueId(uniqueId);
            patientResourceId.setResourceId(UUID.randomUUID());

            LOG.trace("Create new Patient:" + patientResourceId.getUniqueId() + " resource:" + patientResourceId.getResourceId());

            saveResourceId(patientResourceId);

            // Create patient
            Patient fhirPatient = new Patient();

            fhirPatient.setId(patientResourceId.getResourceId().toString());

            Identifier patientIdentifier = new Identifier()
                    .setSystem("http://endeavourhealth.org/fhir/id/v2-local-patient-id/barts-mrn")
                    .setValue(StringUtils.deleteWhitespace(mrn));
            fhirPatient.addIdentifier(patientIdentifier);

            if (nhsno != null && nhsno.length() > 0) {
                patientIdentifier = new Identifier()
                        .setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_NHS_NO)
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

            LOG.debug("Save Patient:" + FhirSerializationHelper.serializeResource(fhirPatient));
            savePatientResource(fhirResourceFiler, currentParserState, patientResourceId.getResourceId().toString(), fhirPatient);
        }
        return patientResourceId;
    }

    public static ResourceId getProblemResourceId(String patientId, String onsetDate, String problem) throws Exception {
        String uniqueId = "PatientId=" + patientId + "-OnsetDate=" + onsetDate + "-ProblemCode=" + problem;
        return getConditionResourceId(uniqueId);
    }

    public static ResourceId getDiagnosisResourceIdFromCDSData(String CDSUniqueID, String diagnosis) throws Exception {
        String uniqueId = "CDSIdValue=" + CDSUniqueID + "-DiagnosisCode=" + diagnosis;
        return getConditionResourceId(uniqueId);
    }

    public static ResourceId getDiagnosisResourceId(String patientId, String diagnosisDate, String diagnosis) throws Exception {
        String uniqueId = "PatientId=" + patientId + "-DiagnosisDate=" + diagnosisDate + "-DiagnosisCode=" + diagnosis;
        return getConditionResourceId(uniqueId);
    }

    public static ResourceId getConditionResourceId(String uniqueId) throws Exception {
        ResourceId resourceId = getResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, "Condition", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE);
            resourceId.setResourceType("Condition");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }
        return resourceId;
    }

    public static ResourceId getProcedureResourceId(String encounterId, String procedureDateTime, String procedureCode) throws Exception {
        String uniqueId = "EncounterId=" + encounterId + "-ProcedureDateTime=" + procedureDateTime + "-ProcedureCode=" + procedureCode;
        ResourceId resourceId = getResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, "Procedure", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE);
            resourceId.setResourceType("Procedure");
            resourceId.setUniqueId(uniqueId);
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

    public static void deletePatientResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, String groupId, Resource... resources) throws Exception {
        fhirResourceFiler.deletePatientResource(parserState,false, groupId,resources);
    }

    public static void savePatientResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, String groupId, Resource... resources) throws Exception {
        fhirResourceFiler.savePatientResource(parserState, false, groupId, resources);
    }

    public static void saveAdminResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, Resource... resources) throws Exception {
        fhirResourceFiler.saveAdminResource(parserState, false, resources);

    }

}
