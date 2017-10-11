package org.endeavourhealth.transform.barts.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.TailsRecord;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.CsvCurrentState;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class BasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BasisTransformer.class);

    private static Connection hl7receiverConnection = null;
    private static PreparedStatement resourceIdSelectStatement;
    private static PreparedStatement resourceIdInsertStatement;
    private static PreparedStatement mappingSelectStatement;


    public static ResourceId getResourceId(String scope, String resourceType, String uniqueId) throws SQLException, ClassNotFoundException, IOException {
        //ResourceId resourceId = ResourceIdHelper.getResourceId("B", "Condition", uniqueId);
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
    public static CodeableConcept mapToCodeableConcept(String scope, int sourceContextId, String sourceCodeValue, int sourceCodeSystemId, int targetSystemId, boolean throwErrors) throws TransformException, SQLException, IOException, ClassNotFoundException {
        String searchKey = "scope=" + scope + ":sourceContextId=" + sourceContextId + ":sourceCodeValue=" + sourceCodeValue + ":sourceCodeSystemId=" + sourceCodeSystemId + ":targetSystemId=" + targetSystemId;
        LOG.trace("Looking for:" + searchKey);
        CodeableConcept ret = null;
        if (hl7receiverConnection == null) {
            prepareJDBCConnection();
        }

        mappingSelectStatement.setString(1, scope);
        mappingSelectStatement.setInt(2, sourceContextId);
        mappingSelectStatement.setString(3, sourceCodeValue);
        mappingSelectStatement.setInt(4, sourceCodeSystemId);
        mappingSelectStatement.setInt(5, targetSystemId);

        ResultSet rs = mappingSelectStatement.executeQuery();
        if (rs.next()) {
            ret = new CodeableConcept();
            ret.addCoding().setSystem(rs.getString(2)).setCode(rs.getString(1));
            if (rs.getString(3).length() > 0) {
                ret.addCoding().setSystem(rs.getString(3)).setCode(sourceCodeValue);
            }
            if (rs.next()) {
                if (throwErrors) {
                    throw new TransformException("Mapping entry not unique:" + searchKey);
                } else {
                    ret = null;
                    LOG.error("Mapping entry not unique:" + searchKey);
                }
            }
        } else {
            if (throwErrors) {
                throw new TransformException("Mapping entry not found:" + searchKey);
            } else {
                LOG.error("Mapping entry not found:" + searchKey);
            }
        }
        rs.close();

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
        mappingSelectStatement = hl7receiverConnection.prepareStatement("SELECT target_code, b.code_system_identifier as target_code_system, c.code_system_identifier as source_code_system FROM mapping.code a INNER JOIN  mapping.code_system b on a.target_code_system_id = b.code_system_id INNER JOIN  mapping.code_system c on a.source_code_system_id = c.code_system_id where scope_id=? and source_code_context_id =? and source_code=? and source_code_system_id=? and target_code_system_id=? and is_mapped=true");
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

            fhirOrganization.setName("Barts Health NHS Trust");

            Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
            fhirOrganization.addAddress(fhirAddress);

            LOG.debug("Save Organization:" + FhirSerializationHelper.serializeResource(fhirOrganization));
            saveAdminResource(fhirResourceFiler, currentParserState, fhirOrganization);
        }
        return resourceId;
    }

    /*
        Encounter resources are not maintained by this feed. They are only created if missing. Encounter status etc. is maintained by the HL7 feed
        Method must receive either an EncounterId or a CDSUniqueId (to find EncounterId via Tails)
        FINNo is optional
     */
    public static ResourceId resolveEpisodeResource(CsvCurrentState currentParserState, String primaryOrgHL7OrgOID, String CDSUniqueID, String localPatientId, String encounterId, String FINNbr, FhirResourceFiler fhirResourceFiler, ResourceId patientResourceId, ResourceId organisationResourceId, Date episodeStartDate, EpisodeOfCare.EpisodeOfCareStatus status) throws Exception {
        ResourceId resourceId = null;
        if (encounterId == null) {
            TailsRecord tr = TailsPreTransformer.getTailsRecord(CDSUniqueID);
            encounterId = tr.getEncounterId();
            if (FINNbr == null) {
                tr.getFINNbr();
            }
        }
        // For Barts FINNo should probably be used here but existing HL7 feed uses encounter id/visit id
        String uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + localPatientId + "-EpIdTypeCode=VISITID-EpIdValue=" + encounterId;
        resourceId = getResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, "EpisodeOfCare", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE);
            resourceId.setResourceType("EpisodeOfCare");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());

            LOG.trace("Create new EpisodeOfCare:" + resourceId.getUniqueId() + " resource:" + resourceId.getResourceId());
            saveResourceId(resourceId);

            // Save place-holder EpisodeOfCare
            EpisodeOfCare fhirEpisodeOfCare = new EpisodeOfCare();
            fhirEpisodeOfCare.setId(resourceId.getResourceId().toString());

            if (FINNbr != null) {
                fhirEpisodeOfCare.addIdentifier().setSystem("http://endeavourhealth.org/fhir/id/v2-local-episode-id/barts-fin").setValue(FINNbr);
            }
            fhirEpisodeOfCare.addIdentifier().setSystem("http://endeavourhealth.org/fhir/id/v2-local-episode-id/barts-visitno").setValue(encounterId);

            fhirEpisodeOfCare.setStatus(status);

            fhirEpisodeOfCare.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

            fhirEpisodeOfCare.setManagingOrganization(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));

            Period fhirPeriod = new Period();
            fhirPeriod.setStart(episodeStartDate);
            fhirEpisodeOfCare.setPeriod(fhirPeriod);

            LOG.debug("Save fhirEpisodeOfCare:" + FhirSerializationHelper.serializeResource(fhirEpisodeOfCare));
            savePatientResource(fhirResourceFiler, currentParserState, fhirEpisodeOfCare.getId().toString(), fhirEpisodeOfCare);
        }
        return resourceId;
    }

    /*
        Encounter resources are not maintained by this feed. They are only created if missing. Encounter status etc. is maintained by the HL7 feed
     */
    public static ResourceId resolveEncounterResource(CsvCurrentState currentParserState, String primaryOrgHL7OrgOID, String CDSUniqueID, String localPatientId, String encounterId, FhirResourceFiler fhirResourceFiler, ResourceId patientResourceId, ResourceId episodeOfCareResourceId, Encounter.EncounterState status) throws Exception {
        ResourceId resourceId = null;
        if (encounterId == null) {
            TailsRecord tr = TailsPreTransformer.getTailsRecord(CDSUniqueID);
            encounterId = tr.getEncounterId();
        }

        String uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + localPatientId + "-EpIdTypeCode=VISITID-EpIdValue=" + encounterId;
        resourceId = getResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, "Encounter", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE);
            resourceId.setResourceType("Encounter");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());

            LOG.trace("Create new Encounter:" + resourceId.getUniqueId() + " resource:" + resourceId.getResourceId());
            saveResourceId(resourceId);

            // Save place-holder Encounter
            Encounter fhirEncounter = new Encounter();
            fhirEncounter.setId(resourceId.getResourceId().toString());

            fhirEncounter.setStatus(status);

            fhirEncounter.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

            fhirEncounter.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareResourceId.getResourceId().toString()));

            LOG.debug("Save Encounter:" + FhirSerializationHelper.serializeResource(fhirEncounter));
            savePatientResource(fhirResourceFiler, currentParserState, fhirEncounter.getId().toString(), fhirEncounter);
        }

        return resourceId;
    }

    public static ResourceId resolvePatientResource(CsvCurrentState currentParserState, String primaryOrgHL7OrgOID, FhirResourceFiler fhirResourceFiler, String mrn, String nhsno, HumanName name, Address fhirAddress, Enumerations.AdministrativeGender gender, Date dob, ResourceId organisationResourceId, CodeableConcept maritalStatus) throws Exception {
        String uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + mrn;
        ResourceId patientResourceId = getResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, "Patient", uniqueId);
        if (patientResourceId == null) {
            patientResourceId = new ResourceId();
            patientResourceId.setScopeId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE);
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
                        .setSystem("http://fhir.nhs.net/Id/nhs-number")
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

    public static ResourceId resolveProblemResourceId(String primaryOrgOdsCode, FhirResourceFiler fhirResourceFiler, String patientId, String onsetDate, String problem) throws Exception {
        String uniqueId = "ParentOdsCode=" + primaryOrgOdsCode + "-PatientId=" + patientId + "-OnsetDate=" + onsetDate + "-ProblemCode=" + problem;
        return resolveConditionResourceId(uniqueId, fhirResourceFiler);
    }

    public static ResourceId resolveDiagnosisResourceIdFromCDSData(String primaryOrgOdsCode, FhirResourceFiler fhirResourceFiler, String CDSUniqueID, String diagnosis) throws Exception {
        String uniqueId = "ParentOdsCode=" + primaryOrgOdsCode + "-CDSIdValue=" + CDSUniqueID + "-DiagnosisCode=" + diagnosis;
        return resolveConditionResourceId(uniqueId, fhirResourceFiler);
    }

    public static ResourceId resolveDiagnosisResourceId(String primaryOrgOdsCode, FhirResourceFiler fhirResourceFiler, String patientId, String diagnosisDate, String diagnosis) throws Exception {
        String uniqueId = "ParentOdsCode=" + primaryOrgOdsCode + "-PatientId=" + patientId + "-DiagnosisDate=" + diagnosisDate + "-DiagnosisCode=" + diagnosis;
        return resolveConditionResourceId(uniqueId, fhirResourceFiler);
    }

    public static ResourceId resolveConditionResourceId(String uniqueId, FhirResourceFiler fhirResourceFiler) throws Exception {
        ResourceId resourceId = getResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, "Condition", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE);
            resourceId.setResourceType("Condition");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }
        return resourceId;
    }

    public static ResourceId resolveProcedureResourceId(String primaryOrgOdsCode, FhirResourceFiler fhirResourceFiler, String CDSUniqueID, String patientId, String encounterId, String procedureDateTime, String procedureCode) throws Exception {
        if (encounterId == null) {
            TailsRecord tr = TailsPreTransformer.getTailsRecord(CDSUniqueID);
            encounterId = tr.getEncounterId();
        }
        String uniqueId = "ParentOdsCode=" + primaryOrgOdsCode + "-PatientId=" + patientId + "-EncounterId=" + encounterId + "-ProcedureDateTime=" + procedureDateTime + "-ProcedureCode=" + procedureCode;
        ResourceId resourceId = getResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, "Procedure", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE);
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
