package org.endeavourhealth.transform.barts.transformsOld;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.Hl7ResourceIdDalI;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceMergeDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class BasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BasisTransformer.class);

    private static Hl7ResourceIdDalI hl7ResourceIdDal = DalProvider.factoryHL7ResourceDal();

    private static ResourceMergeDalI mergeDAL = null;

    /*
     * Example: ResourceId resourceId = ResourceIdHelper.getResourceId("B", "Condition", uniqueId);
     */
    public static ResourceId getResourceId(String scope, String resourceType, String uniqueId) throws Exception {

        //all the Cerner 2.2 transforms have been rewritten to use standard ID mapping
        //so all the 2.1 stuff no longer works. This needs fixing (to use standard mapping) and testing
        if (true) {
            throw new RuntimeException("This node needs fixing");
        }
        return null;

        /*//Try to find the resourceId in the cache first
        String resourceIdLookup = scope + "|" + resourceType + "|" + uniqueId;
        ResourceId resourceId  = BartsCsvHelper.getResourceIdFromCache (resourceIdLookup);
        if (resourceId != null) {
            return resourceId;
        }

        resourceId = hl7ResourceIdDal.getResourceId(scope, resourceType, uniqueId);

        return resourceId;*/
    }


    public static void saveResourceId(ResourceId resourceId) throws Exception {
        hl7ResourceIdDal.saveResourceId(resourceId);
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
            saveAdminResource(fhirResourceFiler, currentParserState, new OrganizationBuilder(fhirOrganization));
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

    public static ResourceId createEpisodeOfCareResourceId(String scope, String episodeId, UUID resourceUUID) throws Exception {
        String uniqueId = "EpisodeId=" + episodeId;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(scope);
        resourceId.setResourceType("EpisodeOfCare");
        resourceId.setUniqueId(uniqueId);
        resourceId.setResourceId(resourceUUID);
        LOG.trace("Create EpisodeOfCare resourceId:" + resourceId.getUniqueId() + "==>" + resourceId.getResourceId());
        saveResourceId(resourceId);
        return resourceId;
    }

    public static ResourceId createEpisodeOfCareResourceId(String scope, String episodeId) throws Exception {
        return createEpisodeOfCareResourceId(scope, episodeId, UUID.randomUUID());
        /*
        String uniqueId = "EpisodeId=" + episodeId;
        ResourceId resourceId = new ResourceId();
        resourceId.setScopeId(scope);
        resourceId.setResourceType("EpisodeOfCare");
        resourceId.setUniqueId(uniqueId);
        resourceId.setResourceId(UUID.randomUUID());
        LOG.trace("Create EpisodeOfCare resourceId:" + resourceId.getUniqueId() + "==>" + resourceId.getResourceId());
        saveResourceId(resourceId);
        return resourceId;
        */
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
        savePatientResource(fhirResourceFiler, currentParserState, new EpisodeOfCareBuilder(fhirEpisodeOfCare));

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
        savePatientResource(fhirResourceFiler, currentParserState, new EncounterBuilder(fhirEncounter));

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

    public static ProcedureBuilder createProcedureResource(ResourceId procedureResourceId, ResourceId encounterResourceId,
                                                           ResourceId patientResourceId, Procedure.ProcedureStatus status,
                                                           String procedureCode, String procedureTerm, String procedureCodeSystem, Date procedureDate,
                                                           String notes, String cdsId, String context) throws Exception {

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();

        // Turn key into Resource id
        procedureBuilder.setId(procedureResourceId.getResourceId().toString());

        // Encounter
        if (encounterResourceId != null) {
            procedureBuilder.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }

        // set patient reference
        procedureBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        if (context != null) {
            procedureBuilder.setContext(context);
        }

        if (!Strings.isNullOrEmpty(cdsId)) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsId);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
        codeableConceptBuilder.addCoding(procedureCodeSystem);
        codeableConceptBuilder.setCodingCode(procedureCode);

        if (procedureCodeSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_OPCS4)) {
            String term = TerminologyService.lookupOpcs4ProcedureName(procedureCode);
            if (!Strings.isNullOrEmpty(term)) {
                codeableConceptBuilder.setCodingDisplay(term);
                codeableConceptBuilder.setText(term);
            }

        } else if (procedureCodeSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)) {
            SnomedCode snomedCode = TerminologyService.lookupSnomedFromConceptId(procedureCode);
            if (snomedCode != null) {
                String term = snomedCode.getTerm();
                codeableConceptBuilder.setCodingDisplay(term);
                codeableConceptBuilder.setText(term);
            }

        } else {
            throw new TransformException("Unknown procedure code system " + procedureCodeSystem);
        }

        //if we've been supplied a term cell, then set that in the codeable concept text, but leave the coding with the official term
        if (!Strings.isNullOrEmpty(procedureTerm)) {
            codeableConceptBuilder.setText(procedureTerm);
        }

        // status
        procedureBuilder.setStatus(status);

        // Performed date/time
        if (procedureDate != null) {
            DateTimeType dateDt = new DateTimeType(procedureDate);
            procedureBuilder.setPerformed(dateDt);
        }

        // set notes
        if (!Strings.isNullOrEmpty(notes)) {
            procedureBuilder.addNotes(notes);
        }

        return procedureBuilder;
    }


    /*
     *
     */
    public static ResourceId resolvePatientResource(String scope, String uniqueId, CsvCurrentState currentParserState, String primaryOrgHL7OrgOID, FhirResourceFiler fhirResourceFiler, String mrn, String nhsno, HumanName name, Address fhirAddress, Enumerations.AdministrativeGender gender, Date dob, ResourceId organisationResourceId, CodeableConcept maritalStatus, Identifier identifiers[], ResourceId gp, ResourceId gpPractice, CodeableConcept ethnicGroup) throws Exception {
        if (uniqueId == null) {
            // Default format is for Barts
            if (scope.compareToIgnoreCase("H") == 0) {
                uniqueId = "PatIdTypeCode=CNN-PatIdValue=" + mrn;
            } else {
                uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + mrn;
            }
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

            //Example: Address fhirAddress = AddressHelper.createAddress(Address.AddressUse.HOME, parser.getAddress1(), parser.getAddress2(), parser.getAddress3(), parser.getAddress4(), parser.getAddress5(), parser.getPostCode());
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
            savePatientResource(fhirResourceFiler, currentParserState, new PatientBuilder(fhirPatient));
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
        String uniqueId = null;
        if (scope.compareToIgnoreCase("H") == 0) {
            uniqueId = "PatIdTypeCode=CNN-PatIdValue=" + mrn;
        } else {
            uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + mrn;
        }
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

    /*public static ResourceId getObservationResourceId(String scope, String patientId, String observationDate, String observationCode) throws Exception {
        String uniqueId = "PatientId=" + patientId + "-ObservationDate=" + observationDate + "-ObservationCode=" + observationCode;
        return getObservationResourceId(scope, uniqueId);
    }*/

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

    //orgs use standard ID mapping now, not the HL7 Receiver DB
    /*public static ResourceId getOrganisationResourceId(String scope, CsvCell orgIdCell) throws Exception {

        String uniqueId = "Organisation=" + orgIdCell.getString();
        ResourceId resourceId = getResourceId(scope, "Organization", uniqueId);

        //if we failed to find a UUID for our personnelID, generate one and return it
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(scope);
            resourceId.setResourceType("Organization");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }
        return resourceId;
    }*/

    //practitioners use standard ID mapping now, not the HL7 Receiver DB
    /*public static ResourceId getPractitionerResourceId(String scope, CsvCell personnelIdCell) throws Exception {

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
    }*/

    public static ResourceId getOrCreateSpecialtyResourceId(String scope, String specialtyId) throws Exception {
        String uniqueId = "SpecialtyId=" + specialtyId;
        ResourceId resourceId = getResourceId(scope, ResourceType.Organization.toString(), uniqueId);

        //if we failed to find a UUID, generate one and return it
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId(scope);
            resourceId.setResourceType(ResourceType.Organization.toString());
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            saveResourceId(resourceId);
        }

        return resourceId;
    }




    public static ResourceId createPatientResourceId(String scope, String primaryOrgHL7OrgOID, String mrn) throws Exception {
        String uniqueId = null;
        if (scope.compareToIgnoreCase("H") == 0) {
            uniqueId = "PatIdTypeCode=CNN-PatIdValue=" + mrn;
        } else {
            uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID + "-PatIdValue=" + mrn;
        }
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

    /*public static ResourceId getOrCreateDiagnosticReportResourceId(String scope, CsvCell observationIdCell) throws Exception {
        return getOrCreateResourceId(scope, ResourceType.DiagnosticReport, observationIdCell);
    }*/

    /*public static ResourceId getOrCreateObservationResourceId(String scope, CsvCell observationIdCell) throws Exception {
        return getOrCreateResourceId(scope, ResourceType.Observation, observationIdCell);
    }*/

    /*public static ResourceId getOrCreateProcedureResourceId(String scope, CsvCell procedureIdCell) throws Exception {
        return getOrCreateResourceId(scope, ResourceType.Procedure, procedureIdCell);
    }*/

    /*public static ResourceId getOrCreateConditionResourceId(String scope, CsvCell conditionIdCell) throws Exception {
        return getOrCreateResourceId(scope, ResourceType.Condition, conditionIdCell);
    }*/

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



    public static void deletePatientResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, ResourceBuilderBase... resourceBuilders) throws Exception {
        fhirResourceFiler.deletePatientResource(parserState, false, resourceBuilders);
    }

    /*public static void deletePatientResourceMapIds(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, ResourceBuilderBase... resourceBuilders) throws Exception {
        fhirResourceFiler.deletePatientResource(parserState, true, resourceBuilders);
    }*/

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

    /*public static void deletePatientResource(FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, String groupId, Resource... resources) throws Exception {
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
    }*/

}
