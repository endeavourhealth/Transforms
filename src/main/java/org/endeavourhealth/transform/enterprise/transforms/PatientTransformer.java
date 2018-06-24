package org.endeavourhealth.transform.enterprise.transforms;

import OpenPseudonymiser.Crypto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariDataSource;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.LibraryRepositoryHelper;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.eds.PatientSearchDalI;
import org.endeavourhealth.core.database.dal.eds.models.PatientLinkPair;
import org.endeavourhealth.core.database.dal.eds.models.PatientSearch;
import org.endeavourhealth.core.database.dal.reference.PostcodeDalI;
import org.endeavourhealth.core.database.dal.reference.models.PostcodeLookup;
import org.endeavourhealth.core.database.dal.subscriberTransform.EnterpriseAgeUpdaterlDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.EnterpriseIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.HouseholdIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.EnterpriseAge;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.xml.QueryDocument.*;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.json.ConfigParameter;
import org.endeavourhealth.transform.enterprise.json.LinkDistributorConfig;
import org.endeavourhealth.transform.enterprise.json.LinkDistributorModel;
import org.endeavourhealth.transform.enterprise.json.PatientPseudoDetails;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hibernate.internal.SessionImpl;
import org.hl7.fhir.instance.model.*;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PatientTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    private static final String PSEUDO_KEY_NHS_NUMBER = "NHSNumber";
    private static final String PSEUDO_KEY_PATIENT_NUMBER = "PatientNumber";
    private static final String PSEUDO_KEY_DATE_OF_BIRTH = "DOB";
    //private static final String PSEUDO_SALT_RESOURCE = "Endeavour Enterprise - East London.EncryptedSalt";

    private static final int BEST_ORG_SCORE = 10;

    private static Map<String, byte[]> saltCacheMap = new HashMap<>();
    private static Map<String, List<LinkDistributorConfig>> linkDistributorCacheMap = new HashMap<>();
    private static Map<String, HikariDataSource> connectionPools = new ConcurrentHashMap<>();
    private static Map<String, String> escapeCharacters = new ConcurrentHashMap<>();
    private static String insertSQL = "INSERT INTO link_distributor (source_skid, target_salt_key_name, target_skid ) " +
            " VALUES (?, ?, ?)" +
            " ON DUPLICATE KEY UPDATE " +
            " target_skid = VALUES(target_skid);";

    private static String updateSQL = "UPDATE link_distributor_populator  " +
            " SET done = 1" +
            " WHERE patient_id = ? ;";

    private static final PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();
    private static final PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();
    //private static byte[] saltBytes = null;
    //private static ResourceRepository resourceRepository = new ResourceRepository();

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractEnterpriseCsvWriter csvWriter,
                          EnterpriseTransformParams params) throws Exception {

        Patient fhirPatient = (Patient)resource;


        String discoveryPersonId = patientLinkDal.getPersonId(fhirPatient.getId());

        //when the person ID table was populated, patients who had been deleted weren't added,
        //so we'll occasionally get null for some patients. If this happens, just do what would have
        //been done originally and assign an ID
        if (Strings.isNullOrEmpty(discoveryPersonId)) {
            PatientLinkPair pair = patientLinkDal.updatePersonId(params.getServiceId(), fhirPatient);
            discoveryPersonId = pair.getNewPersonId();
        }

        EnterpriseIdDalI enterpriseIdDal = DalProvider.factoryEnterpriseIdDal(params.getEnterpriseConfigName());
        Long enterprisePersonId = enterpriseIdDal.findOrCreateEnterprisePersonId(discoveryPersonId);

        long id;
        long organizationId;
        long personId;
        int patientGenderId;
        String pseudoId = null;
        String nhsNumber = null;
        Integer ageYears = null;
        Integer ageMonths = null;
        Integer ageWeeks = null;
        Date dateOfBirth = null;
        Date dateOfDeath = null;
        String postcode = null;
        String postcodePrefix = null;
        Long householdId = null;
        String lsoaCode = null;
        String msoaCode = null;
        String ethnicCode = null;
        String wardCode = null;
        String localAuthorityCode = null;
        Long registeredPracticeId = null;
        String targetSaltKeyName = null;
        String targetSkid = null;

        id = enterpriseId.longValue();
        organizationId = params.getEnterpriseOrganisationId().longValue();
        personId = enterprisePersonId.longValue();

        //Calendar cal = Calendar.getInstance();

        dateOfBirth = fhirPatient.getBirthDate();
        /*cal.setTime(dob);
        int yearOfBirth = cal.get(Calendar.YEAR);
        model.setYearOfBirth(yearOfBirth);*/

        if (fhirPatient.hasDeceasedDateTimeType()) {
            dateOfDeath = fhirPatient.getDeceasedDateTimeType().getValue();
            /*cal.setTime(dod);
            int yearOfDeath = cal.get(Calendar.YEAR);
            model.setYearOfDeath(new Integer(yearOfDeath));*/
        } else if (fhirPatient.hasDeceased()
                && fhirPatient.getDeceased() instanceof DateType) {
            //should always be a DATE TIME type, but a bug in the CSV->FHIR transform
            //means we've got data with a DATE type too
            DateType d = (DateType)fhirPatient.getDeceased();
            dateOfDeath = d.getValue();
        }

        if (fhirPatient.hasGender()) {
            patientGenderId = fhirPatient.getGender().ordinal();

        } else {
            patientGenderId = Enumerations.AdministrativeGender.UNKNOWN.ordinal();
        }

        Address fhirAddress = AddressHelper.findHomeAddress(fhirPatient);
        if (fhirAddress != null) {
            postcode = fhirAddress.getPostalCode();
            postcodePrefix = findPostcodePrefix(postcode);

            HouseholdIdDalI householdIdDal = DalProvider.factoryHouseholdIdDal(params.getEnterpriseConfigName());
            householdId = householdIdDal.findOrCreateHouseholdId(fhirAddress);
        }

        //if we've found a postcode, then get the LSOA etc. for it
        if (!Strings.isNullOrEmpty(postcode)) {
            PostcodeDalI postcodeDal = DalProvider.factoryPostcodeDal();
            PostcodeLookup postcodeReference = postcodeDal.getPostcodeReference(postcode);
            if (postcodeReference != null) {
                lsoaCode = postcodeReference.getLsoaCode();
                msoaCode = postcodeReference.getMsoaCode();
                wardCode = postcodeReference.getWardCode();
                localAuthorityCode = postcodeReference.getLocalAuthorityCode();
                //townsendScore = postcodeReference.getTownsendScore();
            }
        }

        Extension ethnicityExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.PATIENT_ETHNICITY);
        if (ethnicityExtension != null) {
            CodeableConcept codeableConcept = (CodeableConcept)ethnicityExtension.getValue();
            ethnicCode = CodeableConceptHelper.findCodingCode(codeableConcept, EthnicCategory.ASIAN_BANGLADESHI.getSystem());
        }

        if (fhirPatient.hasCareProvider()) {

            Reference orgReference = findOrgReference(fhirPatient, params);
            if (orgReference != null) {
                //added try/catch to track down a bug in Cerner->FHIR->Enterprise
                try {
                    registeredPracticeId = super.findEnterpriseId(params, orgReference);
                } catch (Throwable t) {
                    LOG.error("Error finding enterprise ID for reference " + orgReference.getReference());
                    throw t;
                }
            }
        }

        //check if our patient demographics also should be used as the person demographics. This is typically
        //true if our patient record is at a GP practice.
        boolean shouldWritePersonRecord = shouldWritePersonRecord(fhirPatient, discoveryPersonId, params.getProtocolId());

        org.endeavourhealth.transform.enterprise.outputModels.Patient patientWriter = (org.endeavourhealth.transform.enterprise.outputModels.Patient)csvWriter;
        org.endeavourhealth.transform.enterprise.outputModels.Person personWriter = params.getOutputContainer().getPersons();
        org.endeavourhealth.transform.enterprise.outputModels.LinkDistributor linkDistributorWriter = params.getOutputContainer().getLinkDistributors();

        if (patientWriter.isPseduonymised()) {

            //if pseudonymised, all non-male/non-female genders should be treated as female
            if (fhirPatient.getGender() != Enumerations.AdministrativeGender.FEMALE
                    && fhirPatient.getGender() != Enumerations.AdministrativeGender.MALE) {
                patientGenderId = Enumerations.AdministrativeGender.FEMALE.ordinal();
            }

            pseudoId = pseudonymise(fhirPatient, getEncryptedSalt(params.getEnterpriseConfigName()));

            List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(params.getEnterpriseConfigName());
            if (linkDistributorConfigs != null) {
                for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
                    targetSaltKeyName = ldConfig.getSaltKeyName();
                    targetSkid = pseudonymiseUsingConfig(fhirPatient, ldConfig);

                    linkDistributorWriter.writeUpsert(pseudoId,
                            targetSaltKeyName,
                            targetSkid);
                }
            }

            //only persist the pseudo ID if it's non-null
            if (!Strings.isNullOrEmpty(pseudoId)) {
                PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(params.getEnterpriseConfigName());
                pseudoIdDal.storePseudoId(fhirPatient.getId(), pseudoId);
            }

            EnterpriseAgeUpdaterlDalI enterpriseAgeUpdaterlDal = DalProvider.factoryEnterpriseAgeUpdaterlDal(params.getEnterpriseConfigName());
            Integer[] ageValues = enterpriseAgeUpdaterlDal.calculateAgeValues(id, dateOfBirth);
            ageYears = ageValues[EnterpriseAge.UNIT_YEARS];
            ageMonths = ageValues[EnterpriseAge.UNIT_MONTHS];
            ageWeeks = ageValues[EnterpriseAge.UNIT_WEEKS];

            patientWriter.writeUpsertPseudonymised(id,
                    organizationId,
                    personId,
                    patientGenderId,
                    pseudoId,
                    ageYears,
                    ageMonths,
                    ageWeeks,
                    dateOfDeath,
                    postcodePrefix,
                    householdId,
                    lsoaCode,
                    msoaCode,
                    ethnicCode,
                    wardCode,
                    localAuthorityCode,
                    registeredPracticeId);

            //if our patient record is the one that should define the person record, then write that too
            if (shouldWritePersonRecord) {
                personWriter.writeUpsertPseudonymised(personId,
                        patientGenderId,
                        pseudoId,
                        ageYears,
                        ageMonths,
                        ageWeeks,
                        dateOfDeath,
                        postcodePrefix,
                        householdId,
                        lsoaCode,
                        msoaCode,
                        ethnicCode,
                        wardCode,
                        localAuthorityCode,
                        registeredPracticeId);
            }

        } else {

            nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient);

            patientWriter.writeUpsertIdentifiable(id,
                    organizationId,
                    personId,
                    patientGenderId,
                    nhsNumber,
                    dateOfBirth,
                    dateOfDeath,
                    postcode,
                    householdId,
                    lsoaCode,
                    msoaCode,
                    ethnicCode,
                    wardCode,
                    localAuthorityCode,
                    registeredPracticeId);

            //if our patient record is the one that should define the person record, then write that too
            if (shouldWritePersonRecord) {
                personWriter.writeUpsertIdentifiable(personId,
                        patientGenderId,
                        nhsNumber,
                        dateOfBirth,
                        dateOfDeath,
                        postcode,
                        householdId,
                        lsoaCode,
                        msoaCode,
                        ethnicCode,
                        wardCode,
                        localAuthorityCode,
                        registeredPracticeId);
            }
        }
    }

    private Reference findOrgReference(Patient fhirPatient, EnterpriseTransformParams params) throws Exception {

        //find a direct org reference first
        for (Reference reference: fhirPatient.getCareProvider()) {
            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);

            if (comps.getResourceType() == ResourceType.Organization) {
                return reference;
            }
        }

        //if no org reference, look for a practitioner one
        for (Reference reference: fhirPatient.getCareProvider()) {
            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
            if (comps.getResourceType() == ResourceType.Practitioner) {

                Practitioner practitioner = (Practitioner)super.findResource(reference, params);
                if (practitioner == null
                        || !practitioner.hasPractitionerRole()) {
                    continue;
                }

                for (Practitioner.PractitionerPractitionerRoleComponent role: practitioner.getPractitionerRole()) {

                    //ignore any ended roles
                    if (role.hasPeriod()
                        && !PeriodHelper.isActive(role.getPeriod())) {
                        continue;
                    }

                    return role.getManagingOrganization();
                }
            }
        }

        return null;
    }

    private boolean shouldWritePersonRecord(Patient fhirPatient, String discoveryPersonId, UUID protocolId) throws Exception {

        //check if OUR patient record is an active one at a GP practice, in which case it definitely should define the person record
        String patientId = fhirPatient.getId();
        PatientSearch patientSearch = patientSearchDal.searchByPatientId(UUID.fromString(patientId));

        if (patientSearch != null //if we get null back, then we'll have deleted the patient, so just skip the ID
                && isActive(patientSearch)
                && getPatientSearchOrgScore(patientSearch) == BEST_ORG_SCORE) {
            return true;
        }

        //if we fail the above check, we need to know what other services are in our protocol, so we can see
        //which service has the most relevant data for the person record
        LibraryItem libraryItem = LibraryRepositoryHelper.getLibraryItemUsingCache(protocolId);
        Protocol protocol = libraryItem.getProtocol();
        Set<String> serviceIdsInProtocol = new HashSet<>();

        for (ServiceContract serviceContract: protocol.getServiceContract()) {
            if (serviceContract.getType().equals(ServiceContractType.SUBSCRIBER)
                && serviceContract.getActive() == ServiceContractActive.TRUE) {

                serviceIdsInProtocol.add(serviceContract.getService().getUuid());
            }
        }

        //get the other patient IDs for our person record
        List<PatientSearch> patientSearchesInProtocol = new ArrayList<>();
        patientSearchesInProtocol.add(patientSearch);

        Map<String, String> allPatientIdMap = patientLinkDal.getPatientAndServiceIdsForPerson(discoveryPersonId);
        for (String otherPatientId: allPatientIdMap.keySet()) {

            //skip the patient ID we've already retrieved
            if (otherPatientId.equals(patientId)) {
                continue;
            }

            PatientSearch otherPatientSearch = patientSearchDal.searchByPatientId(UUID.fromString(otherPatientId));

            //if we get null back, then we'll have deleted the patient, so just skip the ID
            if (otherPatientSearch == null) {
                LOG.error("Failed to get patient search record for patient ID " + otherPatientId);
                continue;
            }

            //if this patient search record isn't in our protocol, skip it
            String otherPatientSearchService = otherPatientSearch.getServiceId().toString();
            if (!serviceIdsInProtocol.contains(otherPatientSearchService)) {
                continue;
            }

            patientSearchesInProtocol.add(otherPatientSearch);
        }

        //sort the patient searches so active GP ones are first
        patientSearchesInProtocol.sort((o1, o2) -> {

            //active records always supersede inactive ones
            boolean o1Active = isActive(o1);
            boolean o2Active = isActive(o2);
            if (o1Active && !o2Active) {
                return -1;
            } else if (!o1Active && o2Active) {
                return 1;
            } else {
                //if they both have the same active state, then check the org type. We want
                //GP practices first, then hospital records, then everything else
                int o1OrgScore = getPatientSearchOrgScore(o1);
                int o2OrgScore = getPatientSearchOrgScore(o2);
                if (o1OrgScore > o2OrgScore) {
                    return -1;
                } else if (o1OrgScore < o2OrgScore) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });

        //check if the best patient search record matches our patient ID. If it does, then our patient
        //should be the one that is used to define the person table.
        PatientSearch bestPatientSearch = patientSearchesInProtocol.get(0);

        //if there are no patient search records (i.e. patient is deleted everywhere), just return false since there doesn't need to be a person record
        if (bestPatientSearch == null) {
            return false;
        }

        return bestPatientSearch.getPatientId().equals(patientId);
    }

    private static int getPatientSearchOrgScore(PatientSearch patientSearch) {
        String typeCode = patientSearch.getOrganisationTypeCode();
        if (Strings.isNullOrEmpty(typeCode)) {
            return BEST_ORG_SCORE-4;
        }

        if (typeCode.equals(OrganisationType.GP_PRACTICE.getCode())) {
            return BEST_ORG_SCORE;
        } else if (typeCode.equals(OrganisationType.NHS_TRUST.getCode())
                || typeCode.equals(OrganisationType.NHS_TRUST_SERVICE.getCode())) {
            return BEST_ORG_SCORE-1;
        } else {
            return BEST_ORG_SCORE-2;
        }
    }

    private static boolean isActive(PatientSearch patientSearch) {
        Date deducted = patientSearch.getRegistrationEnd();
        return deducted == null || deducted.after(new Date());
    }

    private static final String findPostcodePrefix(String postcode) {

        if (Strings.isNullOrEmpty(postcode)) {
            return null;
        }

        //if the postcode is already formatted with a space, use that
        int spaceIndex = postcode.indexOf(" ");
        if (spaceIndex > -1) {
            return postcode.substring(0, spaceIndex);
        }

        //if no space, then drop the last three chars off, which works
        //for older format postcodes (e.g. AN, ANN, AAN, AANN) and the newer London ones (e.g. ANA, AANA)
        int len = postcode.length();
        if (len <= 3) {
            return null;
        }

        return postcode.substring(0, len-3);
    }

    private static String pseudonymiseUsingConfig(Patient fhirPatient, LinkDistributorConfig config) throws Exception {
        TreeMap<String, String> keys = new TreeMap<>();

        List<ConfigParameter> parameters = config.getParameters();

        for (ConfigParameter param : parameters) {
            String fieldValue = null;
            if (param.getFieldName().equals("date_of_birth")) {
                if (fhirPatient.hasBirthDate()) {
                    Date d = fhirPatient.getBirthDate();
                    fieldValue = new SimpleDateFormat("dd-MM-yyyy").format(d);
                }
            }

            if (param.getFieldName().equals("nhs_number")) {
                fieldValue = IdentifierHelper.findNhsNumber(fhirPatient);
            }

            if (Strings.isNullOrEmpty(fieldValue)) {
                // we always need a non null string for the psuedo ID
                return null;
            }

            keys.put(param.getFieldLabel(), fieldValue);
        }

        return applySaltToKeys(keys, Base64.getDecoder().decode(config.getSalt()));
    }

    private static String pseudonymise(Patient fhirPatient, byte[] encryptedSalt) throws Exception {

        String dob = null;
        if (fhirPatient.hasBirthDate()) {
            Date d = fhirPatient.getBirthDate();
            dob = new SimpleDateFormat("dd-MM-yyyy").format(d);
        }

        if (Strings.isNullOrEmpty(dob)) {
            //we always need DoB for the psuedo ID
            return null;
        }

        TreeMap<String, String> keys = new TreeMap<>();
        keys.put(PSEUDO_KEY_DATE_OF_BIRTH, dob);

        String nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient);
        if (!Strings.isNullOrEmpty(nhsNumber)) {
            keys.put(PSEUDO_KEY_NHS_NUMBER, nhsNumber);

        } else {

            //if we don't have an NHS number, use the Emis patient number
            String patientNumber = null;
            if (fhirPatient.hasIdentifier()) {
                patientNumber = IdentifierHelper.findIdentifierValue(fhirPatient.getIdentifier(), FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_PATIENT_NUMBER);
            }

            if (!Strings.isNullOrEmpty(patientNumber)) {
                keys.put(PSEUDO_KEY_PATIENT_NUMBER, patientNumber);

            } else {
                //if no NHS number or patient number
                return null;
            }
        }

        return applySaltToKeys(keys, encryptedSalt);
    }

    private static String applySaltToKeys(TreeMap<String, String> keys, byte[] salt) throws Exception {
        Crypto crypto = new Crypto();
        crypto.SetEncryptedSalt(salt);
        return crypto.GetDigest(keys);
    }

    private static byte[] getEncryptedSalt(String configName) throws Exception {

        byte[] ret = saltCacheMap.get(configName);
        if (ret == null) {

            synchronized (saltCacheMap) {
                ret = saltCacheMap.get(configName);
                if (ret == null) {

                    JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
                    JsonNode saltNode = config.get("salt");
                    if (saltNode == null) {
                        throw new Exception("No 'Salt' element found in Enterprise config " + configName);
                    }
                    String base64Salt = saltNode.asText();

                    ret = Base64.getDecoder().decode(base64Salt);
                    saltCacheMap.put(configName, ret);
                }
            }
        }
        return ret;
    }

    private static List<LinkDistributorConfig> getLinkedDistributorConfig(String configName) throws Exception {

        List<LinkDistributorConfig> ret = linkDistributorCacheMap.get(configName);
        if (ret == null) {
            synchronized (linkDistributorCacheMap) {
                ret = linkDistributorCacheMap.get(configName);
                if (ret == null) {

                    ret = new ArrayList<>();

                    JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
                    JsonNode linkDistributorsNode = config.get("linkedDistributors");

                    if (linkDistributorsNode == null) {
                        return null;
                    }
                    String linkDistributors = convertJsonNodeToString(linkDistributorsNode);
                    LinkDistributorConfig[] arr = ObjectMapperPool.getInstance().readValue(linkDistributors, LinkDistributorConfig[].class);

                    for (LinkDistributorConfig l : arr) {
                        ret.add(l);
                    }

                    if (ret == null) {
                        return null;
                    }
                    linkDistributorCacheMap.put(configName, ret);
                }
            }
        }
        return ret;
    }

    private static String convertJsonNodeToString(JsonNode jsonNode) throws Exception {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Object json = mapper.readValue(jsonNode.toString(), Object.class);
            return mapper.writeValueAsString(json);
        } catch (Exception e) {
            throw new Exception("Error parsing Link Distributor Config");
        }
    }

    private static void bulkProcessLinkDistributor(List<PatientPseudoDetails> patients, String configName) throws Exception {

        LOG.info("Processing Batch of patients");
        List<LinkDistributorModel> processedPatients = new ArrayList<>();
        List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(configName);
        if (linkDistributorConfigs != null) {
            for (PatientPseudoDetails patient : patients) {
                checkPatientHasBeenSentToSubscriber(configName, patient.getPatientId());
                // obtain the source pseudo Id
                String sourceSkid = pseudonymiseFromPatientDetails(patient, configName);
                if (sourceSkid == null) {
                    continue;
                }

                for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
                    LinkDistributorModel model = new LinkDistributorModel();
                    model.setSourceSkid(sourceSkid);
                    model.setTargetSalkKeyName(ldConfig.getSaltKeyName());
                    model.setTargetSkid(pseudonymiseUsingConfigFromPatientDetails(patient, ldConfig));

                    processedPatients.add(model);
                }
            }
        }

        saveLinkDistributorData(processedPatients, configName);
        updateDoneFlag(patients);
    }

    private static boolean checkPatientHasBeenSentToSubscriber(String configName, String patientId) throws Exception {
        EnterpriseIdDalI dal = DalProvider.factoryEnterpriseIdDal(configName);
        Long enterprisePatientId = dal.findEnterpriseId(ResourceType.Patient.toString(), patientId);
        if (enterprisePatientId != null) {
            return true;
        }

        return false;
    }

    private static String pseudonymiseFromPatientDetails(PatientPseudoDetails patient, String configName) throws Exception {
        String dob = null;
        if (patient.getDateOfBirth() != null) {
            Date d = patient.getDateOfBirth();
            dob = new SimpleDateFormat("dd-MM-yyyy").format(d);
        }

        if (Strings.isNullOrEmpty(dob)) {
            //we always need DoB for the psuedo ID
            return null;
        }

        TreeMap<String, String> keys = new TreeMap<>();
        keys.put(PSEUDO_KEY_DATE_OF_BIRTH, dob);

        String nhsNumber = patient.getNhsNumber();
        if (!Strings.isNullOrEmpty(nhsNumber)) {
            keys.put(PSEUDO_KEY_NHS_NUMBER, nhsNumber);

        } else {
            return null;
        }

        return applySaltToKeys(keys, getEncryptedSalt(configName));
    }

    private static String pseudonymiseUsingConfigFromPatientDetails(PatientPseudoDetails patient, LinkDistributorConfig config) throws Exception {
        TreeMap<String, String> keys = new TreeMap<>();

        List<ConfigParameter> parameters = config.getParameters();

        for (ConfigParameter param : parameters) {
            String fieldValue = null;
            if (param.getFieldName().equals("date_of_birth")) {
                if (patient.getDateOfBirth() !=null) {
                    Date d = patient.getDateOfBirth();
                    fieldValue = new SimpleDateFormat("dd-MM-yyyy").format(d);
                }
            }

            if (param.getFieldName().equals("nhs_number")) {
                fieldValue = patient.getNhsNumber();
            }

            if (Strings.isNullOrEmpty(fieldValue)) {
                // we always need a non null string for the psuedo ID
                return null;
            }

            keys.put(param.getFieldLabel(), fieldValue);
        }

        return applySaltToKeys(keys, Base64.getDecoder().decode(config.getSalt()));
    }

    private static void saveLinkDistributorData(List<LinkDistributorModel> patients, String configName) throws Exception {
        LOG.info("Saving Now");
        JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
        String url = config.get("enterprise_url").asText();
        Connection connection = openConnection(url, config);
        connection.setAutoCommit(true);

        PreparedStatement insert = connection.prepareStatement(insertSQL);

        for (LinkDistributorModel pat : patients) {
            insert.setString(1, pat.getSourceSkid());
            insert.setString(2, pat.getTargetSalkKeyName());
            insert.setString(3, pat.getTargetSkid());

            insert.addBatch();
        }

        try {
            LOG.info("Filing");
            insert.executeBatch();
            insert.clearBatch();
            LOG.info("Done Insert");
        } catch (Exception ex) {
            LOG.error(insert.toString());
            throw ex;
        } finally {
            connection.close();
        }

    }

    private static void updateDoneFlag(List<PatientPseudoDetails> patients) throws Exception {
        EntityManager adminEntityManager = ConnectionManager.getAdminEntityManager();
        SessionImpl adminSession = (SessionImpl) adminEntityManager.getDelegate();
        Connection adminConnection = adminSession.connection();
        adminConnection.setAutoCommit(true);

        PreparedStatement update = adminConnection.prepareStatement(updateSQL);

        for (PatientPseudoDetails pat : patients) {
            update.setString(1, pat.getPatientId());
            update.addBatch();
        }

        try {
            LOG.info("Updating");
            update.executeBatch();
            update.clearBatch();
            LOG.info("Done Update");
        } catch (Exception ex) {
            LOG.error(update.toString());
            throw ex;
        } finally {
            adminEntityManager.close();
        }
    }

    private static List<PatientPseudoDetails> getNextBatchOfPatientsToProcess(Integer batchSize) throws Exception {
        EntityManager entityManager = ConnectionManager.getAdminEntityManager();
        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();
        Statement statement = connection.createStatement();

        List<PatientPseudoDetails> patients = new ArrayList<>();

        String sql = String.format("SELECT patient_id, nhs_number, date_of_birth FROM link_distributor_populator WHERE done = 0 limit %d",
                batchSize);

        try {
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                PatientPseudoDetails pat = new PatientPseudoDetails();
                pat.setPatientId(rs.getString(1));
                pat.setNhsNumber(rs.getString(2));
                pat.setDateOfBirth(rs.getDate(3));
                patients.add(pat);
            }
            rs.close();
        } catch (Throwable t) {
            LOG.error("", t);
        }
        finally {
            statement.close();
            entityManager.close();
        }

        return patients;

    }

    public static void processPatientsInBatches(String configName, Integer batchSize) throws Exception {
        LOG.info("Populating link distributor table from link_distributor_populator");

        try {

            boolean stopProcessing = false;

            List<PatientPseudoDetails> patients = new ArrayList<>();

            while (!stopProcessing) {
                patients.clear();

                LOG.info("Getting next batch of patients");
                patients = getNextBatchOfPatientsToProcess(batchSize);

                if (patients.size() < 1) {
                    stopProcessing = true;
                }

                bulkProcessLinkDistributor(patients, configName);
            }

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    private static String getKeywordEscapeChar(String url) {
        return escapeCharacters.get(url);
    }

    private static Connection openConnection(String url, JsonNode config) throws Exception {

        HikariDataSource pool = connectionPools.get(url);
        if (pool == null) {
            synchronized (connectionPools) {
                pool = connectionPools.get(url);
                if (pool == null) {

                    String driverClass = config.get("driverClass").asText();
                    String username = config.get("enterprise_username").asText();
                    String password = config.get("enterprise_password").asText();

                    //force the driver to be loaded
                    Class.forName(driverClass);

                    pool = new HikariDataSource();
                    pool.setJdbcUrl(url);
                    pool.setUsername(username);
                    pool.setPassword(password);
                    pool.setMaximumPoolSize(3);
                    pool.setMinimumIdle(1);
                    pool.setIdleTimeout(60000);
                    pool.setPoolName("EnterpriseFilerConnectionPool" + url);
                    pool.setAutoCommit(false);

                    connectionPools.put(url, pool);

                    //cache the escape string too, since getting the metadata each time is extra load
                    Connection conn = pool.getConnection();
                    String escapeStr = conn.getMetaData().getIdentifierQuoteString();
                    escapeCharacters.put(url, escapeStr);
                    conn.close();
                }
            }
        }

        return pool.getConnection();
    }

    /*private static byte[] getEncryptedSalt() throws Exception {
        if (saltBytes == null) {
            saltBytes = Resources.getResourceAsBytes(PSEUDO_SALT_RESOURCE);
        }
        return saltBytes;
    }*/

}
