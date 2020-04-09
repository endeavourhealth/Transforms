package org.endeavourhealth.transform.enterprise.transforms;

import OpenPseudonymiser.Crypto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.eds.PatientSearchDalI;
import org.endeavourhealth.core.database.dal.eds.models.PatientLinkPair;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.reference.PostcodeDalI;
import org.endeavourhealth.core.database.dal.reference.models.PostcodeLookup;
import org.endeavourhealth.core.database.dal.subscriberTransform.EnterpriseAgeUpdaterlDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPersonMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.EnterpriseAge;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.PseudoIdBuilder;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.enterprise.outputModels.PatientAddress;
import org.endeavourhealth.transform.enterprise.outputModels.PatientAddressMatch;
import org.endeavourhealth.transform.enterprise.outputModels.PatientContact;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.UPRN;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class PatientEnterpriseTransformer extends AbstractEnterpriseTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientEnterpriseTransformer.class);

    /*private static final String PSEUDO_KEY_NHS_NUMBER = "NHSNumber";
    private static final String PSEUDO_KEY_PATIENT_NUMBER = "PatientNumber";
    private static final String PSEUDO_KEY_DATE_OF_BIRTH = "DOB";*/

    //private static final int BEST_ORG_SCORE = 10;
    private static final String PREFIX_ADDRESS_ID = "-ADDR-";
    private static final String PREFIX_TELECOM_ID = "-TELECOM-";
    private static final String PREFIX_ADDRESS_MATCH_ID = "-ADDRMATCH-";

    private static Map<String, LinkDistributorConfig> mainPseudoCacheMap = new HashMap<>();
    private static Map<String, List<LinkDistributorConfig>> linkDistributorCacheMap = new HashMap<>();

    private static final PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();
    private static final PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();
    //private static byte[] saltBytes = null;
    //private static ResourceRepository resourceRepository = new ResourceRepository();

    public static String uprnToken = "";

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Patient;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    public void UPRN(Patient fhirPatient, long id, long personId, AbstractEnterpriseCsvWriter csvWriter, String configName)  throws Exception {
        if (!fhirPatient.hasAddress()) {return;}

        Iterator var2 = fhirPatient.getAddress().iterator();
        String adrec = "";

        JsonNode config = ConfigManager.getConfigurationAsJson("UPRN", "db_enterprise");
        if (config==null) {return;}

        // call the UPRN API
        JsonNode token_endpoint=config.get("token_endpoint");
        JsonNode clientid = config.get("clientid");
        JsonNode password = config.get("password");
        JsonNode username = config.get("username");

        JsonNode uprn_endpoint = config.get("uprn_endpoint");

        JsonNode zs = config.get("subscribers");
        Integer ok = UPRN.Activated(zs, configName);
        if (ok.equals(0)) {
            LOG.debug("subscriber "+configName+" not activated, exiting");
            return;
        }

        uprnToken = UPRN.getUPRNToken(password.asText(), username.asText(), clientid.asText(), LOG, token_endpoint.asText());

        org.endeavourhealth.transform.enterprise.outputModels.PatientAddressMatch uprnWriter = (org.endeavourhealth.transform.enterprise.outputModels.PatientAddressMatch)csvWriter;

        Integer stati = 0;

        while (true) {
            Address address;
            if (!var2.hasNext()) {
                break;
            }
            address = (Address) var2.next();
            adrec = AddressHelper.generateDisplayText(address);
            LOG.debug(adrec);

            boolean isActive = PeriodHelper.isActive(address.getPeriod());
            stati=0;
            if (isActive) {stati=1;}

            String ids = Long.toString(id)+"`"+Long.toString(personId)+"`"+configName;
            String csv = UPRN.getAdrec(adrec, uprnToken, uprn_endpoint.asText(), ids);

            // token time out?
            if (csv.isEmpty()) {
                UPRN.uprnToken = "";
                uprnToken = UPRN.getUPRNToken(password.asText(), username.asText(), clientid.asText(), LOG, token_endpoint.asText());
                csv = UPRN.getAdrec(adrec, uprnToken, uprn_endpoint.asText(), ids);
                if (csv.isEmpty()) {
                    LOG.debug("Unable to get address from UPRN API");
                    return;
                }
            }

            String[] ss = csv.split("\\~", -1);
            String sLat = ss[14];
            String sLong = ss[15];
            String sX = ss[17];
            String sY = ss[18];
            String sClass = ss[19];
            String sQualifier = ss[7];

            String sUprn = ss[20];
            Long luprn = new Long(0);

            if (sUprn.isEmpty()) {
                LOG.debug("UPRN = 0");
                return;
            }

            luprn = new Long(sUprn);

            BigDecimal lat = new BigDecimal(0);
            if (!sLat.isEmpty()) {
                lat = new BigDecimal(sLat);
            }

            BigDecimal longitude = new BigDecimal(0);
            if (!sLong.isEmpty()) {
                longitude = new BigDecimal(sLong);
            }

            BigDecimal x = new BigDecimal(0);
            if (!sX.isEmpty()) {
                x = new BigDecimal(sX);
            }

            BigDecimal y = new BigDecimal(0);
            if (!sY.isEmpty()) {
                y = new BigDecimal(sY);
            }

            Date match_date = new Date();

            if (uprnWriter.isPseduonymised()) {

                LOG.debug("Pseduonymise!");

                config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
                JsonNode pseudoNode = config.get("pseudonymisation");
                JsonNode saltNode = pseudoNode.get("salt");
                String base64Salt = saltNode.asText();
                byte[] saltBytes = Base64.getDecoder().decode(base64Salt);

                String pseudoUprn = null;
                TreeMap<String, String> keys = new TreeMap<>();
                keys.put("UPRN", "" + sUprn);

                Crypto crypto = new Crypto();
                crypto.SetEncryptedSalt(saltBytes);
                pseudoUprn = crypto.GetDigest(keys);

                uprnWriter.writeUpsertPseudonymised(id,
                        personId,
                        pseudoUprn,
                        stati,
                        sClass,
                        sQualifier,
                        ss[6],
                        match_date,
                        "",
                        ""
                        );
            } else {
            uprnWriter.writeUpsert(id,
                    personId,
                    luprn,
                    stati, // status
                    sClass,
                    lat,
                    longitude,
                    x,
                    y,
                    sQualifier,
                    ss[6],
                    match_date,
                    ss[1], // number
                    ss[4], // street
                    ss[0], // locality
                    ss[5], // town
                    ss[3], // postcode
                    ss[2], // org
                    ss[11], // match post
                    ss[12], // match street
                    ss[10], // match number
                    ss[8], // match building
                    ss[9], // match flat
                    "", // alg_version
                    ""); // epoc
            }
        }
    }

    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {


        Patient fhirPatient = (Patient)resourceWrapper.getResource();

        List<ResourceWrapper> fullHistory = getFullHistory(resourceWrapper);

        //work out if something has changed that means we'll need to process the full patient record
        Patient previousVersion = findPreviousVersion(resourceWrapper, params);
        if (previousVersion != null) {
            processChangesFromPreviousVersion(params.getServiceId(), fhirPatient, previousVersion, params);
        }

        //check if the patient is deleted, is confidential, has no NHS number etc.
        if (!params.shouldPatientBePresentInSubscriber(fhirPatient)) {
            csvWriter.writeDelete(enterpriseId.longValue());
            return;
        }

        String discoveryPersonId = patientLinkDal.getPersonId(fhirPatient.getId());

        //when the person ID table was populated, patients who had been deleted weren't added,
        //so we'll occasionally get null for some patients. If this happens, just do what would have
        //been done originally and assign an ID
        if (Strings.isNullOrEmpty(discoveryPersonId)) {
            PatientLinkPair pair = patientLinkDal.updatePersonId(params.getServiceId(), fhirPatient);
            discoveryPersonId = pair.getNewPersonId();
        }

        SubscriberPersonMappingDalI personMappingDal = DalProvider.factorySubscriberPersonMappingDal(params.getEnterpriseConfigName());
        Long enterprisePersonId = personMappingDal.findOrCreateEnterprisePersonId(discoveryPersonId);

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
        String lsoaCode = null;
        String msoaCode = null;
        String ethnicCode = null;
        String wardCode = null;
        String localAuthorityCode = null;
        Long registeredPracticeId = null;
        String targetSaltKeyName = null;
        String targetSkid = null;
        String title = null;
        String firstNames = null;
        String lastNames = null;
        Long currentAddressId = null;

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

            /*HouseholdIdDalI householdIdDal = DalProvider.factoryHouseholdIdDal(params.getSubscriberConfigName());
            householdId = householdIdDal.findOrCreateHouseholdId(fhirAddress);*/
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
                    registeredPracticeId = transformOnDemandAndMapId(orgReference, params);
                } catch (Throwable t) {
                    LOG.error("Error finding enterprise ID for reference " + orgReference.getReference());
                    throw t;
                }
            }
        }

        org.endeavourhealth.transform.enterprise.outputModels.Patient patientWriter = (org.endeavourhealth.transform.enterprise.outputModels.Patient)csvWriter;
        org.endeavourhealth.transform.enterprise.outputModels.LinkDistributor linkDistributorWriter = params.getOutputContainer().getLinkDistributors();

        if (!params.isPseudonymised()) {
            title = NameHelper.findPrefix(fhirPatient);
            firstNames = NameHelper.findForenames(fhirPatient);
            lastNames = NameHelper.findSurname(fhirPatient);
        }

        //TODO: remove this check for go live to introduce Compass v1 upgrade tables population
        if (!TransformConfig.instance().isLive()) {
            currentAddressId = transformAddresses(enterpriseId, personId, fhirPatient, fullHistory, resourceWrapper, params);
            transformTelecoms(enterpriseId, personId, fhirPatient, fullHistory, resourceWrapper, params);
        }

        if (patientWriter.isPseduonymised()) {

            //if pseudonymised, all non-male/non-female genders should be treated as female
            if (fhirPatient.getGender() != Enumerations.AdministrativeGender.FEMALE
                    && fhirPatient.getGender() != Enumerations.AdministrativeGender.MALE) {
                patientGenderId = Enumerations.AdministrativeGender.FEMALE.ordinal();
            }

            LinkDistributorConfig mainPseudoSalt = getMainSaltConfig(params.getEnterpriseConfigName());
            pseudoId = pseudonymiseUsingConfig(params, fhirPatient, id, mainPseudoSalt, true);

            if (pseudoId != null) {

                //generate any other pseudo mappings - the table uses the main pseudo ID as the source key, so this
                //can only be done if we've successfully generated a main pseudo ID
                List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(params.getEnterpriseConfigName());
                for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
                    targetSaltKeyName = ldConfig.getSaltKeyName();
                    targetSkid = pseudonymiseUsingConfig(params, fhirPatient, id, ldConfig, false);


                    linkDistributorWriter.writeUpsert(pseudoId,
                            targetSaltKeyName,
                            targetSkid);
                }
            }

            EnterpriseAgeUpdaterlDalI enterpriseAgeUpdaterlDal = DalProvider.factoryEnterpriseAgeUpdaterlDal(params.getEnterpriseConfigName());
            Integer[] ageValues = enterpriseAgeUpdaterlDal.calculateAgeValuesAndUpdateTable(id, dateOfBirth, dateOfDeath);
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
                    lsoaCode,
                    msoaCode,
                    ethnicCode,
                    wardCode,
                    localAuthorityCode,
                    registeredPracticeId,
                    title,
                    firstNames,
                    lastNames,
                    currentAddressId);

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
                    lsoaCode,
                    msoaCode,
                    ethnicCode,
                    wardCode,
                    localAuthorityCode,
                    registeredPracticeId,
                    title,
                    firstNames,
                    lastNames,
                    currentAddressId);
        }

        PatientAddressMatch uprnwriter = params.getOutputContainer().findCsvWriter(PatientAddressMatch.class);
        UPRN(fhirPatient, id, personId, uprnwriter, params.getEnterpriseConfigName());
    }


    private Patient findPreviousVersion(ResourceWrapper resourceWrapper, EnterpriseTransformHelper params) throws Exception {
        UUID serviceId = params.getServiceId();
        String resourceType = resourceWrapper.getResourceType();
        UUID resourceId = resourceWrapper.getResourceId();
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> history = resourceDal.getResourceHistory(serviceId, resourceType, resourceId);

        //history is most-recent first
        if (history.size() > 1) {
            ResourceWrapper wrapper = history.get(1); //want the second one
            return (Patient)wrapper.getResource();
        } else {
            return null;
        }
    }


    private Reference findOrgReference(Patient fhirPatient, EnterpriseTransformHelper params) throws Exception {

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

                ResourceWrapper wrapper = params.findOrRetrieveResource(reference);
                if (wrapper == null) {
                    continue;
                }
                Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
                if (!practitioner.hasPractitionerRole()) {
                    continue;
                }

                for (Practitioner.PractitionerPractitionerRoleComponent role: practitioner.getPractitionerRole()) {

                    //ignore any ended roles
                    if (role.hasPeriod()
                        && !PeriodHelper.isActive(role.getPeriod())) {
                        continue;
                    }

                    if (role.hasManagingOrganization()) {
                        return role.getManagingOrganization();
                    }
                }
            }
        }

        return null;
    }

    /**
     * if a patient record becomes confidential, then we need to delete all data previously sent, and conversely
     * if a patient record becomes non-confidential, then we need to send all data again
     */
    private void processChangesFromPreviousVersion(UUID serviceId, Patient current, Patient previous, EnterpriseTransformHelper params) throws  Exception {

        //if the present status has changed then we need to either bulk-add or bulk-delete all data for the patient
        //and if the NHS number has changed, the person ID on each table will need updating
        if (hasPresentStateChanged(params, current, previous)
                || hasNhsNumberChanged(current, previous)) {

            //retrieve all resources and add them to the current transform. This will ensure they then get transformed
            //back in FhirToEnterpriseCsvTransformer. Each individual transform will know if the patient is confidential
            //or not, which will result in either a delete or insert being sent
            UUID patientUuid = UUID.fromString(previous.getId()); //current may be null, so get ID from previous
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> allPatientResources = resourceDal.getResourcesByPatient(serviceId, patientUuid);
            for (ResourceWrapper wrapper: allPatientResources) {
                params.addResourceToTransform(wrapper);
            }
        }
    }

    private boolean hasPresentStateChanged(EnterpriseTransformHelper params, Patient current, Patient previous) {

        boolean nowShouldBePresent = params.shouldPatientBePresentInSubscriber(current);
        boolean previousShouldBePresent = params.shouldPatientBePresentInSubscriber(previous);

        return nowShouldBePresent != previousShouldBePresent;
    }

    private boolean hasNhsNumberChanged(Patient current, Patient previous) {

        String nowNhsNumber = null;
        if (current != null) {
            nowNhsNumber = IdentifierHelper.findNhsNumber(current);
        }

        String previousNhsNumber = null;
        if (previous != null) {
            previousNhsNumber = IdentifierHelper.findNhsNumber(previous);
        }

        if (nowNhsNumber == null && previousNhsNumber == null) {
            //if both null, no change
            return false;
        } else if (nowNhsNumber == null || previousNhsNumber == null) {
            //if only one null, change found
            return true;
        } else {
            //if neither null, compare values
            return !nowNhsNumber.equals(previousNhsNumber);
        }
    }

    /*private boolean shouldWritePersonRecord(Patient fhirPatient, String discoveryPersonId, UUID protocolId) throws Exception {

        //find all service IDs publishing into the protocol
        LibraryItem libraryItem = LibraryRepositoryHelper.getLibraryItemUsingCache(protocolId);
        Protocol protocol = libraryItem.getProtocol();
        Set<String> serviceIdsInProtocol = new HashSet<>();

        for (ServiceContract serviceContract: protocol.getServiceContract()) {
            if (serviceContract.getType().equals(ServiceContractType.PUBLISHER)
                    && serviceContract.getActive() == ServiceContractActive.TRUE) {

                serviceIdsInProtocol.add(serviceContract.getService().getUuid());
            }
        }

        //find all patient IDs that match to our person and are from publishing services
        List<UUID> possiblePatients = new ArrayList<>();

        Map<String, String> allPatientIdMap = patientLinkDal.getPatientAndServiceIdsForPerson(discoveryPersonId);
        for (String otherPatientId: allPatientIdMap.keySet()) {

            //if this patient search record isn't in our protocol, skip it
            String serviceId = allPatientIdMap.get(otherPatientId);
            if (!serviceIdsInProtocol.contains(serviceId)) {
                //LOG.trace("Patient record is not part of protocol, so skipping");
                continue;
            }

            possiblePatients.add(UUID.fromString(otherPatientId));
        }

        //if the service has been disabled in its publishing protocol, we can end up in a situation where
        //we have no potential patient IDs, which causes the below fn to fail. So always ensure there's at least one.
        if (possiblePatients.isEmpty()) {
            UUID thisPatientId = UUID.fromString(fhirPatient.getId());
            possiblePatients.add(thisPatientId);
        }

        //find the "best" patient UUI from the patient search table
        UUID bestPatientId = patientSearchDal.findBestPatientRecord(possiblePatients);
        UUID patientId = UUID.fromString(fhirPatient.getId());
        return patientId.equals(bestPatientId);
    }*/



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

    private String pseudonymiseUsingConfig(EnterpriseTransformHelper params, Patient fhirPatient, long enterprisePatientId, LinkDistributorConfig config, boolean mainPseudoId) throws Exception {

        String pseudoId = PseudoIdBuilder.generatePsuedoIdFromConfig(params.getEnterpriseConfigName(), config, fhirPatient);

        //save the mapping to the new-style table
        if (pseudoId != null) {
            PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(params.getEnterpriseConfigName());
            pseudoIdDal.saveSubscriberPseudoId(UUID.fromString(fhirPatient.getId()), enterprisePatientId, config.getSaltKeyName(), pseudoId);

            //the frailty API still uses the old pseudo ID map table to find pseudo ID from NHS number, so continue to populate that
            if (mainPseudoId) {
                pseudoIdDal.storePseudoIdOldWay(fhirPatient.getId(), pseudoId);
            }
        }

        return pseudoId;
    }

    /*public static String pseudonymiseUsingConfig(Patient fhirPatient, LinkDistributorConfig config) throws Exception {
        TreeMap<String, String> keys = new TreeMap<>();

        List<ConfigParameter> parameters = config.getParameters();

        for (ConfigParameter param : parameters) {

            String fieldName = param.getFieldName();
            String fieldFormat = param.getFormat();
            String fieldValue = null;

            if (fieldName.equals("date_of_birth")) {
                if (fhirPatient.hasBirthDate()) {
                    Date d = fhirPatient.getBirthDate();
                    fieldValue = formatPseudoDate(d, fieldFormat);
                }
            } else if (fieldName.equals("nhs_number")) {

                String nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient); //this will be in nnnnnnnnnn format
                if (!Strings.isNullOrEmpty(nhsNumber)) {
                    fieldValue = formatPseudoNhsNumber(nhsNumber, fieldFormat);
                }

            } else {
                throw new Exception("Unsupported field name [" + fieldName + "]");
            }

            if (Strings.isNullOrEmpty(fieldValue)) {
                // we always need a non null string for the psuedo ID
                continue;
            }

            //if this element is mandatory, then fail if our field is empty
            Boolean mandatory = param.getMandatory();
            if (mandatory != null
                    && mandatory.booleanValue()
                    && Strings.isNullOrEmpty(fieldValue)) {
                return null;
            }

            String fieldLabel = param.getFieldLabel();
            keys.put(fieldLabel, fieldValue);
        }

        //if not keys, then we can't generate a pseudo ID
        if (keys.isEmpty()) {
            return null;
        }

        return applySaltToKeys(keys, Base64.getDecoder().decode(config.getSalt()));
    }

    private static String applySaltToKeys(TreeMap<String, String> keys, byte[] salt) throws Exception {
        Crypto crypto = new Crypto();
        crypto.SetEncryptedSalt(salt);
        return crypto.GetDigest(keys);
    }

    private static String formatPseudoNhsNumber(String nhsNumber, String fieldFormat) throws Exception {

        //if no explicit format provided, assume one
        if (Strings.isNullOrEmpty(fieldFormat)) {
            fieldFormat = "nnnnnnnnnn";
        }

        StringBuilder sb = new StringBuilder();

        int pos = 0;
        char[] chars = nhsNumber.toCharArray();

        char[] formatChars = fieldFormat.toCharArray();
        for (int i=0; i<formatChars.length; i++) {
            char formatChar = formatChars[i];
            if (formatChar == 'n') {
                if (pos < chars.length) {
                    char c = chars[pos];
                    sb.append(c);
                    pos ++;
                }

            } else if (Character.isAlphabetic(formatChar)) {
                throw new Exception("Unsupported character " + formatChar + " in NHS number format [" + fieldFormat + "]");

            } else {
                sb.append(formatChar);
            }
        }

        return sb.toString();
    }

    private static String formatPseudoDate(Date d, String fieldFormat) {

        //if no explicit format provided, assume one
        if (Strings.isNullOrEmpty(fieldFormat)) {
            fieldFormat = "dd-MM-yyyy";
        }

        return new SimpleDateFormat(fieldFormat).format(d);
    }*/


    private LinkDistributorConfig getMainSaltConfig(String configName) throws Exception {

        LinkDistributorConfig ret = mainPseudoCacheMap.get(configName);
        if (ret == null) {
            JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
            JsonNode saltNode = config.get("pseudonymisation");

            String json = convertJsonNodeToString(saltNode);
            ret = ObjectMapperPool.getInstance().readValue(json, LinkDistributorConfig.class);

            mainPseudoCacheMap.put(configName, ret);
        }
        return ret;
    }



    private static List<LinkDistributorConfig> getLinkedDistributorConfig(String configName) throws Exception {

        List<LinkDistributorConfig> ret = linkDistributorCacheMap.get(configName);
        if (ret == null) {

            ret = new ArrayList<>();

            JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
            JsonNode linkDistributorsNode = config.get("linkedDistributors");

            if (linkDistributorsNode != null) {
                String linkDistributors = convertJsonNodeToString(linkDistributorsNode);
                LinkDistributorConfig[] arr = ObjectMapperPool.getInstance().readValue(linkDistributors, LinkDistributorConfig[].class);

                for (LinkDistributorConfig l : arr) {
                    ret.add(l);
                }
            }

            linkDistributorCacheMap.put(configName, ret);
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



    /*private static byte[] getEncryptedSalt() throws Exception {
        if (saltBytes == null) {
            saltBytes = Resources.getResourceAsBytes(PSEUDO_SALT_RESOURCE);
        }
        return saltBytes;
    }*/

    private Long transformAddresses(long subscriberPatientId, long subscriberPersonId, Patient currentPatient,
                                    List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper,
                                    EnterpriseTransformHelper params) throws Exception {

        PatientAddress writer = params.getOutputContainer().getPatientAddresses();

        Long currentAddressId = 0l;

        //update all addresses, delete any extra
        if (currentPatient.hasAddress()) {

            Address currentAddress = AddressHelper.findHomeAddress(currentPatient);

            for (int i = 0; i < currentPatient.getAddress().size(); i++) {
                Address address = currentPatient.getAddress().get(i);

                String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_ADDRESS_ID + i;
                SubscriberId subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PATIENT_ADDRESS, sourceId);
                params.setSubscriberIdTransformed(resourceWrapper, subTableId);

                //if this address is our current home one, then assign the ID
                if (address == currentAddress) {
                    currentAddressId = new Long(subTableId.getSubscriberId());
                }

                long organisationId = params.getEnterpriseOrganisationId();
                String addressLine1 = null;
                String addressLine2 = null;
                String addressLine3 = null;
                String addressLine4 = null;
                String city = null;
                String postcode = null;
                Integer useConceptId = null;
                Date startDate = null;
                Date endDate = null;
                String lsoa2001 = null;
                String lsoa2011 = null;
                String msoa2001 = null;
                String msoa2011 = null;
                String ward = null;
                String localAuthority = null;

                if (!params.isPseudonymised()) {
                    addressLine1 = getAddressLine(address, 0);
                    addressLine2 = getAddressLine(address, 1);
                    addressLine3 = getAddressLine(address, 2);
                    addressLine4 = getAddressLine(address, 3);
                    city = address.getCity();
                    postcode = address.getPostalCode();

                } else {
                    //if pseudonymised, just carry over the first half of the postcode
                    postcode = findPostcodePrefix(address.getPostalCode());
                }

                Address.AddressUse use = address.getUse();

                if (address.hasUse()) {
                    useConceptId = IMHelper.getIMConcept(params, currentPatient, IMConstant.FHIR_ADDRESS_USE, use.toCode(), use.getDisplay());
                }

                if (address.hasPeriod()) {
                    startDate = address.getPeriod().getStart();
                    endDate = address.getPeriod().getEnd();
                }

                PostcodeDalI postcodeDal = DalProvider.factoryPostcodeDal();
                PostcodeLookup postcodeReference = postcodeDal.getPostcodeReference(address.getPostalCode());
                if (postcodeReference != null) {
                    lsoa2001 = postcodeReference.getLsoa2001Code();
                    lsoa2011 = postcodeReference.getLsoa2011Code();
                    msoa2001 = postcodeReference.getMsoa2001Code();
                    msoa2011 = postcodeReference.getMsoa2011Code();
                    ward = postcodeReference.getWardCode();
                    localAuthority = postcodeReference.getLocalAuthorityCode();
                }

                writer.writeUpsert(subTableId,
                        organisationId,
                        subscriberPatientId,
                        subscriberPersonId,
                        addressLine1,
                        addressLine2,
                        addressLine3,
                        addressLine4,
                        city,
                        postcode,
                        useConceptId,
                        startDate,
                        endDate,
                        lsoa2001,
                        lsoa2011,
                        msoa2001,
                        msoa2011,
                        ward,
                        localAuthority);

                // get the address details again for UPRN (might be Pseudonymised)
                addressLine1 = getAddressLine(address, 0);
                addressLine2 = getAddressLine(address, 1);
                addressLine3 = getAddressLine(address, 2);
                addressLine4 = getAddressLine(address, 3);
                city = address.getCity();
                postcode = address.getPostalCode();

                //UPRN(params,currentPatient,i,resourceWrapper,subTableId,addressLine1,addressLine2,addressLine3,addressLine4,city,postcode,currentAddressId);
            }
        }

        //and make sure to delete any that we previously sent over that are no longer present
        //i.e. if we previously had five addresses and now only have three, we need to delete four and five
        int maxsAddresses = getMaxNumberOfAddresses(fullHistory);

        int start = 0;
        if (currentPatient.hasAddress()) {
            start = currentPatient.getAddress().size();
        }

        for (int i=start; i<maxsAddresses; i++) {

            String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_ADDRESS_ID + i;
            SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PATIENT_ADDRESS, sourceId);
            if (subTableId != null) {
                params.setSubscriberIdTransformed(resourceWrapper, subTableId);

                writer.writeDelete(subTableId.getSubscriberId());
            }
        }

        return currentAddressId;
    }

    private static String getAddressLine(Address address, int num) {
        if (num >= address.getLine().size()) {
            return null;
        } else {
            StringType st = address.getLine().get(num);
            return st.toString();
        }
    }

    private static int getMaxNumberOfAddresses(List<ResourceWrapper> history) throws Exception {
        int max = 0;
        for (ResourceWrapper wrapper: history) {
            if (!wrapper.isDeleted()) {
                Patient patient = (Patient) wrapper.getResource();
                if (patient.hasAddress()) {
                    max = Math.max(max, patient.getAddress().size());
                }
            }
        }
        return max;
    }

    private static List<ResourceWrapper> getFullHistory(ResourceWrapper resourceWrapper) throws Exception {
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        UUID serviceId = resourceWrapper.getServiceId();
        String resourceType = resourceWrapper.getResourceType();
        UUID resourceId = resourceWrapper.getResourceId();
        return resourceDal.getResourceHistory(serviceId, resourceType, resourceId);
    }

    /*
    private void UPRN(EnterpriseTransformHelper params, Patient currentPatient, int i, ResourceWrapper resourceWrapper,
                      SubscriberId subTableId, String addressLine1, String addressLine2, String addressLine3, String addressLine4,
                      String city, String postcode, Long currentAddressId) throws Exception {

        JsonNode config = ConfigManager.getConfigurationAsJson("UPRN", "db_subscriber");
        if (config==null) {return;}

        //JsonNode enabled = config.get("enabled");
        //if (enabled.asText().equals("0")) {return;}

        String configName = params.getEnterpriseConfigName();

        String uprn_sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_ADDRESS_MATCH_ID + i;
        SubscriberId uprn_subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PATIENT_ADDRESS_MATCH, uprn_sourceId); // was sourceId

        params.setSubscriberIdTransformed(resourceWrapper, uprn_subTableId);

        // call the UPRN API
        JsonNode token_endpoint=config.get("token_endpoint");
        JsonNode clientid = config.get("clientid");
        JsonNode password = config.get("password");
        JsonNode username = config.get("username");
        JsonNode uprn_endpoint = config.get("uprn_endpoint");

        JsonNode zs = config.get("subscribers");
        Integer ok = UPRN.Activated(zs, configName);
        if (ok.equals(0)) {
            LOG.debug("subscriber "+configName+" not activated, exiting");
            return;
        }

        uprnToken = UPRN.getUPRNToken(password.asText(), username.asText(), clientid.asText(), LOG, token_endpoint.asText());

        if (addressLine1==null) {addressLine1="";}
        if (addressLine2==null) {addressLine2="";}
        if (addressLine3==null) {addressLine3="";}
        if (addressLine4==null) {addressLine4="";}
        if (city==null) {city="";}
        if (postcode==null) {postcode="";}

        String adrec = addressLine1+","+addressLine2+","+addressLine3+","+addressLine4+","+city+","+postcode;

        // debug
        // adrec="201,Darkes Lane,Potters Bar,EN6 1BX";

        String ids = Long.toString(subTableId.getSubscriberId())+"`"+configName;
        String csv = UPRN.getAdrec(adrec, uprnToken, uprn_endpoint.asText(), ids);
        // token time out?
        if (csv.isEmpty()) {
            UPRN.uprnToken="";
            // get another token
            uprnToken = UPRN.getUPRNToken(password.asText(), username.asText(), clientid.asText(), LOG, token_endpoint.asText());
            csv = UPRN.getAdrec(adrec, uprnToken, uprn_endpoint.asText(), ids);
            if (csv.isEmpty()) {
                LOG.debug("Unable to get address from UPRN API");
                return;
            }
        }

        PatientAddressMatch uprnwriter = params.getOutputContainer().getPatientAddressMatch();

        Date match_date = new Date();

        String[] ss = csv.split("\\~",-1);

        // 0=locality, 1=number, 2=org, 3=postcode, 4=street, 5=town, 6=alg, 7=qual
        // 8=matpatbuild, 9=matpatflat, 10=matpatnumber, 11=matpatpostcode, 12=matpatstreet
        // 13=quality, 14=latitude, 15=longitude, 16=point
        // 17=X, 18=Y, 19=class, 20=uprn

        String sLat=ss[14]; String sLong = ss[15]; String sX=ss[17]; String sY=ss[18]; String sClass = ss[19]; String sQualifier = ss[7];

        String sUprn = ss[20];

        if (sUprn.isEmpty()) {
            LOG.debug("UPRN = 0");
            return;
        }

        BigDecimal lat = new BigDecimal(0);
        if (!sLat.isEmpty()) {lat=new BigDecimal(sLat);}

        BigDecimal longitude = new BigDecimal(0);
        if (!sLong.isEmpty()) {longitude=new BigDecimal(sLong);}

        BigDecimal x = new BigDecimal(0);
        if (!sX.isEmpty()) {x = new BigDecimal(sX);}

        BigDecimal y = new BigDecimal(0);
        if (!sY.isEmpty()) {y = new BigDecimal(sY);}

        Integer stati = 0;
        if (currentAddressId!=null) {stati=1;}

        String znumber = ss[1]; String zstreet = ss[4]; String zlocality = ss[0]; String ztown = ss[5]; String zpostcode = ss[3]; String zorg = ss[2];
        String match_post = ss[11]; String match_street = ss[12]; String match_number = ss[10]; String match_building = ss[8];
        String match_flat = ss[9];

        // null fields because isPseudonymised
        if (params.isPseudonymised()) {
            LOG.debug("Pseduonymise!");

            config = ConfigManager.getConfigurationAsJson(params.getEnterpriseConfigName(), "db_subscriber");

            JsonNode s = config.get("pseudo_salts");
            ArrayNode arrayNode = (ArrayNode) s;

            if (s == null) {throw new Exception("Unable to find UPRN salt");}

            JsonNode arrayElement = arrayNode.get(0);
            String base64Salt = arrayElement.get("salt").asText();

            LOG.debug(base64Salt);

            byte[] saltBytes = Base64.getDecoder().decode(base64Salt);

            String pseudoUprn = null;
            TreeMap<String, String> keys = new TreeMap<>();
            keys.put("UPRN", "" + sUprn);

            Crypto crypto = new Crypto();
            crypto.SetEncryptedSalt(saltBytes);
            pseudoUprn = crypto.GetDigest(keys);
            sUprn = pseudoUprn;
            // nullify fields
            znumber=null; zstreet=null; zlocality=null; ztown=null; zpostcode=null; zorg=null;
            match_post=null; match_street=null; match_number=null; match_building=null; match_flat=null;
            lat = null; longitude=null; x=null; y=null;
        }

        uprnwriter.writeUpsert(//uprn_subTableId,
                //subTableId.getSubscriberId(),
                subTableId.getSubscriberId(),
                sUprn,
                stati, // status
                sClass,
                lat,
                longitude,
                x,
                y,
                sQualifier,
                ss[6], // algorithm
                match_date, // match_date
                znumber, // number [1]
                zstreet, // street [4]
                zlocality, // locality [0]
                ztown, // town [5]
                zpostcode, // postcode [3]
                zorg, // org [2]
                match_post, // match post [11]
                match_street, // match street [12]
                match_number, // match number [10]
                match_building, // match building [8]
                match_flat, // match flat [9]
                "", // alg version ** TO DO
                ""); // epoc ** TO DO
    }*/

    private void transformTelecoms(long subscriberPatientId, long subscriberPersonId, Patient currentPatient,
                                   List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper, EnterpriseTransformHelper params) throws Exception {

        PatientContact writer = params.getOutputContainer().getPatientContact();

        //update all addresses, delete any extra
        if (currentPatient.hasTelecom()) {
            for (int i = 0; i < currentPatient.getTelecom().size(); i++) {
                ContactPoint telecom = currentPatient.getTelecom().get(i);

                String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_TELECOM_ID + i;
                SubscriberId subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PATIENT_CONTACT, sourceId);
                params.setSubscriberIdTransformed(resourceWrapper, subTableId);

                long organisationId = params.getEnterpriseOrganisationId();
                Integer useConceptId = null;
                Integer typeConceptId = null;
                Date startDate = null;
                Date endDate = null;
                String value = null;

                if (telecom.hasUse()) {
                    useConceptId = IMHelper.getIMConcept(params, currentPatient, IMConstant.FHIR_TELECOM_USE, telecom.getUse().toCode(), telecom.getValue());
                }

                if (telecom.hasSystem()) {
                    typeConceptId = IMHelper.getIMConcept(params, currentPatient, IMConstant.FHIR_TELECOM_SYSTEM, telecom.getSystem().toCode(), telecom.getValue());
                }

                if (!params.isPseudonymised()) {
                    value = telecom.getValue();
                }

                if (telecom.hasPeriod()) {
                    startDate = telecom.getPeriod().getStart();
                    endDate = telecom.getPeriod().getEnd();
                }

                writer.writeUpsert(subTableId,
                        organisationId,
                        subscriberPatientId,
                        subscriberPersonId,
                        useConceptId,
                        typeConceptId,
                        startDate,
                        endDate,
                        value);

            }
        }

        //and make sure to delete any that we previously sent over that are no longer present
        //i.e. if we previously had five addresses and now only have three, we need to delete four and five
        int maxPreviousTelecoms = getMaxNumberOfTelecoms(fullHistory);

        int start = 0;
        if (currentPatient.hasTelecom()) {
            start = currentPatient.getTelecom().size();
        }

        for (int i=start; i<maxPreviousTelecoms; i++) {

            String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_TELECOM_ID + i;
            SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PATIENT_CONTACT, sourceId);
            if (subTableId != null) {
                params.setSubscriberIdTransformed(resourceWrapper, subTableId);

                writer.writeDelete(subTableId.getSubscriberId());
            }

        }
    }

    private static int getMaxNumberOfTelecoms(List<ResourceWrapper> history) throws Exception {
        int max = 0;
        for (ResourceWrapper wrapper: history) {
            if (!wrapper.isDeleted()) {
                Patient patient = (Patient) wrapper.getResource();
                if (patient.hasTelecom()) {
                    max = Math.max(max, patient.getTelecom().size());
                }
            }
        }
        return max;
    }
}
