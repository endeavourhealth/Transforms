package org.endeavourhealth.transform.enterprise.transforms;

import OpenPseudonymiser.Crypto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
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
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.reference.PostcodeDalI;
import org.endeavourhealth.core.database.dal.reference.models.PostcodeLookup;
import org.endeavourhealth.core.database.dal.subscriberTransform.EnterpriseAgeUpdaterlDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPersonMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.EnterpriseAge;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.xml.QueryDocument.*;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.json.ConfigParameter;
import org.endeavourhealth.transform.enterprise.json.LinkDistributorConfig;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class PatientEnterpriseTransformer extends AbstractEnterpriseTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientEnterpriseTransformer.class);

    /*private static final String PSEUDO_KEY_NHS_NUMBER = "NHSNumber";
    private static final String PSEUDO_KEY_PATIENT_NUMBER = "PatientNumber";
    private static final String PSEUDO_KEY_DATE_OF_BIRTH = "DOB";*/

    private static final int BEST_ORG_SCORE = 10;

    private static Map<String, LinkDistributorConfig> mainPseudoCacheMap = new HashMap<>();
    private static Map<String, List<LinkDistributorConfig>> linkDistributorCacheMap = new HashMap<>();

    private static final PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();
    private static final PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();
    //private static byte[] saltBytes = null;
    //private static ResourceRepository resourceRepository = new ResourceRepository();

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Patient;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }


    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {


        Patient fhirPatient = (Patient)resourceWrapper.getResource();

        //work out if something has changed that means we'll need to process the full patient record
        Patient previousVersion = findPreviousVersion(resourceWrapper, params);
        if (previousVersion != null) {
            processChangesFromPreviousVersion(params.getServiceId(), fhirPatient, previousVersion, params);
        }

        //check if the patient is deleted, is confidential, has no NHS number etc.
        if (!EnterpriseTransformHelper.shouldPatientBePresentInSubscriber(fhirPatient)) {
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

        id = enterpriseId.longValue();
        organizationId = params.getEnterpriseOrganisationId().longValue();
        personId = enterprisePersonId.longValue();

        if (fhirPatient.hasCareProvider()) {
            if (fhirPatient.getCareProvider() != null) {
                List<Reference> refs = fhirPatient.getCareProvider();
                List<Resource> resources = fhirPatient.getCareProviderTarget();
                for (int m = 0; m < resources.size(); m++) {
                     if (resources.get(m).getResourceType().equals(ResourceType.Organization)) {
                        registeredPracticeId = transformOnDemandAndMapId(refs.get(m), params);
                        break; //Take first value
                    }
                }
            }
        }

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

            LinkDistributorConfig mainPseudoSalt = getMainSaltConfig(params.getEnterpriseConfigName());
            pseudoId = pseudonymiseUsingConfig(fhirPatient, mainPseudoSalt);
            //pseudoId = pseudonymise(fhirPatient, getEncryptedSalt(params.getSubscriberConfigName()));

            if (pseudoId != null) {

                //only persist the pseudo ID if it's non-null
                PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(params.getEnterpriseConfigName());
                pseudoIdDal.storePseudoIdOldWay(fhirPatient.getId(), pseudoId);

                //generate any other pseudo mappings - the table uses the main pseudo ID as the source key, so this
                //can only be done if we've successfully generated a main pseudo ID
                List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(params.getEnterpriseConfigName());
                for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
                    targetSaltKeyName = ldConfig.getSaltKeyName();
                    targetSkid = pseudonymiseUsingConfig(fhirPatient, ldConfig);

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
                        lsoaCode,
                        msoaCode,
                        ethnicCode,
                        wardCode,
                        localAuthorityCode,
                        registeredPracticeId);
            }
        }
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
        if (hasPresentStateChanged(current, previous)
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

    private boolean hasPresentStateChanged(Patient current, Patient previous) {

        boolean nowShouldBePresent = EnterpriseTransformHelper.shouldPatientBePresentInSubscriber(current);
        boolean previousShouldBePresent = EnterpriseTransformHelper.shouldPatientBePresentInSubscriber(previous);

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


    private boolean shouldWritePersonRecord(Patient fhirPatient, String discoveryPersonId, UUID protocolId) throws Exception {

        //check if OUR patient record is an active one at a GP practice, in which case it definitely should define the person record
        String patientIdStr = fhirPatient.getId();
        PatientSearch patientSearch = patientSearchDal.searchByPatientId(UUID.fromString(patientIdStr));

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
            if (serviceContract.getType().equals(ServiceContractType.PUBLISHER)
                && serviceContract.getActive() == ServiceContractActive.TRUE) {

                serviceIdsInProtocol.add(serviceContract.getService().getUuid());
            }
        }

        //LOG.trace("Checking if should write person record for patient " + patientId + " with person ID " + discoveryPersonId);

        //get the other patient IDs for our person record
        List<PatientSearch> patientSearchesInProtocol = new ArrayList<>();
        patientSearchesInProtocol.add(patientSearch);
        //LOG.trace("Added our patient search record: " + patientSearch);

        Map<String, String> allPatientIdMap = patientLinkDal.getPatientAndServiceIdsForPerson(discoveryPersonId);
        for (String otherPatientId: allPatientIdMap.keySet()) {

            //skip the patient ID we've already retrieved
            if (otherPatientId.equals(patientIdStr)) {
                continue;
            }

            PatientSearch otherPatientSearch = patientSearchDal.searchByPatientId(UUID.fromString(otherPatientId));
            //LOG.trace("Matches to other patient " + otherPatientId + " with patient search record: " + otherPatientSearch);

            //if we get null back, then we'll have deleted the patient, so just skip the ID
            if (otherPatientSearch == null) {
                //LOG.trace("Deleted patient search record, so skipping");
                continue;
            }

            //if this patient search record isn't in our protocol, skip it
            String otherPatientSearchService = otherPatientSearch.getServiceId().toString();
            if (!serviceIdsInProtocol.contains(otherPatientSearchService)) {
                //LOG.trace("Patient record is not part of protocol, so skipping");
                continue;
            }

            patientSearchesInProtocol.add(otherPatientSearch);
            //LOG.trace("Added other patient search record: " + otherPatientSearch);
        }

        //LOG.trace("Got " + patientSearchesInProtocol.size() + " patient search records: " + patientSearchesInProtocol);

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

        String patientPatientIdStr = bestPatientSearch.getPatientId().toString();
        return patientPatientIdStr.equals(patientIdStr);
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

    public static String pseudonymiseUsingConfig(Patient fhirPatient, LinkDistributorConfig config) throws Exception {
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
    }

    /*private static String pseudonymise(Patient fhirPatient, byte[] encryptedSalt) throws Exception {

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

    */

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

    private static String applySaltToKeys(TreeMap<String, String> keys, byte[] salt) throws Exception {
        Crypto crypto = new Crypto();
        crypto.SetEncryptedSalt(salt);
        return crypto.GetDigest(keys);
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

}
