package org.endeavourhealth.transform.subscriber.transforms;

import OpenPseudonymiser.Crypto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.*;
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
import org.endeavourhealth.core.database.dal.subscriberTransform.ExchangeBatchExtraResourceDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPersonMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.core.xml.QueryDocument.*;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.json.ConfigParameter;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;
import org.endeavourhealth.transform.subscriber.targetTables.PatientAddress;
import org.endeavourhealth.transform.subscriber.targetTables.PatientContact;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class PatientTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    private static final String PREFIX_PSEUDO_ID = "-PSEUDO-";
    private static final String PREFIX_ADDRESS_ID = "-ADDR-";
    private static final String PREFIX_TELECOM_ID = "-TELECOM-";

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



    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Patient patientWriter = params.getOutputContainer().getPatients();
        org.endeavourhealth.transform.subscriber.targetTables.Person personWriter = params.getOutputContainer().getPersons();
        Patient previousVersion = findPreviousVersionSent(resourceWrapper, subscriberId);

        if (resourceWrapper.isDeleted()) {

            //delete the patient
            patientWriter.writeDelete(subscriberId);

            //if we've previously sent the patient, we'll also need to delete any dependent entities
            if (previousVersion != null) {
                deletePseudoIds(previousVersion, resourceWrapper, params);
                deleteAddresses(previousVersion, resourceWrapper, params);
                deleteTelecoms(previousVersion, resourceWrapper, params);
            }

            return;
        }

        Patient fhirPatient = (Patient) FhirResourceHelper.deserialiseResouce(resourceWrapper);

        //if confidential, don't send (and remove)
        if (isConfidential(fhirPatient)) {
            //TODO - can this happen? If it does happen, does that mean we should delete ALL data for the patient?
            throw new RuntimeException("Not sure how to handle a confidential patient resource");
        }

        String discoveryPersonId = patientLinkDal.getPersonId(fhirPatient.getId());

        //when the person ID table was populated, patients who had been deleted weren't added,
        //so we'll occasionally get null for some patients. If this happens, just do what would have
        //been done originally and assign an ID
        if (Strings.isNullOrEmpty(discoveryPersonId)) {
            PatientLinkPair pair = patientLinkDal.updatePersonId(params.getServiceId(), fhirPatient);
            discoveryPersonId = pair.getNewPersonId();
        }

        SubscriberPersonMappingDalI enterpriseIdDal = DalProvider.factorySubscriberPersonMappingDal(params.getEnterpriseConfigName());
        Long enterprisePersonId = enterpriseIdDal.findOrCreateEnterprisePersonId(discoveryPersonId);

        long organizationId;
        long personId;
        String title = null;
        String firstNames = null;
        String lastName = null;
        Integer genderConceptId = null;
        String nhsNumber = null;
        Date dateOfBirth = null;
        Date dateOfDeath = null;
        Long currentAddressId = null;
        Integer ethnicCodeConceptId = null;
        Long registeredPracticeId = null;


        organizationId = params.getEnterpriseOrganisationId().longValue();
        personId = enterprisePersonId.longValue();

        if (previousVersion != null) {
            processChangesFromPreviousVersion(params.getServiceId(), fhirPatient, previousVersion,
                    resourceWrapper.getResourceId(), personId, params);
        }


        //transform our dependent objects first, as we need the address ID
        transformPseudoIds(subscriberId.getSubscriberId(), personId, fhirPatient, resourceWrapper, params);
        currentAddressId = transformAddresses(subscriberId.getSubscriberId(), personId, fhirPatient, previousVersion, resourceWrapper, params);
        transformTelecoms(subscriberId.getSubscriberId(), personId, fhirPatient, previousVersion, resourceWrapper, params);



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


        /*Address fhirAddress = AddressHelper.findHomeAddress(fhirPatient);
        if (fhirAddress != null) {
            postcode = fhirAddress.getPostalCode();
            postcodePrefix = findPostcodePrefix(postcode);
        }*/

        //if we've found a postcode, then get the LSOA etc. for it
        /*if (!Strings.isNullOrEmpty(postcode)) {
            PostcodeDalI postcodeDal = DalProvider.factoryPostcodeDal();
            PostcodeLookup postcodeReference = postcodeDal.getPostcodeReference(postcode);
            if (postcodeReference != null) {
                lsoaCode = postcodeReference.getLsoaCode();
                msoaCode = postcodeReference.getMsoaCode();
                wardCode = postcodeReference.getWardCode();
                localAuthorityCode = postcodeReference.getLocalAuthorityCode();
                //townsendScore = postcodeReference.getTownsendScore();
            }
        }*/

        Extension ethnicityExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.PATIENT_ETHNICITY);
        if (ethnicityExtension != null) {
            CodeableConcept codeableConcept = (CodeableConcept)ethnicityExtension.getValue();
            String ethnicCodeId = CodeableConceptHelper.findCodingCode(codeableConcept, FhirValueSetUri.VALUE_SET_ETHNIC_CATEGORY);
            String display = CodeableConceptHelper.findCodingDisplay(codeableConcept, FhirValueSetUri.VALUE_SET_ETHNIC_CATEGORY);

            ethnicCodeConceptId = IMHelper.getIMConcept(params, fhirPatient, IMConstant.FHIR_ETHNIC_CATEGORY, ethnicCodeId, display);
        }

        if (fhirPatient.hasCareProvider()) {

            Reference orgReference = findOrgReference(fhirPatient, params);
            if (orgReference != null) {
                //added try/catch to track down a bug in Cerner->FHIR->Enterprise
                try {
                    registeredPracticeId = super.findEnterpriseId(params, SubscriberTableId.ORGANIZATION, orgReference);
                } catch (Throwable t) {
                    LOG.error("Error finding enterprise ID for reference " + orgReference.getReference());
                    throw t;
                }
            }
        }

        if (fhirPatient.hasBirthDate()) {
            if (!params.isPseudonymised()) {
                dateOfBirth = fhirPatient.getBirthDate();

            } else {
                Calendar cal = Calendar.getInstance();
                cal.setTime(fhirPatient.getBirthDate());
                cal.set(Calendar.DAY_OF_MONTH, 1);
                dateOfBirth = cal.getTime();
            }
        }

        if (fhirPatient.hasGender()) {

            Enumerations.AdministrativeGender gender = fhirPatient.getGender();

            //if pseudonymised, all non-male/non-female genders should be treated as female
            if (params.isPseudonymised()) {
                if (gender != Enumerations.AdministrativeGender.FEMALE
                        && gender != Enumerations.AdministrativeGender.MALE) {
                    gender = Enumerations.AdministrativeGender.FEMALE;
                }
            }

            genderConceptId = IMHelper.getIMConcept(params, fhirPatient, IMConstant.FHIR_ADMINISTRATIVE_GENDER, gender.toCode(), gender.getDisplay());
        }

        //only present name fields if non-pseudonymised
        if (!params.isPseudonymised()) {
            title = NameHelper.findPrefix(fhirPatient);
            firstNames = NameHelper.findForenames(fhirPatient);
            lastName = NameHelper.findSurname(fhirPatient);
        }

        //only present name fields if non-pseudonymised
        if (!params.isPseudonymised()) {
            nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient);
        }

        patientWriter.writeUpsert(subscriberId,
                organizationId,
                personId,
                title,
                firstNames,
                lastName,
                genderConceptId,
                nhsNumber,
                dateOfBirth,
                dateOfDeath,
                currentAddressId,
                ethnicCodeConceptId,
                registeredPracticeId);


        //check if our patient demographics also should be used as the person demographics. This is typically
        //true if our patient record is at a GP practice.
        boolean shouldWritePersonRecord = shouldWritePersonRecord(fhirPatient, discoveryPersonId, params.getProtocolId());

        //if our patient record is the one that should define the person record, then write that too
        if (shouldWritePersonRecord) {
            personWriter.writeUpsert(personId,
                    organizationId,
                    title,
                    firstNames,
                    lastName,
                    genderConceptId,
                    nhsNumber,
                    dateOfBirth,
                    dateOfDeath,
                    currentAddressId,
                    ethnicCodeConceptId,
                    registeredPracticeId);

        }

    }

    private void transformTelecoms(long subscriberPatientId, long subscriberPersonId, Patient currentPatient, Patient previousPatient, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        PatientContact writer = params.getOutputContainer().getPatientContacts();

        //update all addresses, delete any extra
        if (currentPatient.hasTelecom()) {
            for (int i = 0; i < currentPatient.getTelecom().size(); i++) {
                ContactPoint telecom = currentPatient.getTelecom().get(i);

                String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_TELECOM_ID + i;
                SubscriberId subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PATIENT_CONTACT, sourceId);

                long organisationId = params.getEnterpriseOrganisationId();
                Integer useConceptId = null;
                Integer typeConceptId = null;
                Date startDate = null;
                Date endDate = null;
                String value = null;

                useConceptId = IMHelper.getIMConcept(params, currentPatient, IMConstant.FHIR_TELECOM_USE, telecom.getUse().toCode(), telecom.getValue());
                typeConceptId = IMHelper.getIMConcept(params, currentPatient, IMConstant.FHIR_TELECOM_SYSTEM, telecom.getSystem().toCode(), telecom.getValue());

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
        if (previousPatient != null
                && previousPatient.hasTelecom()) {
            int start = 0;
            if (currentPatient.hasTelecom()) {
                start = currentPatient.getTelecom().size();
            }

            for (int i=start; i<previousPatient.getTelecom().size(); i++) {

                String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_TELECOM_ID + i;
                SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PATIENT_CONTACT, sourceId);

                writer.writeDelete(subTableId);

            }
        }
    }

    private void deleteTelecoms(Patient previousVersion, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {
        if (!previousVersion.hasTelecom()) {
            return;
        }

        PatientContact writer = params.getOutputContainer().getPatientContacts();

        for (int i=0; i < previousVersion.getTelecom().size(); i++) {

            String sourceId = ReferenceHelper.createReferenceExternal(previousVersion).getReference() + PREFIX_TELECOM_ID + i;
            SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PATIENT_CONTACT, sourceId);

            writer.writeDelete(subTableId);

        }
    }

    private void deleteAddresses(Patient previousVersion, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        if (!previousVersion.hasAddress()) {
            return;
        }

        PatientAddress writer = params.getOutputContainer().getPatientAddresses();

        for (int i=0; i < previousVersion.getAddress().size(); i++) {

            String sourceId = ReferenceHelper.createReferenceExternal(previousVersion).getReference() + PREFIX_ADDRESS_ID + i;
            SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PATIENT_ADDRESS, sourceId);

            writer.writeDelete(subTableId);

        }
    }

    private Long transformAddresses(long subscriberPatientId, long subscriberPersonId, Patient currentPatient, Patient previousPatient, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        PatientAddress writer = params.getOutputContainer().getPatientAddresses();

        Long currentAddressId = null;

        //update all addresses, delete any extra
        if (currentPatient.hasAddress()) {

            Address currentAddress = AddressHelper.findHomeAddress(currentPatient);

            for (int i = 0; i < currentPatient.getAddress().size(); i++) {
                Address address = currentPatient.getAddress().get(i);

                String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_ADDRESS_ID + i;
                SubscriberId subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PATIENT_ADDRESS, sourceId);

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
                useConceptId = IMHelper.getIMConcept(params, currentPatient, IMConstant.FHIR_ADDRESS_USE, use.toCode(), use.getDisplay());

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

            }
        }

        //and make sure to delete any that we previously sent over that are no longer present
        //i.e. if we previously had five addresses and now only have three, we need to delete four and five
        if (previousPatient != null
                && previousPatient.hasAddress()) {
            int start = 0;
            if (currentPatient.hasAddress()) {
                start = currentPatient.getAddress().size();
            }

            for (int i=start; i<previousPatient.getAddress().size(); i++) {

                String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_ADDRESS_ID + i;
                SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PATIENT_ADDRESS, sourceId);

                writer.writeDelete(subTableId);

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

    private Patient findPreviousVersionSent(ResourceWrapper resourceWrapper, SubscriberId subscriberId) throws Exception {

        //if we've a null datetime, it means we've never sent for this patient or
        //the last thing we sent was a delete
        Date dtLastSent = subscriberId.getDtUpdatedPreviouslySent();
        if (dtLastSent == null) {
            return null;
        }

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> history = resourceDal.getResourceHistory(resourceWrapper.getServiceId(), resourceWrapper.getResourceType(), resourceWrapper.getResourceId());

        //history is most-recent-first
        for (ResourceWrapper wrapper: history) {
            if (wrapper.getCreatedAt().compareTo(dtLastSent) == 0) {
                return (Patient)FhirResourceHelper.deserialiseResouce(wrapper.getResourceData());
            }
        }

        throw new Exception("Failed to find previous version of " + resourceWrapper.getResourceType() + " " + resourceWrapper.getResourceId() + " for dtLastSent " + dtLastSent);
    }


    private void deletePseudoIds(Patient previousVersion, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.PseudoId pseudoIdWriter = params.getOutputContainer().getPseudoIds();

        List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(params.getEnterpriseConfigName());
        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
            String saltKeyName = ldConfig.getSaltKeyName();

            //create a unique source ID from the patient UUID plus the salt key name
            String sourceId = ReferenceHelper.createReferenceExternal(previousVersion).getReference() + PREFIX_PSEUDO_ID + saltKeyName;
            SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PSEUDO_ID, sourceId);

            pseudoIdWriter.writeDelete(subTableId);

        }

    }



    private void transformPseudoIds(long subscriberPatientId, long subscriberPersonId, Patient fhirPatient, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.PseudoId pseudoIdWriter = params.getOutputContainer().getPseudoIds();

        List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(params.getEnterpriseConfigName());
        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
            String saltKeyName = ldConfig.getSaltKeyName();
            String pseudoId = pseudonymiseUsingConfig(fhirPatient, ldConfig);

            //create a unique source ID from the patient UUID plus the salt key name
            String sourceId = ReferenceHelper.createReferenceExternal(fhirPatient).getReference() + PREFIX_PSEUDO_ID + saltKeyName;
            SubscriberId subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PSEUDO_ID, sourceId);

            if (!Strings.isNullOrEmpty(pseudoId)) {

                pseudoIdWriter.writeUpsert(subTableId,
                        subscriberPatientId,
                        saltKeyName,
                        pseudoId);

                //only persist the pseudo ID if it's non-null
                PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(params.getEnterpriseConfigName());
                pseudoIdDal.saveSubscriberPseudoId(UUID.fromString(fhirPatient.getId()), subscriberPatientId, saltKeyName, pseudoId);

            } else {

                pseudoIdWriter.writeDelete(subTableId);

            }
        }
    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.PATIENT;
    }


    private Reference findOrgReference(Patient fhirPatient, SubscriberTransformParams params) throws Exception {

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

                    if (role.hasManagingOrganization()) {
                        return role.getManagingOrganization();
                    }
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
            JsonNode linkDistributorsNode = config.get("pseudo_salts");

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

    private void processChangesFromPreviousVersion(UUID serviceId, Patient current, Patient previous, UUID resourceID,
                                                   long personId, SubscriberTransformParams params) throws  Exception {


        String currentNHSNumber = IdentifierHelper.findNhsNumber(current);
        String previousNHSNumber = IdentifierHelper.findNhsNumber(previous);

        ExchangeBatchExtraResourceDalI exchangeBatchExtraResourceDalI =
                DalProvider.factoryExchangeBatchExtraResourceDal(params.getEnterpriseConfigName());
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();

        List<ResourceWrapper> resources = resourceDal.getResourcesByPatient(serviceId, resourceID);

        Long enterprisePatientId = findEnterpriseId(params, SubscriberTableId.PATIENT,
                ReferenceHelper.createResourceReference(ResourceType.Patient, previous.getId()));
        if (params.getEnterprisePatientId() == null) {
            params.setEnterprisePatientId(enterprisePatientId);
        }

        String discoveryPersonId = patientLinkDal.getPersonId(previous.getId());
        SubscriberPersonMappingDalI subscriberPersonMappingDal =
                DalProvider.factorySubscriberPersonMappingDal(params.getEnterpriseConfigName());
        Long enterprisePersonId = subscriberPersonMappingDal.findOrCreateEnterprisePersonId(discoveryPersonId);
        if (params.getEnterprisePersonId() == null) {
            params.setEnterprisePersonId(enterprisePersonId);
        }

        //NHS Number was removed, removing resources related to it
        if (StringUtils.isEmpty(currentNHSNumber) && !StringUtils.isEmpty(previousNHSNumber)) {
            for (ResourceWrapper wrapper : resources) {
                wrapper.setDeleted(true);
                Resource resource = FhirResourceHelper.deserialiseResouce(wrapper.getResourceData());
                exchangeBatchExtraResourceDalI.saveExtraResource(
                        params.getExchangeId(), params.getBatchId(), resource.getResourceType(), wrapper.getResourceId());
            }
            transformResources(resources, params);
        }
        //NHS Number was added or person id changed, adding resources related to it
        else if(((!StringUtils.isEmpty(currentNHSNumber) && StringUtils.isEmpty(previousNHSNumber))
                || enterprisePersonId != personId)) {
            for (ResourceWrapper wrapper : resources) {
                Resource resource = FhirResourceHelper.deserialiseResouce(wrapper.getResourceData());
                exchangeBatchExtraResourceDalI.saveExtraResource(
                        params.getExchangeId(), params.getBatchId(), resource.getResourceType(), wrapper.getResourceId());
            }
            transformResources(resources, params);
        }

        //DOB changed values need to reprocess specific resources for changes in age_at_event
        if (((current.getBirthDate() == null) != (previous.getBirthDate() == null))
                || (current.getBirthDate() != null && previous.getBirthDate() != null
                && current.getBirthDate().equals(previous.getBirthDate()))) {

            List<ResourceWrapper> reprocess = new ArrayList();

            List<ResourceType> ageRelatedResourceTypes = new ArrayList();
            ageRelatedResourceTypes.add(ResourceType.Encounter);
            ageRelatedResourceTypes.add(ResourceType.AllergyIntolerance);
            ageRelatedResourceTypes.add(ResourceType.MedicationStatement);
            ageRelatedResourceTypes.add(ResourceType.MedicationOrder);
            ageRelatedResourceTypes.add(ResourceType.Observation);
            ageRelatedResourceTypes.add(ResourceType.ProcedureRequest);
            ageRelatedResourceTypes.add(ResourceType.ReferralRequest);

            for (ResourceWrapper resource : resources) {
                for (ResourceType type : ageRelatedResourceTypes) {
                    if (type.toString().equalsIgnoreCase(resource.getResourceType())) {
                        reprocess.add(resource);
                        exchangeBatchExtraResourceDalI.saveExtraResource(
                                params.getExchangeId(), params.getBatchId(), type, resource.getResourceId());
                    }
                }
            }
            transformResources(reprocess, params);
        }
    }


    /*private static byte[] getEncryptedSalt() throws Exception {
        if (saltBytes == null) {
            saltBytes = Resources.getResourceAsBytes(PSEUDO_SALT_RESOURCE);
        }
        return saltBytes;
    }*/

}
