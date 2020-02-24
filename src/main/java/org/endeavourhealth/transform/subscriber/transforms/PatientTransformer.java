package org.endeavourhealth.transform.subscriber.transforms;

import OpenPseudonymiser.Crypto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
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
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPersonMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.core.xml.QueryDocument.*;
import org.endeavourhealth.transform.common.PseudoIdBuilder;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.json.ConfigParameter;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;
import org.endeavourhealth.transform.subscriber.targetTables.PatientAddress;
import org.endeavourhealth.transform.subscriber.targetTables.PatientContact;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class PatientTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static final String PREFIX_PSEUDO_ID = "-PSEUDO-";
    private static final String PREFIX_ADDRESS_ID = "-ADDR-";
    private static final String PREFIX_TELECOM_ID = "-TELECOM-";

    /*private static final String PSEUDO_KEY_NHS_NUMBER = "NHSNumber";
    private static final String PSEUDO_KEY_PATIENT_NUMBER = "PatientNumber";
    private static final String PSEUDO_KEY_DATE_OF_BIRTH = "DOB";*/

    //private static final int BEST_ORG_SCORE = 10;

    //private static Map<String, LinkDistributorConfig> mainPseudoCacheMap = new HashMap<>();
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

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Patient patientWriter = params.getOutputContainer().getPatients();

        Patient fhirPatient = (Patient)resourceWrapper.getResource(); //returns null if deleted

        //work out if something has changed that means we'll need to process the full patient record
        List<ResourceWrapper> fullHistory = getFullHistory(resourceWrapper);
        Patient previousVersion = findPreviousVersionSent(resourceWrapper, fullHistory, subscriberId);
        if (previousVersion != null) {
            processChangesFromPreviousVersion(params.getServiceId(), fhirPatient, previousVersion, params);
        }

        //check if the patient is deleted, is confidential, has no NHS number etc.
        if (!params.shouldPatientBePresentInSubscriber(fhirPatient)) {

            //delete the patient
            patientWriter.writeDelete(subscriberId);

            //delete any dependent pseudo ID records
            deletePseudoIds(resourceWrapper, params);

            //we'll need a previous instance to delete any dependent addresses and telecoms
            deleteAddresses(resourceWrapper, fullHistory, params);
            deleteTelecoms(resourceWrapper, fullHistory, params);

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

        SubscriberPersonMappingDalI enterpriseIdDal = DalProvider.factorySubscriberPersonMappingDal(params.getSubscriberConfigName());
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


        organizationId = params.getSubscriberOrganisationId().longValue();
        personId = enterprisePersonId.longValue();

        //transform our dependent objects first, as we need the address ID
        transformPseudoIds(subscriberId.getSubscriberId(), personId, fhirPatient, resourceWrapper, params);
        currentAddressId = transformAddresses(subscriberId.getSubscriberId(), personId, fhirPatient, fullHistory, resourceWrapper, params);
        transformTelecoms(subscriberId.getSubscriberId(), personId, fhirPatient, fullHistory, resourceWrapper, params);


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
            String ethnicCodeId = "";
            ethnicCodeId = CodeableConceptHelper.findCodingCode(codeableConcept, FhirValueSetUri.VALUE_SET_ETHNIC_CATEGORY);
            String display = CodeableConceptHelper.findCodingDisplay(codeableConcept, FhirValueSetUri.VALUE_SET_ETHNIC_CATEGORY);

            if (!ethnicCodeId.isEmpty()) {
                ethnicCodeConceptId = IMHelper.getIMConcept(params, fhirPatient, IMConstant.FHIR_ETHNIC_CATEGORY, ethnicCodeId, display);
            }
        }

        if (fhirPatient.hasCareProvider()) {

            Reference orgReference = findOrgReference(fhirPatient, params);
            if (orgReference != null) {
                //added try/catch to track down a bug in Cerner->FHIR->Enterprise
                try {
                    registeredPracticeId = transformOnDemandAndMapId(orgReference, SubscriberTableId.ORGANIZATION, params);
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
    }

    private void transformTelecoms(long subscriberPatientId, long subscriberPersonId, Patient currentPatient, List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        PatientContact writer = params.getOutputContainer().getPatientContacts();

        //update all addresses, delete any extra
        if (currentPatient.hasTelecom()) {
            for (int i = 0; i < currentPatient.getTelecom().size(); i++) {
                ContactPoint telecom = currentPatient.getTelecom().get(i);

                String sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_TELECOM_ID + i;
                SubscriberId subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PATIENT_CONTACT, sourceId);
                params.setSubscriberIdTransformed(resourceWrapper, subTableId);

                long organisationId = params.getSubscriberOrganisationId();
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

                writer.writeDelete(subTableId);
            }

        }
    }

    private void deleteTelecoms(ResourceWrapper resourceWrapper, List<ResourceWrapper> fullHistory, SubscriberTransformHelper params) throws Exception {

        PatientContact writer = params.getOutputContainer().getPatientContacts();
        int maxTelecoms = getMaxNumberOfTelecoms(fullHistory);

        for (int i=0; i<maxTelecoms; i++) {

            String sourceId = resourceWrapper.getReferenceString() + PREFIX_TELECOM_ID + i;
            SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PATIENT_CONTACT, sourceId);
            if (subTableId != null) {
                params.setSubscriberIdTransformed(resourceWrapper, subTableId);
                writer.writeDelete(subTableId);
            }
        }
    }

    private void deleteAddresses(ResourceWrapper resourceWrapper, List<ResourceWrapper> fullHistory, SubscriberTransformHelper params) throws Exception {

        PatientAddress writer = params.getOutputContainer().getPatientAddresses();
        int maxAddresses = getMaxNumberOfAddresses(fullHistory);

        for (int i=0; i<maxAddresses; i++) {

            String sourceId = resourceWrapper.getReferenceString() + PREFIX_ADDRESS_ID + i;
            SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PATIENT_ADDRESS, sourceId);
            if (subTableId != null) {
                params.setSubscriberIdTransformed(resourceWrapper, subTableId);
                writer.writeDelete(subTableId);
            }
        }
    }

    private Long transformAddresses(long subscriberPatientId, long subscriberPersonId, Patient currentPatient, List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        PatientAddress writer = params.getOutputContainer().getPatientAddresses();

        Long currentAddressId = null;

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

                long organisationId = params.getSubscriberOrganisationId();
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

    private static List<ResourceWrapper> getFullHistory(ResourceWrapper resourceWrapper) throws Exception {
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        UUID serviceId = resourceWrapper.getServiceId();
        String resourceType = resourceWrapper.getResourceType();
        UUID resourceId = resourceWrapper.getResourceId();
        return resourceDal.getResourceHistory(serviceId, resourceType, resourceId);
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

    private Patient findPreviousVersionSent(ResourceWrapper currentWrapper, List<ResourceWrapper> history, SubscriberId subscriberId) throws Exception {

        //if we've a null datetime, it means we've never sent for this patient
        Date dtLastSent = subscriberId.getDtUpdatedPreviouslySent();
        //LOG.debug("Transforming " + currentWrapper.getReferenceString() + " dt_last_sent = " + dtLastSent + " and dt current version = " + currentWrapper.getCreatedAt());

        if (dtLastSent == null) {
            //LOG.debug("" + currentWrapper.getReferenceString() + " has dt_last_sent of null, so this must be first time it is being transformed (or was previously deleted)");
            return null;
        }

        //history is most-recent-first
        for (ResourceWrapper wrapper: history) {
            Date dtCreatedAt = wrapper.getCreatedAt();
            //LOG.debug("Compare dt_last_sent " + dtLastSent + " (" + dtLastSent.getTime() + ") against dtCreatedAt " + dtCreatedAt + " (" + dtCreatedAt.getTime() + ")");
            if (dtCreatedAt.equals(dtLastSent)) {
                //LOG.debug("" + currentWrapper.getReferenceString() + " has dt_last_sent of " + dtLastSent + " so using version from that date");
                return (Patient)wrapper.getResource();
            }
        }

        throw new Exception("Failed to find previous version of " + currentWrapper.getReferenceString() + " for dtLastSent " + dtLastSent);
    }


    private void deletePseudoIds(ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.PseudoId pseudoIdWriter = params.getOutputContainer().getPseudoIds();

        List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(params.getSubscriberConfigName());
        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
            String saltKeyName = ldConfig.getSaltKeyName();

            //create a unique source ID from the patient UUID plus the salt key name
            String referenceStr = ReferenceHelper.createResourceReference(resourceWrapper.getResourceTypeObj(), resourceWrapper.getResourceIdStr());
            String sourceId = referenceStr + PREFIX_PSEUDO_ID + saltKeyName;
            SubscriberId subTableId = findSubscriberId(params, SubscriberTableId.PSEUDO_ID, sourceId);
            if (subTableId != null) {
                params.setSubscriberIdTransformed(resourceWrapper, subTableId);
                pseudoIdWriter.writeDelete(subTableId);
            }
        }

    }



    private void transformPseudoIds(long subscriberPatientId, long subscriberPersonId, Patient fhirPatient, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.PseudoId pseudoIdWriter = params.getOutputContainer().getPseudoIds();

        List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(params.getSubscriberConfigName());
        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
            String saltKeyName = ldConfig.getSaltKeyName();


            String pseudoId = PseudoIdBuilder.generatePsuedoIdFromConfig(params.getSubscriberConfigName(), ldConfig, fhirPatient);

            //create a unique source ID from the patient UUID plus the salt key name
            String sourceId = ReferenceHelper.createReferenceExternal(fhirPatient).getReference() + PREFIX_PSEUDO_ID + saltKeyName;
            SubscriberId subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PSEUDO_ID, sourceId);
            params.setSubscriberIdTransformed(resourceWrapper, subTableId);

            if (!Strings.isNullOrEmpty(pseudoId)) {

                pseudoIdWriter.writeUpsert(subTableId,
                        subscriberPatientId,
                        saltKeyName,
                        pseudoId);

                //only persist the pseudo ID if it's non-null
                PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(params.getSubscriberConfigName());
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


    private Reference findOrgReference(Patient fhirPatient, SubscriberTransformHelper params) throws Exception {

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

                Practitioner practitioner = (Practitioner)params.findOrRetrieveResource(reference);
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

    private String pseudonymiseUsingConfig(SubscriberTransformHelper params, Patient fhirPatient, LinkDistributorConfig config) throws Exception {

        PseudoIdBuilder builder = new PseudoIdBuilder(params.getSubscriberConfigName(), config.getSaltKeyName(), config.getSalt());

        List<ConfigParameter> parameters = config.getParameters();
        for (ConfigParameter param : parameters) {

            String fieldName = param.getFieldName();
            String fieldFormat = param.getFormat();
            String fieldLabel = param.getFieldLabel();

            boolean foundValue = builder.addPatientValue(fhirPatient, fieldName, fieldLabel, fieldFormat);

            //if this element is mandatory, then fail if our field is empty
            Boolean mandatory = param.getMandatory();
            if (mandatory != null
                    && mandatory.booleanValue()
                    && !foundValue) {
                return null;
            }
        }

        return builder.createPseudoId();
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

    private void processChangesFromPreviousVersion(UUID serviceId, Patient current, Patient previous, SubscriberTransformHelper params) throws  Exception {

        //if the present status has changed then we need to either bulk-add or bulk-delete all data for the patient
        //and if the NHS number has changed, the person ID on each table will need updating
        //and if the DoB has changed, then the age_at_event will need recalculating for everything
        if (hasPresentStateChanged(params, current, previous)
            || hasDobChanged(current, previous)
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

    private boolean hasPresentStateChanged(SubscriberTransformHelper helper, Patient current, Patient previous) {

        boolean nowShouldBePresent = helper.shouldPatientBePresentInSubscriber(current);
        boolean previousShouldBePresent = helper.shouldPatientBePresentInSubscriber(previous);

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

    private static boolean hasDobChanged(Patient current, Patient previous) {

        Date nowDoB = null;
        if (current != null) {
            nowDoB = current.getBirthDate();
        }

        Date previousDoB = null;
        if (previous != null) {
            previousDoB = previous.getBirthDate();
        }

        if (nowDoB == null && previousDoB == null) {
            //if both null, no change
            return false;
        } else if (nowDoB == null || previousDoB == null) {
            //if only one is null, change found
            return true;
        } else {
            //if neither null, compare dates
            return !nowDoB.equals(previousDoB);
        }
    }


    /*private static byte[] getEncryptedSalt() throws Exception {
        if (saltBytes == null) {
            saltBytes = Resources.getResourceAsBytes(PSEUDO_SALT_RESOURCE);
        }
        return saltBytes;
    }*/

}
