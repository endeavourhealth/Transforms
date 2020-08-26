package org.endeavourhealth.transform.subscriber.transforms;

import OpenPseudonymiser.Crypto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.eds.models.PatientLinkPair;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.reference.PostcodeDalI;
import org.endeavourhealth.core.database.dal.reference.models.PostcodeLookup;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPersonMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.PseudoIdBuilder;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.subscriber.*;
import org.endeavourhealth.transform.subscriber.json.ConfigParameter;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;
import org.endeavourhealth.transform.subscriber.targetTables.*;
import org.hl7.fhir.instance.model.*;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Practitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class PatientTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static final String PREFIX_PSEUDO_ID = "-PSEUDO-";
    private static final String PREFIX_ADDRESS_ID = "-ADDR-";
    private static final String PREFIX_TELECOM_ID = "-TELECOM-";
    //private static final String PREFIX_ADDRESS_MATCH_ID = "-ADDRMATCH-";

    private static final PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();

    public static String uprnToken = "";

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

        Patient fhirPatient = (Patient) resourceWrapper.getResource(); //returns null if deleted

        //call this so we can audit which version of the patient we transformed last - must be done whether deleted or not
        params.setDtLastTransformedPatient(resourceWrapper);

        //work out if something has changed that means we'll need to process the full patient record
        List<ResourceWrapper> fullHistory = getFullHistory(resourceWrapper);
        Patient previousVersion = findPreviousVersionSent(resourceWrapper, fullHistory, params);
        if (previousVersion != null) {
            processChangesFromPreviousVersion(params.getServiceId(), fhirPatient, previousVersion, params);
        }

        //check if the patient is deleted, is confidential, has no NHS number etc.
        if (fhirPatient == null
                || params.isBulkDeleteFromSubscriber()) {

            //delete the patient
            patientWriter.writeDelete(subscriberId);

            //delete any dependent pseudo ID records
            //deletePseudoIdsOldWay(resourceWrapper, params);
            deletePseudoIdsNewWay(resourceWrapper, params);

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
        transformPseudoIdsNewWay(organizationId, subscriberId.getSubscriberId(), personId, fhirPatient, resourceWrapper, params);
        //transformPseudoIdsOldWay(subscriberId.getSubscriberId(), personId, fhirPatient, resourceWrapper, params);

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
            DateType d = (DateType) fhirPatient.getDeceased();
            dateOfDeath = d.getValue();
        }


        Extension ethnicityExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.PATIENT_ETHNICITY);
        if (ethnicityExtension != null) {
            CodeableConcept codeableConcept = (CodeableConcept) ethnicityExtension.getValue();
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
            //don't flatten now - https://endeavourhealth.atlassian.net/browse/SD-112
            /*if (params.isPseudonymised()) {
                if (gender != Enumerations.AdministrativeGender.FEMALE
                        && gender != Enumerations.AdministrativeGender.MALE) {
                    gender = Enumerations.AdministrativeGender.FEMALE;
                }
            }*/

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

        transformPatientAdditionals(fhirPatient, params, subscriberId);
    }

    private void transformTelecoms(long subscriberPatientId, long subscriberPersonId, Patient currentPatient, List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        PatientContact writer = params.getOutputContainer().getPatientContacts();

        int maxTelecoms = getMaxNumberOfTelecoms(fullHistory);

        Map<Integer, SubscriberId> hmIds = findTelecomIds(maxTelecoms, params.getSubscriberConfigName(), resourceWrapper, true);

        //update all addresses, delete any extra
        if (currentPatient.hasTelecom()) {
            for (int i = 0; i < currentPatient.getTelecom().size(); i++) {
                ContactPoint telecom = currentPatient.getTelecom().get(i);

                SubscriberId subTableId = hmIds.get(new Integer(i));
                //params.setSubscriberIdTransformed(resourceWrapper, subTableId);

                long organisationId = params.getSubscriberOrganisationId().longValue();
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
        int start = 0;
        if (currentPatient.hasTelecom()) {
            start = currentPatient.getTelecom().size();
        }

        for (int i = start; i < maxTelecoms; i++) {
            SubscriberId subTableId = hmIds.get(new Integer(i));
            if (subTableId != null) {
                writer.writeDelete(subTableId);
            }

        }
    }

    private void deleteTelecoms(ResourceWrapper resourceWrapper, List<ResourceWrapper> fullHistory, SubscriberTransformHelper params) throws Exception {

        PatientContact writer = params.getOutputContainer().getPatientContacts();
        int maxTelecoms = getMaxNumberOfTelecoms(fullHistory);

        Map<Integer, SubscriberId> hmIds = findTelecomIds(maxTelecoms, params.getSubscriberConfigName(), resourceWrapper, false);

        for (int i = 0; i < maxTelecoms; i++) {

            SubscriberId subTableId = hmIds.get(new Integer(i));
            if (subTableId != null) { //may be null if never transformed before
                writer.writeDelete(subTableId);
            }
        }
    }

    private void deleteAddresses(ResourceWrapper resourceWrapper, List<ResourceWrapper> fullHistory, SubscriberTransformHelper params) throws Exception {

        //PatientAddressMatch uprnwriter = params.getOutputContainer().getPatientAddressMatch();

        PatientAddress writer = params.getOutputContainer().getPatientAddresses();
        int maxAddresses = getMaxNumberOfAddresses(fullHistory);

        Map<Integer, SubscriberId> hmIds = findAddressIds(maxAddresses, params.getSubscriberConfigName(), resourceWrapper, false);

        for (int i = 0; i < maxAddresses; i++) {
            SubscriberId subTableId = hmIds.get(new Integer(i));
            if (subTableId != null) { //may be null if never transformed before
                writer.writeDelete(subTableId);
            }
        }
    }

    private void uprn(SubscriberTransformHelper params, Patient currentPatient, int i, ResourceWrapper resourceWrapper, SubscriberId subTableId, String addressLine1, String addressLine2, String addressLine3, String addressLine4, String city, String postcode, Long currentAddressId) throws Exception {

        JsonNode config = ConfigManager.getConfigurationAsJson("UPRN", "db_subscriber");
        if (config == null) {
            return;
        }

        //JsonNode enabled = config.get("enabled");
        //if (enabled.asText().equals("0")) {return;}

        String configName = params.getSubscriberConfigName();

        //removed as this ID was never used but was generating a ton of IDs in the DB
        /*String uprn_sourceId = ReferenceHelper.createReferenceExternal(currentPatient).getReference() + PREFIX_ADDRESS_MATCH_ID + i;
        SubscriberId uprn_subTableId = findOrCreateSubscriberId(params, SubscriberTableId.PATIENT_ADDRESS_MATCH, uprn_sourceId); // was sourceId*/

        //params.setSubscriberIdTransformed(resourceWrapper, uprn_subTableId);

        // call the UPRN API
        JsonNode token_endpoint = config.get("token_endpoint");
        JsonNode clientid = config.get("clientid");
        JsonNode password = config.get("password");
        JsonNode username = config.get("username");
        JsonNode uprn_endpoint = config.get("uprn_endpoint");

        JsonNode zs = config.get("subscribers");
        Integer ok = UPRN.Activated(zs, configName);
        if (ok.equals(0)) {
            LOG.debug("subscriber " + configName + " not activated for UPRN, exiting");
            return;
        }

        uprnToken = UPRN.getUPRNToken(password.asText(), username.asText(), clientid.asText(), LOG, token_endpoint.asText());

        if (addressLine1 == null) {
            addressLine1 = "";
        }
        if (addressLine2 == null) {
            addressLine2 = "";
        }
        if (addressLine3 == null) {
            addressLine3 = "";
        }
        if (addressLine4 == null) {
            addressLine4 = "";
        }
        if (city == null) {
            city = "";
        }
        if (postcode == null) {
            postcode = "";
        }

        String adrec = addressLine1 + "," + addressLine2 + "," + addressLine3 + "," + addressLine4 + "," + city + "," + postcode;

        // debug
        // adrec="201,Darkes Lane,Potters Bar,EN6 1BX";

        String ids = Long.toString(subTableId.getSubscriberId()) + "`" + configName;
        String csv = UPRN.getAdrec(adrec, uprnToken, uprn_endpoint.asText(), ids);
        // token time out?
        if (csv.isEmpty()) {
            UPRN.uprnToken = "";
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

        String[] ss = csv.split("\\~", -1);

        // 0=locality, 1=number, 2=org, 3=postcode, 4=street, 5=town, 6=alg, 7=qual
        // 8=matpatbuild, 9=matpatflat, 10=matpatnumber, 11=matpatpostcode, 12=matpatstreet
        // 13=quality, 14=latitude, 15=longitude, 16=point
        // 17=X, 18=Y, 19=class, 20=uprn

        String sLat = ss[14];
        String sLong = ss[15];
        String sX = ss[17];
        String sY = ss[18];
        String sClass = ss[19];
        String sQualifier = ss[7];

        String sUprn = ss[20];

        if (sUprn.isEmpty()) {
            LOG.debug("UPRN = 0");
            return;
        }

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

        Integer stati = 0;
        if (currentAddressId != null) {
            stati = 1;
        }

        String znumber = ss[1];
        String zstreet = ss[4];
        String zlocality = ss[0];
        String ztown = ss[5];
        String zpostcode = ss[3];
        String zorg = ss[2];
        String match_post = ss[11];
        String match_street = ss[12];
        String match_number = ss[10];
        String match_building = ss[8];
        String match_flat = ss[9];

        // null fields because isPseudonymised
        if (params.isPseudonymised()) {
            //LOG.debug("Pseduonymise!");

            SubscriberConfig c = params.getConfig();
            List<LinkDistributorConfig> salts = c.getPseudoSalts();
            LinkDistributorConfig firstSalt = salts.get(0);
            String base64Salt = firstSalt.getSalt();

            /*config = ConfigManager.getConfigurationAsJson(params.getSubscriberConfigName(), "db_subscriber");
            JsonNode s = config.get("pseudo_salts");
            ArrayNode arrayNode = (ArrayNode) s;
            if (s == null) {throw new Exception("Unable to find UPRN salt");}
            JsonNode arrayElement = arrayNode.get(0);
            String base64Salt = arrayElement.get("salt").asText();*/

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
            znumber = null;
            zstreet = null;
            zlocality = null;
            ztown = null;
            zpostcode = null;
            zorg = null;
            //match_post=null; match_street=null; match_number=null; match_building=null; match_flat=null;
            lat = null;
            longitude = null;
            x = null;
            y = null;
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
    }

    private Long transformAddresses(long subscriberPatientId, long subscriberPersonId, Patient currentPatient, List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        PatientAddress writer = params.getOutputContainer().getPatientAddresses();

        int maxAddresses = getMaxNumberOfAddresses(fullHistory);

        Map<Integer, SubscriberId> hmIds = findAddressIds(maxAddresses, params.getSubscriberConfigName(), resourceWrapper, true);

        Long currentAddressId = null;

        //update all addresses, delete any extra
        if (currentPatient.hasAddress()) {

            Address currentAddress = AddressHelper.findHomeAddress(currentPatient);

            for (int i = 0; i < currentPatient.getAddress().size(); i++) {
                Address address = currentPatient.getAddress().get(i);

                SubscriberId subTableId = hmIds.get(new Integer(i));

                //we have some addresses from the HL7 feed that are nearly empty and don't include a "use",
                //which makes them useless in a subscriber DB, so delete these rather than adding them
                if (!address.hasUse()) {
                    writer.writeDelete(subTableId);
                    continue;
                }

                //if this address is our current home one, then assign the ID
                if (address == currentAddress) {
                    currentAddressId = new Long(subTableId.getSubscriberId());
                }

                long organisationId = params.getSubscriberOrganisationId().longValue();
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

                // get the address details again for UPRN (might be Pseudonymised)
                addressLine1 = getAddressLine(address, 0);
                addressLine2 = getAddressLine(address, 1);
                addressLine3 = getAddressLine(address, 2);
                addressLine4 = getAddressLine(address, 3);
                city = address.getCity();
                postcode = address.getPostalCode();

                uprn(params, currentPatient, i, resourceWrapper, subTableId, addressLine1, addressLine2, addressLine3, addressLine4, city, postcode, currentAddressId);
            }
        }

        //and make sure to delete any that we previously sent over that are no longer present
        //i.e. if we previously had five addresses and now only have three, we need to delete four and five
        int start = 0;
        if (currentPatient.hasAddress()) {
            start = currentPatient.getAddress().size();
        }

        for (int i = start; i < maxAddresses; i++) {

            SubscriberId subTableId = hmIds.get(new Integer(i));
            if (subTableId != null) {
                //params.setSubscriberIdTransformed(resourceWrapper, subTableId);

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

    public static int getMaxNumberOfTelecoms(List<ResourceWrapper> history) throws Exception {
        int max = 0;
        for (ResourceWrapper wrapper : history) {
            if (!wrapper.isDeleted()) {
                Patient patient = (Patient) wrapper.getResource();
                if (patient.hasTelecom()) {
                    max = Math.max(max, patient.getTelecom().size());
                }
            }
        }
        return max;
    }

    public static int getMaxNumberOfAddresses(List<ResourceWrapper> history) throws Exception {
        int max = 0;
        for (ResourceWrapper wrapper : history) {
            if (!wrapper.isDeleted()) {
                Patient patient = (Patient) wrapper.getResource();
                if (patient.hasAddress()) {
                    max = Math.max(max, patient.getAddress().size());
                }
            }
        }
        return max;
    }

    private Patient findPreviousVersionSent(ResourceWrapper currentWrapper, List<ResourceWrapper> history, SubscriberTransformHelper helper) throws Exception {

        //if the helper has a null exchange ID we're invoking the transform from some one-off routine (e.g. bulk
        //populating the patient_pseudo_id table), so don't return any previous version so we don't end up trying
        //to re-bulk the entire patient record
        if (helper.getExchangeId() == null) {
            return null;
        }

        //if we've a null datetime, it means we've never sent for this patient
        Date dtLastSent = helper.getDtLastTransformedPatient(currentWrapper.getResourceId());
        //Date dtLastSent = subscriberId.getDtUpdatedPreviouslySent();

        if (dtLastSent == null) {
            //LOG.debug("" + currentWrapper.getReferenceString() + " has dt_last_sent of null, so this must be first time it is being transformed (or was previously deleted)");
            return null;
        }

        //history is most-recent-first
        for (ResourceWrapper wrapper : history) {
            Date dtCreatedAt = wrapper.getCreatedAt();
            //LOG.debug("Compare dt_last_sent " + dtLastSent + " (" + dtLastSent.getTime() + ") against dtCreatedAt " + dtCreatedAt + " (" + dtCreatedAt.getTime() + ")");
            if (dtCreatedAt.equals(dtLastSent)) {
                //LOG.debug("" + currentWrapper.getReferenceString() + " has dt_last_sent of " + dtLastSent + " so using version from that date");
                return (Patient) wrapper.getResource();
            }
        }

        //in cases where we've deleted and re-bulked everything then the past audit of which version we sent is useless
        //and we aren't able to match to the new version. In that case, we should return an empty FHIR Patient
        //which will trigger the thing to send all the data again.
        LOG.warn("Failed to find previous version of " + currentWrapper.getReferenceString() + " for dtLastSent " + dtLastSent + ", will send all data again");
        Patient p = new Patient();
        p.setId(currentWrapper.getResourceId().toString());
        return p;
        //throw new Exception("Failed to find previous version of " + currentWrapper.getReferenceString() + " for dtLastSent " + dtLastSent);
    }

    private void deletePseudoIdsNewWay(ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        PatientPseudoId writer = params.getOutputContainer().getPatientPseudoId();

        List<LinkDistributorConfig> linkDistributorConfigs = params.getConfig().getPseudoSalts();

        Map<LinkDistributorConfig, SubscriberId> hmIds = findPseudoIdIds(linkDistributorConfigs, params.getSubscriberConfigName(), resourceWrapper, false);

        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {

            //create a unique source ID from the patient UUID plus the salt key name
            SubscriberId subTableId = hmIds.get(ldConfig);
            if (subTableId != null) {
                writer.writeDelete(subTableId);
            }
        }
    }

    /*private void deletePseudoIdsOldWay(ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.PseudoId pseudoIdWriter = params.getOutputContainer().getPseudoIds();

        List<LinkDistributorConfig> linkDistributorConfigs = params.getConfig().getPseudoSalts();

        Map<LinkDistributorConfig, SubscriberId> hmIds = findPseudoIdIds(linkDistributorConfigs, params.getSubscriberConfigName(), resourceWrapper, false);

        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {

            //create a unique source ID from the patient UUID plus the salt key name
            SubscriberId subTableId = hmIds.get(ldConfig);
            if (subTableId != null) {
                //params.setSubscriberIdTransformed(resourceWrapper, subTableId);
                pseudoIdWriter.writeDelete(subTableId);
            }
        }

    }*/

    public void transformPseudoIdsNewWay(long organizationId, long subscriberPatientId, long personId, Patient fhirPatient, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        List<LinkDistributorConfig> linkDistributorConfigs = params.getConfig().getPseudoSalts();
        if (linkDistributorConfigs.isEmpty()) {
            return;
        }


        String nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient);
        Boolean b = IdentifierHelper.isValidNhsNumber(nhsNumber);
        boolean isNhsNumberValid = b != null && b.booleanValue();

        NhsNumberVerificationStatus status = IdentifierHelper.findNhsNumberVerificationStatus(fhirPatient);
        boolean isNhsNumberVerifiedByPublisher = status != null && status == NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;

        PatientPseudoId writer = params.getOutputContainer().getPatientPseudoId();

        Map<LinkDistributorConfig, SubscriberId> hmIds = findPseudoIdIds(linkDistributorConfigs, params.getSubscriberConfigName(), resourceWrapper, true);

        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
            String saltKeyName = ldConfig.getSaltKeyName();

            String pseudoId = PseudoIdBuilder.generatePsuedoIdFromConfig(params.getSubscriberConfigName(), ldConfig, fhirPatient);

            //create a unique source ID from the patient UUID plus the salt key name
            SubscriberId subTableId = hmIds.get(ldConfig);

            if (!Strings.isNullOrEmpty(pseudoId)) {

                writer.writeUpsert(subTableId,
                        organizationId,
                        subscriberPatientId,
                        personId,
                        saltKeyName,
                        pseudoId,
                        isNhsNumberValid,
                        isNhsNumberVerifiedByPublisher);

                //only persist the pseudo ID if it's non-null
                PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(params.getSubscriberConfigName());
                pseudoIdDal.saveSubscriberPseudoId(UUID.fromString(fhirPatient.getId()), subscriberPatientId, saltKeyName, pseudoId);

            } else {
                writer.writeDelete(subTableId);
            }
        }
    }

    /*private void transformPseudoIdsOldWay(long subscriberPatientId, long subscriberPersonId, Patient fhirPatient, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.PseudoId pseudoIdWriter = params.getOutputContainer().getPseudoIds();

        List<LinkDistributorConfig> linkDistributorConfigs = params.getConfig().getPseudoSalts();

        Map<LinkDistributorConfig, SubscriberId> hmIds = findPseudoIdIds(linkDistributorConfigs, params.getSubscriberConfigName(), resourceWrapper, true);

        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
            String saltKeyName = ldConfig.getSaltKeyName();


            String pseudoId = PseudoIdBuilder.generatePsuedoIdFromConfig(params.getSubscriberConfigName(), ldConfig, fhirPatient);

            SubscriberId subTableId = hmIds.get(ldConfig);

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
    }*/

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.PATIENT;
    }


    private Reference findOrgReference(Patient fhirPatient, SubscriberTransformHelper params) throws Exception {

        //find a direct org reference first
        for (Reference reference : fhirPatient.getCareProvider()) {
            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);

            if (comps.getResourceType() == ResourceType.Organization) {
                return reference;
            }
        }

        //if no org reference, look for a practitioner one
        for (Reference reference : fhirPatient.getCareProvider()) {
            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
            if (comps.getResourceType() == ResourceType.Practitioner) {

                Practitioner practitioner = (Practitioner) params.findOrRetrieveResource(reference);
                if (practitioner == null
                        || !practitioner.hasPractitionerRole()) {
                    continue;
                }

                for (Practitioner.PractitionerPractitionerRoleComponent role : practitioner.getPractitionerRole()) {

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

        return postcode.substring(0, len - 3);
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


    private static String convertJsonNodeToString(JsonNode jsonNode) throws Exception {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Object json = mapper.readValue(jsonNode.toString(), Object.class);
            return mapper.writeValueAsString(json);
        } catch (Exception e) {
            throw new Exception("Error parsing Link Distributor Config");
        }
    }

    private void processChangesFromPreviousVersion(UUID serviceId, Patient current, Patient previous, SubscriberTransformHelper params) throws Exception {

        //if the present status has changed then we need to either bulk-add or bulk-delete all data for the patient
        //and if the NHS number has changed, the person ID on each table will need updating
        //and if the DoB has changed, then the age_at_event will need recalculating for everything
        if (hasDobChanged(current, previous)
                || hasNhsNumberChanged(current, previous)) {

            //retrieve all resources and add them to the current transform. This will ensure they then get transformed
            //back in FhirToEnterpriseCsvTransformer. Each individual transform will know if the patient is confidential
            //or not, which will result in either a delete or insert being sent
            UUID patientUuid = UUID.fromString(previous.getId()); //current may be null, so get ID from previous
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> allPatientResources = resourceDal.getResourcesByPatient(serviceId, patientUuid);
            for (ResourceWrapper wrapper : allPatientResources) {
                params.addResourceToTransform(wrapper);
            }
        }
    }

    /*private boolean hasPresentStateChanged(SubscriberTransformHelper helper, Patient current, Patient previous) {

        boolean nowShouldBePresent = helper.shouldPatientBePresentInSubscriber(current);
        boolean previousShouldBePresent = helper.shouldPatientBePresentInSubscriber(previous);

        return nowShouldBePresent != previousShouldBePresent;
    }*/

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


    /**
     * finds (and creates) mapped IDs for each pseudo ID
     */
    public static Map<LinkDistributorConfig, SubscriberId> findPseudoIdIds(List<LinkDistributorConfig> linkDistributorConfigs,
                                                                            String subscriberConfigName,
                                                                            ResourceWrapper patientWrapper,
                                                                            boolean createIfMissing) throws Exception {

        if (linkDistributorConfigs.isEmpty()) {
            return new HashMap<>();
        }

        //pre-prepare a list of all source IDs that need mapping
        Map<String, LinkDistributorConfig> hmBySourceId = new HashMap<>();
        List<String> sourceIds = new ArrayList<>();
        for (LinkDistributorConfig linkDistributorConfig : linkDistributorConfigs) {
            String sourceId = patientWrapper.getReferenceString() + PREFIX_PSEUDO_ID + linkDistributorConfig.getSaltKeyName();
            sourceIds.add(sourceId);
            hmBySourceId.put(sourceId, linkDistributorConfig);
        }

        //map all in one go
        Map<String, SubscriberId> hmIds = findOrCreateSubscriberIds(subscriberConfigName, SubscriberTableId.PATIENT_PSEUDO_ID, sourceIds, createIfMissing);

        //reverse look up to return an ID for each status
        Map<LinkDistributorConfig, SubscriberId> ret = new HashMap<>();
        for (String sourceId : hmIds.keySet()) {
            SubscriberId id = hmIds.get(sourceId);
            LinkDistributorConfig linkDistributorConfig = hmBySourceId.get(sourceId);
            ret.put(linkDistributorConfig, id);
        }

        return ret;
    }


    /**
     * finds (and creates) mapped IDs for each pseudo ID
     */
    public static Map<Integer, SubscriberId> findTelecomIds(int maxTelecoms,
                                                             String subscriberConfigName,
                                                             ResourceWrapper patientWrapper,
                                                             boolean createIfMissing) throws Exception {

        if (maxTelecoms == 0) {
            return new HashMap<>();
        }

        //pre-prepare a list of all source IDs that need mapping
        Map<String, Integer> hmBySourceId = new HashMap<>();
        List<String> sourceIds = new ArrayList<>();
        for (int i = 0; i < maxTelecoms; i++) {
            String sourceId = patientWrapper.getReferenceString() + PREFIX_TELECOM_ID + i;
            sourceIds.add(sourceId);
            hmBySourceId.put(sourceId, new Integer(i));
        }

        //map all in one go
        Map<String, SubscriberId> hmIds = findOrCreateSubscriberIds(subscriberConfigName, SubscriberTableId.PATIENT_CONTACT, sourceIds, createIfMissing);

        //reverse look up to return an ID for each record
        Map<Integer, SubscriberId> ret = new HashMap<>();
        for (String sourceId : hmIds.keySet()) {
            SubscriberId id = hmIds.get(sourceId);
            Integer index = hmBySourceId.get(sourceId);
            ret.put(index, id);
        }

        return ret;
    }



    /**
     * finds (and creates) mapped IDs for each pseudo ID
     */
    public static Map<Integer, SubscriberId> findAddressIds(int maxAddresses,
                                                             String subscriberConfigName,
                                                             ResourceWrapper patientWrapper,
                                                             boolean createIfMissing) throws Exception {

        if (maxAddresses == 0) {
            return new HashMap<>();
        }

        //pre-prepare a list of all source IDs that need mapping
        Map<String, Integer> hmBySourceId = new HashMap<>();
        List<String> sourceIds = new ArrayList<>();
        for (int i = 0; i < maxAddresses; i++) {
            String sourceId = patientWrapper.getReferenceString() + PREFIX_ADDRESS_ID + i;
            sourceIds.add(sourceId);
            hmBySourceId.put(sourceId, new Integer(i));
        }

        //map all in one go
        Map<String, SubscriberId> hmIds = findOrCreateSubscriberIds(subscriberConfigName, SubscriberTableId.PATIENT_ADDRESS, sourceIds, createIfMissing);

        //reverse look up to return an ID for each record
        Map<Integer, SubscriberId> ret = new HashMap<>();
        for (String sourceId : hmIds.keySet()) {
            SubscriberId id = hmIds.get(sourceId);
            Integer index = hmBySourceId.get(sourceId);
            ret.put(index, id);
        }

        return ret;
    }

    private void transformPatientAdditionals(Patient fhir, SubscriberTransformHelper params, SubscriberId id) throws Exception {
        //if it has no extension data, then nothing further to do
        if (!fhir.hasExtension()) {
            return;
        }
        //then for each additional extension parameter the additional data
        Extension additionalExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ADDITIONAL);
        if (additionalExtension != null) {

            Reference idReference = (Reference)additionalExtension.getValue();
            String idReferenceValue = idReference.getReference();
            idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char

            for (Resource containedResource: fhir.getContained()) {
                if (containedResource.getId().equals(idReferenceValue)) {

                    OutputContainer outputContainer = params.getOutputContainer();
                    PatientAdditional patientAdditional = outputContainer.getPatientAdditional();

                    //additional extension data is stored as Parameter resources
                    Parameters parameters = (Parameters)containedResource;

                    //get all the entries in the parameters list
                    List<Parameters.ParametersParameterComponent> entries = parameters.getParameter();
                    for (Parameters.ParametersParameterComponent parameter : entries) {

                        //each parameter entry  will have a key value pair of name and CodeableConcept value
                        if (parameter.hasName() && parameter.hasValue()) {

                            //these values are from IM API mapping
                            String propertyCode = parameter.getName();
                            String propertyScheme = "CM_DiscoveryCode";

                            CodeableConcept parameterValue = (CodeableConcept) parameter.getValue();
                            String valueCode = parameterValue.getCoding().get(0).getCode();
                            String valueScheme = parameterValue.getCoding().get(0).getSystem();

                            //we need to look up DBids for both
                            Integer propertyConceptDbid =
                                    IMClient.getConceptDbidForSchemeCode(propertyScheme, propertyCode);
                            Integer valueConceptDbid =
                                    IMClient.getConceptDbidForSchemeCode(valueScheme, valueCode);
                            //write the IM values to the patient_additional table upsert
                            patientAdditional.writeUpsert(id, propertyConceptDbid, valueConceptDbid);
                        }
                    }
                    break;
                }
            }
        }
    }


}
