package org.endeavourhealth.transform.enterprise.transforms;

import OpenPseudonymiser.Crypto;
import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.eds.models.PatientLinkPair;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.reference.PostcodeDalI;
import org.endeavourhealth.core.database.dal.reference.models.PostcodeLookup;
import org.endeavourhealth.core.database.dal.subscriberTransform.EnterpriseAgeUpdaterlDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPersonMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.EnterpriseAge;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.PseudoIdAudit;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.PseudoIdBuilder;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.*;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberConfig;
import org.endeavourhealth.transform.subscriber.UPRN;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;
import org.endeavourhealth.transform.subscriber.transforms.PatientTransformer;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class PatientEnterpriseTransformer extends AbstractEnterpriseTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientEnterpriseTransformer.class);

    private static final PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();

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


        Patient fhirPatient = (Patient) resourceWrapper.getResource();

        //call this so we can audit which version of the patient we transformed last - must be done whether deleted or not
        params.setDtLastTransformedPatient(resourceWrapper);

        List<ResourceWrapper> fullHistory = EnterpriseTransformHelper.getFullHistory(resourceWrapper);

        //work out if something has changed that means we'll need to process the full patient record
        Patient previousVersion = findPreviousVersionSent(resourceWrapper, fullHistory, params);
        if (previousVersion != null) {
            processChangesFromPreviousVersion(params.getServiceId(), fhirPatient, previousVersion, params);
        }

        //check if the patient is deleted, is confidential, has no NHS number etc.
        if (fhirPatient == null
                || params.isBulkDeleteFromSubscriber()) {

            //delete the patient
            csvWriter.writeDelete(enterpriseId.longValue());

            //delete any dependent pseudo ID records
            deletePseudoIds(resourceWrapper, params);

            deleteAddresses(resourceWrapper, fullHistory, params);
            deleteTelecoms(resourceWrapper, fullHistory, params);
            PatientAdditional patientAdditional = params.getOutputContainer().getPatientAdditional();
            patientAdditional.writeDelete(enterpriseId.longValue());
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
        String mainPseudoId = null;
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

        mainPseudoId = transformPseudoIds(organizationId, id, personId, fhirPatient, resourceWrapper, params);

        currentAddressId = transformAddresses(enterpriseId.longValue(), personId, fhirPatient, fullHistory, resourceWrapper, params);
        transformTelecoms(enterpriseId.longValue(), personId, fhirPatient, fullHistory, resourceWrapper, params);

        transformPatientAdditionals(fhirPatient, params, enterpriseId.longValue());

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
            DateType d = (DateType) fhirPatient.getDeceased();
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
            postcodePrefix = PatientTransformer.findPostcodePrefix(postcode);

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
            CodeableConcept codeableConcept = (CodeableConcept) ethnicityExtension.getValue();
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

        org.endeavourhealth.transform.enterprise.outputModels.Patient patientWriter = (org.endeavourhealth.transform.enterprise.outputModels.Patient) csvWriter;
        //org.endeavourhealth.transform.enterprise.outputModels.LinkDistributor linkDistributorWriter = params.getOutputContainer().getLinkDistributors();

        if (!params.isPseudonymised()) {
            title = NameHelper.findPrefix(fhirPatient);
            firstNames = NameHelper.findForenames(fhirPatient);
            lastNames = NameHelper.findSurname(fhirPatient);
        }

        if (patientWriter.isPseduonymised()) {

            //if pseudonymised, all non-male/non-female genders should be treated as female
            //don't flatten now - https://endeavourhealth.atlassian.net/browse/SD-112
            /*if (fhirPatient.getGender() != Enumerations.AdministrativeGender.FEMALE
                    && fhirPatient.getGender() != Enumerations.AdministrativeGender.MALE) {
                patientGenderId = Enumerations.AdministrativeGender.FEMALE.ordinal();
            }*/

            //if we've generated a "main" pseudo ID, then make sure to audit it in the old-style
            //audit table used for some old extracts (the function that generated the pseudo IDs
            //already has audited in the newer way)
            if (!Strings.isNullOrEmpty(mainPseudoId)) {
                PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(params.getEnterpriseConfigName());
                pseudoIdDal.storePseudoIdOldWay(fhirPatient.getId(), mainPseudoId);
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
                    mainPseudoId,
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
        uprn(params, fhirPatient, id, personId, uprnwriter, params.getEnterpriseConfigName());
    }

    /**
     * deletes all pseudo IDs for a patient
     */
    private void deletePseudoIds(ResourceWrapper resourceWrapper, EnterpriseTransformHelper params) throws Exception {

        PatientPseudoId pseudoIdWriter = params.getOutputContainer().getPatientPseudoId();

        List<LinkDistributorConfig> linkDistributorConfigs = params.getConfig().getPseudoSalts();
        if (linkDistributorConfigs.isEmpty()) {
            return;
        }

        Map<LinkDistributorConfig, SubscriberId> hmIds = PatientTransformer.findPseudoIdIds(linkDistributorConfigs, params.getEnterpriseConfigName(), resourceWrapper, false);

        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {

            //create a unique source ID from the patient UUID plus the salt key name
            SubscriberId subTableId = hmIds.get(ldConfig);
            if (subTableId != null) { //may be null if never transformed before
                pseudoIdWriter.writeDelete(subTableId.getSubscriberId());
            }
        }
    }

    /**
     * generates the content for patient_pseudo_id table and returns the pseudo_id generated
     * for the first salt, which is later used for the field on the patient table itself
     */
    public String transformPseudoIds(long organizationId, long subscriberPatientId, long personId,
                                    Patient fhirPatient, ResourceWrapper resourceWrapper, EnterpriseTransformHelper params) throws Exception {

        PatientPseudoId pseudoIdWriter = params.getOutputContainer().getPatientPseudoId();

        List<LinkDistributorConfig> linkDistributorConfigs = params.getConfig().getPseudoSalts();
        if (linkDistributorConfigs.isEmpty()) {
            return null;
        }

        String nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient);
        Boolean b = IdentifierHelper.isValidNhsNumber(nhsNumber);
        boolean isNhsNumberValid = b != null && b.booleanValue();

        NhsNumberVerificationStatus status = IdentifierHelper.findNhsNumberVerificationStatus(fhirPatient);
        boolean isNhsNumberVerifiedByPublisher = status != null && status == NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;

        Map<LinkDistributorConfig, SubscriberId> hmIds = PatientTransformer.findPseudoIdIds(linkDistributorConfigs, params.getEnterpriseConfigName(), resourceWrapper, true);

        Map<LinkDistributorConfig, PseudoIdAudit> hmIdsGenerated = PseudoIdBuilder.generatePsuedoIdsFromConfigs(fhirPatient, params.getEnterpriseConfigName(), linkDistributorConfigs);

        for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
            String saltKeyName = ldConfig.getSaltKeyName();

            //get the actual pseudo ID generated
            PseudoIdAudit idGenerated = hmIdsGenerated.get(ldConfig);

            //get the table unique ID generated
            SubscriberId subTableId = hmIds.get(ldConfig);

            if (idGenerated != null) {

                String pseudoId = idGenerated.getPseudoId();

                pseudoIdWriter.writeUpsert(subTableId.getSubscriberId(),
                        organizationId,
                        subscriberPatientId,
                        personId,
                        saltKeyName,
                        pseudoId,
                        isNhsNumberValid,
                        isNhsNumberVerifiedByPublisher);

            } else {
                pseudoIdWriter.writeDelete(subTableId.getSubscriberId());

            }
        }

        //update the master table of patient UUID -> pseudo ID
        List<PseudoIdAudit> audits = new ArrayList<>(hmIdsGenerated.values());
        PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(params.getEnterpriseConfigName());
        pseudoIdDal.saveSubscriberPseudoIds(UUID.fromString(fhirPatient.getId()), subscriberPatientId, audits);

        //find the pseudo ID generated for the first salt and return it
        LinkDistributorConfig firstConfig = linkDistributorConfigs.get(0);
        PseudoIdAudit firstIdGenerated = hmIdsGenerated.get(firstConfig);
        if (firstIdGenerated == null) {
            return null;
        } else {
            return firstIdGenerated.getPseudoId();
        }
    }


    private void transformTelecoms(long subscriberPatientId, long subscriberPersonId, Patient currentPatient,
                                   List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper, EnterpriseTransformHelper params) throws Exception {

        PatientContact writer = params.getOutputContainer().getPatientContact();

        int maxTelecoms = PatientTransformer.getMaxNumberOfTelecoms(fullHistory);

        Map<Integer, SubscriberId> hmIds = PatientTransformer.findTelecomIds(maxTelecoms, params.getEnterpriseConfigName(), resourceWrapper, true);

        //update all addresses, delete any extra
        if (currentPatient.hasTelecom()) {
            for (int i = 0; i < currentPatient.getTelecom().size(); i++) {
                ContactPoint telecom = currentPatient.getTelecom().get(i);

                SubscriberId subTableId = hmIds.get(new Integer(i));

                long organisationId = params.getEnterpriseOrganisationId().longValue();
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
                writer.writeDelete(subTableId.getSubscriberId());
            }
        }
    }


    /**
     * deletes all telecoms for the patient
     */
    private void deleteTelecoms(ResourceWrapper resourceWrapper, List<ResourceWrapper> fullHistory, EnterpriseTransformHelper params) throws Exception {
        PatientContact writer = params.getOutputContainer().getPatientContact();
        int maxTelecoms = PatientTransformer.getMaxNumberOfTelecoms(fullHistory);

        Map<Integer, SubscriberId> hmIds = PatientTransformer.findTelecomIds(maxTelecoms, params.getEnterpriseConfigName(), resourceWrapper, false);

        for (int i = 0; i < maxTelecoms; i++) {

            SubscriberId subTableId = hmIds.get(new Integer(i));
            if (subTableId != null) { //may be null if never transformed before
                writer.writeDelete(subTableId.getSubscriberId());
            }
        }
    }


    /**
     * deletes all addresses for the patient
     */
    private void deleteAddresses(ResourceWrapper resourceWrapper, List<ResourceWrapper> fullHistory, EnterpriseTransformHelper params) throws Exception {
        PatientAddress writer = params.getOutputContainer().getPatientAddresses();
        int maxAddresses = PatientTransformer.getMaxNumberOfAddresses(fullHistory);

        Map<Integer, SubscriberId> hmIds = PatientTransformer.findAddressIds(maxAddresses, params.getEnterpriseConfigName(), resourceWrapper, false);

        for (int i = 0; i < maxAddresses; i++) {
            SubscriberId subTableId = hmIds.get(new Integer(i));
            if (subTableId != null) {
                writer.writeDelete(subTableId.getSubscriberId());
            }
        }
    }


    private Long transformAddresses(long subscriberPatientId, long subscriberPersonId, Patient currentPatient,
                                    List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper,
                                    EnterpriseTransformHelper params) throws Exception {

        PatientAddress writer = params.getOutputContainer().getPatientAddresses();

        int maxAddresses = PatientTransformer.getMaxNumberOfAddresses(fullHistory);

        Map<Integer, SubscriberId> hmIds = PatientTransformer.findAddressIds(maxAddresses, params.getEnterpriseConfigName(), resourceWrapper, true);

        Long currentAddressId = null;  //set as null to avoid 0 entries

        //update all addresses, delete any extra
        if (currentPatient.hasAddress()) {

            Address currentAddress = AddressHelper.findHomeAddress(currentPatient);

            for (int i = 0; i < currentPatient.getAddress().size(); i++) {
                Address address = currentPatient.getAddress().get(i);

                SubscriberId subTableId = hmIds.get(new Integer(i));

                //we have some addresses from the HL7 feed that are nearly empty and don't include a "use",
                //which makes them useless in a subscriber DB, so delete these rather than adding them
                if (!address.hasUse()) {
                    writer.writeDelete(subTableId.getSubscriberId());
                    continue;
                }

                //if this address is our current home one, then assign the ID
                if (address == currentAddress) {
                    currentAddressId = new Long(subTableId.getSubscriberId());
                }

                long organisationId = params.getEnterpriseOrganisationId().longValue();
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
                    postcode = PatientTransformer.findPostcodePrefix(address.getPostalCode());
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
        int start = 0;
        if (currentPatient.hasAddress()) {
            start = currentPatient.getAddress().size();
        }

        for (int i = start; i < maxAddresses; i++) {

            SubscriberId subTableId = hmIds.get(new Integer(i));
            if (subTableId != null) {
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


    private Patient findPreviousVersionSent(ResourceWrapper currentWrapper, List<ResourceWrapper> history, EnterpriseTransformHelper params) throws Exception {

        //if the helper has a null exchange ID we're invoking the transform from some one-off routine (e.g. bulk
        //populating the patient_pseudo_id table), so don't return any previous version so we don't end up trying
        //to re-bulk the entire patient record
        if (params.getExchangeId() == null) {
            return null;
        }

        Date dtLastSent = params.getDtLastTransformedPatient(currentWrapper.getResourceId());
        if (dtLastSent == null) {
            //if we've a null datetime, it means we've never sent for this patient
            //OR we've not sent anything since the audit was put in place, in which case we need to fall back on the old way
            //history is most-recent first
            //TODO - once audit table is fully populated, then this logic should be removed
            if (history.size() > 1) {
                ResourceWrapper wrapper = history.get(1); //want the second one
                return (Patient) wrapper.getResource();
            } else {
                return null;
            }
            //return null;
        }

        //history is most-recent-first
        for (ResourceWrapper wrapper : history) {
            Date dtCreatedAt = wrapper.getCreatedAt();
            if (dtCreatedAt.equals(dtLastSent) && wrapper.getResourceData() != null) {
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
    }


    private Reference findOrgReference(Patient fhirPatient, EnterpriseTransformHelper params) throws Exception {

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

                ResourceWrapper wrapper = params.findOrRetrieveResource(reference);
                if (wrapper == null) {
                    continue;
                }
                Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
                if (!practitioner.hasPractitionerRole()) {
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

    /**
     * if a patient record becomes confidential, then we need to delete all data previously sent, and conversely
     * if a patient record becomes non-confidential, then we need to send all data again
     */
    private void processChangesFromPreviousVersion(UUID serviceId, Patient current, Patient previous, EnterpriseTransformHelper params) throws Exception {

        //if the present status has changed then we need to either bulk-add or bulk-delete all data for the patient
        //and if the NHS number has changed, the person ID on each table will need updating
        if (hasNhsNumberChanged(current, previous)) {

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

    /*private boolean hasPresentStateChanged(EnterpriseTransformHelper params, Patient current, Patient previous) {

        boolean nowShouldBePresent = params.shouldPatientBePresentInSubscriber(current);
        boolean previousShouldBePresent = params.shouldPatientBePresentInSubscriber(previous);

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



    public void uprn(EnterpriseTransformHelper params, Patient fhirPatient, long id, long personId, AbstractEnterpriseCsvWriter csvWriter, String configName) throws Exception {

        if (!fhirPatient.hasAddress()) {
            return;
        }

        if (!UPRN.isConfigured()) {
            return;
        }

        if (!UPRN.isActivated(configName)) {
            //LOG.debug("subscriber " + configName + " not activated for UPRN");
            return;
        }

        PatientAddressMatch uprnWriter = (PatientAddressMatch) csvWriter;

        for (Address address: fhirPatient.getAddress()) {
            String adrec = AddressHelper.generateDisplayText(address);
            //LOG.debug(adrec);

            boolean isActive = PeriodHelper.isActive(address.getPeriod());
            Integer stati = Integer.valueOf(0);
            if (isActive) {
                stati = Integer.valueOf(1);
            }

            String ids = Long.toString(id) + "`" + Long.toString(personId) + "`" + configName;

            String csv = UPRN.getAdrec(adrec, ids);

            if (Strings.isNullOrEmpty(csv)) {
                LOG.debug("Unable to get address from UPRN API");
                return;
            }
            LOG.trace("Got UPRN result " + csv);

            String[] ss = csv.split("\\~", -1);
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

            long luprn = Long.parseLong(sUprn);

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

                //LOG.debug("Pseduonymise!");

                SubscriberConfig c = params.getConfig();
                List<LinkDistributorConfig> salts = c.getPseudoSalts();
                LinkDistributorConfig firstSalt = salts.get(0);
                String base64Salt = firstSalt.getSalt();
                /*config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
                JsonNode pseudoNode = config.get("pseudonymisation");
                JsonNode saltNode = pseudoNode.get("salt");
                String base64Salt = saltNode.asText();*/
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

    private void transformPatientAdditionals(Patient fhir, EnterpriseTransformHelper params, long id) throws Exception {


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
                            String propertyCode = parameter.getName();
                            if (!propertyCode.startsWith("JSON_")) {
                                //these values are from IM API mapping so set as Discovery Code
                                String propertyScheme = IMConstant.DISCOVERY_CODE;
                                String type = parameter.getValue().getClass().getSimpleName();
                                if (type.equalsIgnoreCase("CodeableConcept")) {
                                    CodeableConcept parameterValue = (CodeableConcept) parameter.getValue();
                                    String valueCode = parameterValue.getCoding().get(0).getCode();
                                    String valueScheme = parameterValue.getCoding().get(0).getSystem();

                                    //we need to get the unique IM conceptId for the property and value
                                    String propertyConceptId = IMClient.getConceptIdForSchemeCode(propertyScheme, propertyCode);
                                    String valueConceptId = IMClient.getConceptIdForSchemeCode(valueScheme, valueCode);
                                    //write the IM values to the encounter_additional table upsert
                                    patientAdditional.writeUpsert(id, propertyConceptId, valueConceptId,null);
                                } else if (type.equalsIgnoreCase("StringType")) {
                                    LOG.debug("Non json string found:" + propertyCode);
                                }
                            } else {
                                //Handle JSON blobs
                                String propertyScheme = IMConstant.DISCOVERY_CODE;

                                //get the IM concept code
                                propertyCode = propertyCode.replace("JSON_", "");
                                String propertyConceptId
                                        = IMClient.getConceptIdForSchemeCode(propertyScheme, propertyCode);

                                //the value is a StringType storing JSON
                                StringType jsonValue = (StringType) parameter.getValue();
                                patientAdditional.writeUpsert(id, propertyConceptId, null, jsonValue.getValue());
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

}
