package org.endeavourhealth.transform.pcr.transforms;

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
import org.endeavourhealth.core.database.rdbms.subscriberTransform.models.RdbmsPcrIdMap;
import org.endeavourhealth.core.xml.QueryDocument.*;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.json.LinkDistributorConfig;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PatientTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);


    private static final int BEST_ORG_SCORE = 10;

    private static Map<String, byte[]> saltCacheMap = new HashMap<>();
    private static Map<String, List<LinkDistributorConfig>> linkDistributorCacheMap = new HashMap<>();

    private static final PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();
    private static final PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                                     Resource resource,
                                     AbstractPcrCsvWriter csvWriter,
                                     PcrTransformParams params) throws Exception {

        Patient fhirPatient = (Patient) resource;


        String discoveryPersonId = patientLinkDal.getPersonId(fhirPatient.getId());

        //when the person ID table was populated, patients who had been deleted weren't added,
        //so we'll occasionally get null for some patients. If this happens, just do what would have
        //been done originally and assign an ID
        if (Strings.isNullOrEmpty(discoveryPersonId)) {
            PatientLinkPair pair = patientLinkDal.updatePersonId(params.getServiceId(), fhirPatient);
            discoveryPersonId = pair.getNewPersonId();
        }

        long id;
        long organizationId;
        String nhsNumber = null;
        //TODO where does verification come from?
        Integer nhsNumberVerificationTermId = null;
        Date dateOfBirth = null;
        Date dateOfDeath = null;
        int patientGenderId;
        Long usualPractitionerId = null;
        String title = null;
        String firstName = null;
        String middleNames = null;
        String lastName = null;
        String previousLastName = null;
        Long homeAddressId = null;
       // String ethnicCode = null;
        Long careProviderId = null;
        boolean isSpineSensitive;


        id = pcrId.longValue();
        organizationId = params.getEnterpriseOrganisationId().longValue();


        dateOfBirth = fhirPatient.getBirthDate();

        List<HumanName> names = fhirPatient.getName();
        for (HumanName nom : names) {
            if (nom.getUse().equals(HumanName.NameUse.OFFICIAL)) {

                if (nom.getFamily() != null) {lastName = nom.getFamily().toString();}
                if (nom.getGiven() != null) {firstName = nom.getGiven().get(0).toString();}
                if (nom.getGiven().size() > 1) {
                    StringBuilder midnames = new StringBuilder();
                    for (StringType namepart : nom.getGiven()) {
                        midnames.append(namepart.toString());
                        midnames.append(" ");
                    }
                    middleNames = midnames.toString();
                }
                if (!nom.getPrefix().isEmpty()) {
                    title = nom.getPrefix().get(0).toString();
                }
            }
        }

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
            if (StringUtils.isNumeric(fhirAddress.getId())) {
                homeAddressId = Long.parseLong(fhirAddress.getId());
                writeAddress(fhirAddress, Long.parseLong(fhirPatient.getId()), csvWriter);
            }
        }

        if (fhirPatient.hasContact()) {
            List<Patient.ContactComponent> contactList = fhirPatient.getContact();
            for (Patient.ContactComponent com : contactList) {
                writeContact(com, Long.parseLong(fhirPatient.getId()), csvWriter);
            }
        }


//        Extension ethnicityExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.PATIENT_ETHNICITY);
//        if (ethnicityExtension != null) {
//            CodeableConcept codeableConcept = (CodeableConcept) ethnicityExtension.getValue();
//            ethnicCode = CodeableConceptHelper.findCodingCode(codeableConcept, EthnicCategory.ASIAN_BANGLADESHI.getSystem());
//        }

        Extension spineExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.PATIENT_SPINE_SENSITIVE);
        if (spineExtension != null) {
            isSpineSensitive = true;
            CodeableConcept codeableConcept = (CodeableConcept) spineExtension.getValue();
            String nhsNumberVerificationTerm = CodeableConceptHelper.findCodingCode(codeableConcept, FhirExtensionUri.PATIENT_SPINE_SENSITIVE);
            if (StringUtils.isNumeric(nhsNumberVerificationTerm)) {
                nhsNumberVerificationTermId = Integer.parseInt(nhsNumberVerificationTerm);
            }
            //TODO map verification status to IM
        } else {
            isSpineSensitive = false;
        }
        if (fhirPatient.hasCareProvider()) {

            Reference orgReference = findOrgReference(fhirPatient, params);
            if (orgReference != null) {
                //added try/catch to track down a bug in Cerner->FHIR->enterprise
                try {
                    careProviderId = super.findEnterpriseId(params, orgReference);
                } catch (Throwable t) {
                    LOG.error("Error finding ID for reference " + orgReference.getReference());
                    throw t;
                }
            }
        }

        //check if our patient demographics also should be used as the person demographics. This is typically
        //true if our patient record is at a GP practice.
        //boolean shouldWritePersonRecord = shouldWritePersonRecord(fhirPatient, discoveryPersonId, params.getProtocolId());

        org.endeavourhealth.transform.pcr.outputModels.Patient patientWriter = (org.endeavourhealth.transform.pcr.outputModels.Patient) csvWriter;
//        org.endeavourhealth.transform.pcr.outputModels.Person personWriter = params.getOutputContainer().getPersons();
//        LinkDistributor linkDistributorWriter = params.getOutputContainer().getLinkDistributors();


        nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient);

        patientWriter.writeUpsert(id,
                organizationId,
                nhsNumber,
                nhsNumberVerificationTermId,
                dateOfBirth,
                dateOfDeath,
                patientGenderId,
                usualPractitionerId,
                careProviderId,
                title,
                firstName,
                middleNames,
                lastName,
                previousLastName,
                homeAddressId,
                //TODO - ethnic code in DB or not?
                //           ethnicCode,
                isSpineSensitive);
        writePatientIdentifier(id, fhirPatient, csvWriter);
    }

    private void writePatientIdentifier(long id, Patient patient, AbstractPcrCsvWriter csvWriter) throws Exception {
        org.endeavourhealth.transform.pcr.outputModels.PatientIdentifier patientIdWriter = (org.endeavourhealth.transform.pcr.outputModels.PatientIdentifier) csvWriter;
        List<Identifier> idList = patient.getIdentifier();
        for (Identifier thisId : idList) {
            String identifier = thisId.getValue();
            Long conceptId = IMClient.getOrCreateConceptId("ContactPoint.ContactPointUse." + thisId.getUse().toCode());
            patientIdWriter.writeUpsert(id, Long.parseLong(patient.getId()), conceptId, identifier);
        }
    }

    private void writeAddress(Address fhirAddress, long patientId, AbstractPcrCsvWriter csvWriter) throws Exception {
        org.endeavourhealth.transform.pcr.outputModels.PatientAddress patientAddressWriter = (org.endeavourhealth.transform.pcr.outputModels.PatientAddress) csvWriter;
        Period period = fhirAddress.getPeriod();
        Date startDate = period.getStart();
        Date endDate = period.getEnd();
        long longId;

        RdbmsPcrIdMap idMap = new RdbmsPcrIdMap();

        longId = idMap.getId();

        patientAddressWriter.writeUpsert(Long.parseLong(fhirAddress.getId()),
                patientId,
                Integer.parseInt(fhirAddress.getType().toCode()),
                Integer.parseInt(fhirAddress.getId()),
                startDate,
                endDate
        );
        org.endeavourhealth.transform.pcr.outputModels.Address addressWriter = (org.endeavourhealth.transform.pcr.outputModels.Address) csvWriter;
        List<StringType> addressList = fhirAddress.getLine();
        String al1 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 0);
        String al2 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 1);
        String al3 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 2);
        String al4 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 3);
        String postcode = fhirAddress.getPostalCode();
        //TODO get uprn (OS ref) and approximation. See TODO in Address outputModel
        Long propertyTypeId = IMClient.getOrCreateConceptId("Address.AddressUse." + fhirAddress.getUse().toCode());
        addressWriter.writeUpsert(longId, al1, al2, al3, al4, postcode,
                null, null, propertyTypeId);

    }

    private void writeContact(Patient.ContactComponent cc, long patientId, AbstractPcrCsvWriter csvWriter) throws Exception {
        org.endeavourhealth.transform.pcr.outputModels.PatientContact contactWriter = (org.endeavourhealth.transform.pcr.outputModels.PatientContact) csvWriter;
        List<ContactPoint> cpList = cc.getTelecom();
        for (ContactPoint cp : cpList) {
            String code = cp.getUse().toCode();
            Long type = IMClient.getOrCreateConceptId("ContactPoint.ContactPointSystem."+ cp.getUse().toCode());
            contactWriter.writeUpsert(Long.parseLong(cp.getId()), patientId, type, code);
        }

    }

    private Reference findOrgReference(Patient fhirPatient, PcrTransformParams params) throws Exception {

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

                Practitioner practitioner = (Practitioner) super.findResource(reference, params);
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

        for (ServiceContract serviceContract : protocol.getServiceContract()) {
            if (serviceContract.getType().equals(ServiceContractType.SUBSCRIBER)
                    && serviceContract.getActive() == ServiceContractActive.TRUE) {

                serviceIdsInProtocol.add(serviceContract.getService().getUuid());
            }
        }

        //get the other patient IDs for our person record
        List<PatientSearch> patientSearchesInProtocol = new ArrayList<>();
        patientSearchesInProtocol.add(patientSearch);

        Map<String, String> allPatientIdMap = patientLinkDal.getPatientAndServiceIdsForPerson(discoveryPersonId);
        for (String otherPatientId : allPatientIdMap.keySet()) {

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
            return BEST_ORG_SCORE - 4;
        }

        if (typeCode.equals(OrganisationType.GP_PRACTICE.getCode())) {
            return BEST_ORG_SCORE;
        } else if (typeCode.equals(OrganisationType.NHS_TRUST.getCode())
                || typeCode.equals(OrganisationType.NHS_TRUST_SERVICE.getCode())) {
            return BEST_ORG_SCORE - 1;
        } else {
            return BEST_ORG_SCORE - 2;
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

        return postcode.substring(0, len - 3);
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



    /*private static byte[] getEncryptedSalt() throws Exception {
        if (saltBytes == null) {
            saltBytes = Resources.getResourceAsBytes(PSEUDO_SALT_RESOURCE);
        }
        return saltBytes;
    }*/

}
