package org.endeavourhealth.transform.pcr.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.eds.PatientSearchDalI;
import org.endeavourhealth.core.database.dal.eds.models.PatientLinkPair;
import org.endeavourhealth.core.database.dal.eds.models.PatientSearch;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.json.LinkDistributorConfig;
import org.endeavourhealth.transform.pcr.outputModels.*;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Practitioner;
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
        Long organizationId;
        String nhsNumber = null;
        Long nhsNumberVerificationTermId = null;
        Date dateOfBirth = null;
        Date dateOfDeath = null;
        Long patientGenderId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        Long usualPractitionerId = null;
        Long enteredByPractitionerId = null;
        String title = null;
        String firstName = null;
        String middleNames = null;
        String lastName = null;
        String previousLastName = null;
        Long homeAddressId = null;
        Character ethnicCode = EthnicCategory.NOT_STATED.getCode().charAt(0);;
        Long careProviderId = null;
        boolean isSpineSensitive;


        id = pcrId.longValue();
        organizationId = params.getPcrOrganisationId().longValue();


        dateOfBirth = fhirPatient.getBirthDate();

        List<HumanName> names = fhirPatient.getName();
        for (HumanName nom : names) {
            if (nom.getUse().equals(HumanName.NameUse.OFFICIAL)) {

                if (nom.getFamily() != null) {
                    lastName = removeParen(nom.getFamily().toString());

                    if (nom.getGiven() != null) {
                        firstName = nom.getGiven().get(0).toString();
                    }
                    if (nom.getGiven().size() > 1) {
                        StringBuilder midnames = new StringBuilder();
                        for (int i = 1; i < nom.getGiven().size(); i++) {
                            midnames.append(nom.getGiven().get(i).toString());
                            midnames.append(" ");
                        }
                        middleNames = midnames.toString();
                    }
                    if (!nom.getPrefix().isEmpty()) {
                        title = nom.getPrefix().get(0).toString();
                    }
                } else {
                    if (nom.getText() != null && !nom.getText().isEmpty()) {
                        lastName = removeParen(nom.getText());
                    }
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
            //patientGenderId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            if (fhirPatient.getGender().equals(Enumerations.AdministrativeGender.MALE)) {
                patientGenderId = 0L;
            } else if (fhirPatient.getGender().equals(Enumerations.AdministrativeGender.FEMALE)) {
                patientGenderId = 1L;
            } else if (fhirPatient.getGender().equals(Enumerations.AdministrativeGender.OTHER)) {
                patientGenderId = 2L;
            } else {
                patientGenderId = 3l; //Unknown
            }
            //TODO IMClient.getOrCreateConceptId("Patient.Gender." + fhirPatient.getGender());

        } else {
            //TODO not clear how to map unknown to IM.
            //patientGenderId = Enumerations.AdministrativeGender.UNKNOWN.ordinal();
        }
        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {
            Reference enteredByPractitionerReference = (Reference) enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params);
        }
        if (enteredByPractitionerId == null) {
            enteredByPractitionerId = -1L;
        }

        Extension ethnicityExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.PATIENT_ETHNICITY);
        if (ethnicityExtension != null) {
            CodeableConcept codeableConcept = (CodeableConcept) ethnicityExtension.getValue();
            List<Coding> codes = codeableConcept.getCoding();
            if (codes != null && !codes.isEmpty()) {
                EthnicCategory ethnicCategory = EthnicCategory.NOT_STATED;
                try {
                    ethnicCategory = EthnicCategory.fromCode(codes.get(0).getCode());
                } catch (IllegalArgumentException exception) {
                    LOG.warn("Unknown ethnic code : " + codes.get(0).getCode());
                    ethnicCategory = EthnicCategory.NOT_STATED;
                }
                ethnicCode = ethnicCategory.getCode().charAt(0);
            }
        }

        Extension spineExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.PATIENT_SPINE_SENSITIVE);
        if (spineExtension != null) {
            isSpineSensitive = true;
            CodeableConcept codeableConcept = (CodeableConcept) spineExtension.getValue();
            String nhsNumberVerificationTerm = CodeableConceptHelper.findCodingCode(codeableConcept, FhirExtensionUri.PATIENT_SPINE_SENSITIVE);
            if (StringUtils.isNumeric(nhsNumberVerificationTerm)) {
                nhsNumberVerificationTermId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                //TODO IMClient.getOrCreateConceptId("Patient.NHSStatus." + nhsNumberVerificationTerm);
            }
        } else {
            isSpineSensitive = false;
        }
        if (fhirPatient.hasCareProvider()) {
            if (fhirPatient.getCareProvider() != null) {
                List<Reference> refs = fhirPatient.getCareProvider();
                List<Resource> resources = fhirPatient.getCareProviderTarget();
                for (int m = 0; m < resources.size(); m++) {
                    if (resources.get(m).getResourceType().equals(ResourceType.Practitioner)) {
                        usualPractitionerId = transformOnDemandAndMapId(refs.get(m), params);
                    } else if (resources.get(m).getResourceType().equals(ResourceType.Organization)) {
                        careProviderId = transformOnDemandAndMapId(refs.get(m), params);

                    }
                }
            }
        }

        org.endeavourhealth.transform.pcr.outputModels.Patient patientWriter = (org.endeavourhealth.transform.pcr.outputModels.Patient) csvWriter;
        nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient);
        OutputContainer data = params.getOutputContainer();
        PatientIdentifier patientIdentifierModel = data.getPatientIdentifiers();
        writePatientIdentifier(id, fhirPatient, enteredByPractitionerId, patientIdentifierModel);
        Long addressId = null;
        Address fhirAddress = null;
        if (fhirPatient.hasAddress() && fhirPatient.getAddress() != null) {
            for (Address address : fhirPatient.getAddress()) {
                if (address.hasUse() && address.getUse() != null && address.hasPeriod() && !address.getPeriod().hasEnd()) {
                    if (address.getUse().equals(Address.AddressUse.HOME)) {
                        LOG.debug("Patient has a HOME address");
                        fhirAddress = address;
                    }
                }
            }
            //TODO - the whole address code here needs rewriting to ensure address
            // history and unique ids for addresses.
            //If no home address try a temporary address
            if (fhirAddress == null) {
                for (Address address : fhirPatient.getAddress()) {
                    if (address.hasUse() && address.getUse() != null && address.hasPeriod() && !address.getPeriod().hasEnd()) {
                        if (address.getUse().equals(Address.AddressUse.TEMP)) {
                            LOG.debug("Patient has a TEMP address");
                            fhirAddress = address;
                        }
                    }
                }
            }
            if (fhirAddress == null) {
                fhirAddress = fhirPatient.getAddress().get(0);
            }
        }

        if (fhirAddress != null) {
            String fhirAdId = fhirAddress.getId();
            addressId = findOrCreatePcrId(params, ResourceType.Location.toString(), fhirPatient.getId());
            LOG.debug("Address id for patient is " + addressId);
            PatientAddress patientAddressWriter = data.getPatientAddresses();
            writeAddress(fhirAddress, id, addressId, enteredByPractitionerId, params, patientAddressWriter);
        } else {
            LOG.debug("Address is null for " + id);
        }
        patientWriter.writeUpsert(id,
                organizationId,
                nhsNumber,
                nhsNumberVerificationTermId,
                dateOfBirth,
                dateOfDeath,
                patientGenderId,
                usualPractitionerId,
                careProviderId,
                enteredByPractitionerId,
                title,
                firstName,
                middleNames,
                lastName,
                previousLastName,
                addressId,
                isSpineSensitive,
                ethnicCode);


//        if (fhirPatient.hasContact()) {
//            List<Patient.ContactComponent> contactList = fhirPatient.getContact();
//            for (Patient.ContactComponent com : contactList) {
//                if (com != null && !com.isEmpty()) {
//                    PatientContact patientContactWriter = data.getPatientContacts();
//                    writeContact(com, id, enteredByPractitionerId, patientContactWriter);
//                }
//            }
//        }

    }

    private void writePatientIdentifier(long id, Patient patient, Long
            enteredByPractitionerId, AbstractPcrCsvWriter csvWriter) throws Exception {
        PatientIdentifier patientIdWriter = (PatientIdentifier) csvWriter;
        List<Identifier> idList = patient.getIdentifier();
        for (Identifier thisId : idList) {
            if (!thisId.getValue().isEmpty()) {
                String identifier = thisId.getValue();
                Long conceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                //TODO IMClient.getOrCreateConceptId("ContactPoint.ContactPointUse." + thisId.getUse().toCode());
                patientIdWriter.writeUpsert(null, id, conceptId, identifier, enteredByPractitionerId);
            }
        }
    }

    private void writeAddress(Address fhirAddress, Long patientId, Long addressId, Long
            enteredByPractitionerId, PcrTransformParams params, AbstractPcrCsvWriter csvWriter) throws Exception {
        OutputContainer outputContainer = params.getOutputContainer();
        PatientAddress patientAddressWriter = outputContainer.getPatientAddresses();
        Period period = fhirAddress.getPeriod();
        Date startDate = period.getStart();
        Date endDate = period.getEnd();
        Long addressType;
        if (fhirAddress.getType() != null && StringUtils.isNumeric(fhirAddress.getType().toCode())) {
            addressType = Long.parseLong(fhirAddress.getType().toCode());
        } else {
            addressType = -1L;
        }
        // For referential integrity we need to write the address first so the patientAddress can refer to it
        org.endeavourhealth.transform.pcr.outputModels.Address addressWriter = outputContainer.getAddresses();
        List<StringType> addressList = fhirAddress.getLine();
        String al1 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 0);
        String al2 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 1);
        String al3 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 2);
        String al4 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 3);
        String postcode = fhirAddress.getPostalCode();
        //TODO get uprn (OS ref) and approximation. See TODO in Address outputModel
        Long propertyTypeId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        //TODO IMClient.getOrCreateConceptId("Address.AddressUse." + fhirAddress.getUse().toCode());
        addressWriter.writeUpsert(addressId, al1, al2, al3, al4, postcode,
                null, null, propertyTypeId);
        patientAddressWriter.writeUpsert(null,
                patientId,
                addressType,
                addressId,
                startDate,
                endDate,
                enteredByPractitionerId
        );


    }

    private void writeContact(Patient.ContactComponent cc, long patientId,
                              long enteredByPractitionerId, AbstractPcrCsvWriter csvWriter) throws Exception {
        PatientContact contactWriter = (PatientContact) csvWriter;
        List<ContactPoint> cpList = cc.getTelecom();
        for (ContactPoint cp : cpList) {
            String code = cp.getUse().toCode();
            Long type = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //TODO IMClient.getOrCreateConceptId("ContactPoint.ContactPointSystem."+ cp.getUse().toCode());
            contactWriter.writeUpsert(Long.parseLong(cp.getId()), patientId, type, code, enteredByPractitionerId);
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

    private static String removeParen(String in) {
        String ret = in.replace("[", "");
        ret = ret.replace("]", "");
        return ret;
    }

}
