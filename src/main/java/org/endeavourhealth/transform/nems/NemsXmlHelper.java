package org.endeavourhealth.transform.nems;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.adastra.xml.schema.CodedItem;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.nems.schema.v1.*;
import org.hl7.fhir.instance.model.*;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBElement;
import javax.xml.datatype.XMLGregorianCalendar;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static org.endeavourhealth.common.ods.OdsWebService.lookupOrganisationViaRest;
import static org.endeavourhealth.transform.nems.DataCache.*;

public class NemsXmlHelper implements HasServiceSystemAndExchangeIdI{

    private final Logger LOG = LoggerFactory.getLogger(NemsXmlHelper.class);

    private final ParserPool PARSER_POOL = new ParserPool();
    private final String ID_DELIMITER = ":";
    private final int URN_UUID = 9;
    public final String FILE_ID_BIRTH_NOTIFICATION = "pds-birth-notification-1";
    public final String FILE_ID_DEATH_NOTIFICATION = "pds-death-notification-1";

    public final String NEMS_BIRTH_UUID_TO_NHS_NO = "NEMSBirthUUIDToNHSNo";
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private ReentrantLock cacheLock = new ReentrantLock();
    private UUID serviceId, exchangeId, systemId;
    private static boolean validBatch = false;
    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private Map<String, String> internalIdMapCache = new HashMap<>(); //contains nulls so a regular map but uses the cacheLock
    private Map<StringMemorySaver, StringMemorySaver> hmInternalIdMapCache = new ConcurrentHashMap<>();

    @Override
    public UUID getServiceId() {
        return serviceId;
    }
    @Override
    public UUID getSystemId() {
        return systemId;
    }

    @Override
    public UUID getExchangeId() {
        return exchangeId;
    }

    public void setServiceId(UUID id) {
        serviceId = id;
    }

    public void setExchangeId(UUID id) {
        exchangeId = id;
    }

    public void setSystemId(UUID id) {
        systemId = id;
    }

    public String createUniqueId(String id, ResourceType code) throws TransformException {
        if (code == ResourceType.Patient && !id.contains(":PAT")) {
            return id + ID_DELIMITER + "PAT";
        } else if (code == ResourceType.Organization && !id.contains(":ORG")) {
            return id + ID_DELIMITER + "ORG";
        } else if (code == ResourceType.Observation && !id.contains(":OBS")) {
            return id + ID_DELIMITER + "OBS";
        } else if (code == ResourceType.EpisodeOfCare && !id.contains(":EOC")) {
            return id + ID_DELIMITER + "EOC";
        } else if (code == ResourceType.Practitioner && !id.contains(":PRA")) {
            return id + ID_DELIMITER + "PRA";
        } else if (code == ResourceType.Encounter && !id.contains(":ENC")) {
            return id + ID_DELIMITER + "ENC";
        } else {
            throw new TransformException("Invalid Resource Type passed into the Unique ID Creator: " + code.name());
        }
    }

    public String purifyUUID(String id){
        if(id.contains("urn:uuid:")) {
            return id.substring(URN_UUID);
        }
        return id;
    }

    public Reference createOrganisationReference(String id) {
        return ReferenceHelper.createReference(ResourceType.Organization, id);
    }

    public Reference createLocationReference(String id) {
        return ReferenceHelper.createReference(ResourceType.Location, id);
    }

    public Reference createPatientReference(String id) {
        return ReferenceHelper.createReference(ResourceType.Patient, id);
    }

    public Reference createPractitionerReference(String id) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, id);
    }

    public Reference createEpisodeReference(String id) {
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, id);
    }

    public Reference createEncounterReference(String id) {
        return ReferenceHelper.createReference(ResourceType.Encounter, id);
    }

    public Object retrieveResource(String locallyUniqueId, ResourceType resourceType) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(getServiceId(), resourceType, locallyUniqueId);
        if (globallyUniqueId == null) {
            return null;
        }

        UUID serviceId = getServiceId();
        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), globallyUniqueId);
        if (resourceHistory == null) {
            return null;
        }

        if (resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        return PARSER_POOL.parse(json);
    }

    public void saveInternalId(String idType, String sourceId, String destinationId) throws Exception {
        String cacheKey = idType + "|" + sourceId;

        internalIdDal.save(serviceId, idType, sourceId, destinationId);

        if (internalIdMapCache.containsKey(cacheKey)) {
            internalIdMapCache.replace(cacheKey, destinationId);
        } else {
            internalIdMapCache.put(cacheKey, destinationId);
        }
    }
    public String getInternalId(String idType, String sourceId) throws Exception {
        StringMemorySaver cacheKey = new StringMemorySaver(idType + "|" + sourceId);
        StringMemorySaver cached = hmInternalIdMapCache.get(cacheKey);

        if (cached != null) {
            return cached.toString();
        }

        String ret = internalIdDal.getDestinationId(serviceId, idType, sourceId);

        //add to cache
        if (ret != null) {
            hmInternalIdMapCache.put(cacheKey, new StringMemorySaver(ret));
        }

        return ret;
    }

    public NhsNumberVerificationStatus convertNhsNumberVeriticationStatus(String status) {

        if (status.equalsIgnoreCase("01")) {
            return NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;

        } else if (status.equalsIgnoreCase("02")) {
            return NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED;

        } else {
            throw new RuntimeException("Unexpected national number status [" + status + "]");
        }

    }

    public void createGender(PatientBuilder patientBuilder, PatientType patientType) throws Exception {
        GenderType genderType = patientType.getGender();

        String gender = "";
        if (null != genderType) {
            gender = genderType.getValue();
        }
        if (!Strings.isNullOrEmpty(gender)) {
            Enumerations.AdministrativeGender fhirGender;
            try {
                fhirGender = convertGender(gender);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            patientBuilder.setGender(fhirGender);
        } else {
            patientBuilder.setGender(convertGender("unknown"));
        }
    }

    public void createDOB(PatientBuilder patientBuilder, BirthDateType dobType, boolean isBaby) throws Exception {
        if (!dobType.getValue().isEmpty()) {
            String dateFormat = "yyyy-MM-dd";
            SimpleDateFormat dobFormatter = new SimpleDateFormat(dateFormat);
            Date dob = dobFormatter.parse(dobType.getValue());
            if(isBaby) {
                birthDate = dob;
            }
            patientBuilder.setDateOfBirth(dob);
        }
    }

    public void createDOD(PatientBuilder patientBuilder, DeceasedDateTimeType dodType) throws Exception {
        if(null != dodType) {
            if (!dodType.getValue().isEmpty()) {
                String dateFormat = "yyyy-MM-dd";
                SimpleDateFormat dobFormatter = new SimpleDateFormat(dateFormat);
                Date dod = dobFormatter.parse(dodType.getValue());
                birthDate = dod;
                patientBuilder.setDateOfDeathBoolean(true);
                patientBuilder.setDateOfDeath(dod);
            }
        } else {
            patientBuilder.clearDateOfDeath();
        }
    }

    public void createAddress(PatientBuilder patientBuilder, AddressType address) throws Exception {
        List<LineType> lines = address.getLine();
        String line1 = "";
        String line2 = "";
        if (lines.size() >= 1) {
            line1 = lines.get(0).getValue();
            if(lines.size() >= 2){
                line2 = lines.get(1).getValue();
            }
        }
        String postcode = address.getPostalCode().getValue().replaceAll("\\s", "");

        String type = address.getUse().getValue();

        if (!line1.isEmpty()
                || !line2.isEmpty()
                || !postcode.isEmpty()) {

            AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
            addressBuilder.setUse(Address.AddressUse.fromCode(type));
            addressBuilder.addLine(line1);
            addressBuilder.addLine(line2);
            addressBuilder.setPostcode(postcode);
        }
    }

    public void createIdentifier(PatientBuilder patientBuilder, Identifier.IdentifierUse use, String status, IdentifierType identifier) throws Exception {
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
        identifierBuilder.setUse(use);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
        identifierBuilder.setValue(identifier.getValue().getValue());

        patientBuilder.setNhsNumberVerificationStatus(convertNhsNumberVeriticationStatus(status));
        patientBuilder.setActive(true);
    }

    public void createName(PatientBuilder patientBuilder, NameType patientName) throws Exception {
        FamilyType familyName = null;
        GivenType givenName = null;

        JAXBElement<?> family = (JAXBElement<?>) patientName.getContent().get(3);
        if (family.getValue() instanceof FamilyType) {
            familyName = (FamilyType) family.getValue();
        } else {
            throw new TransformException("Family Name could not be found for patient: " + patientBuilder.getResourceId());
        }
        JAXBElement<?> given = (JAXBElement<?>) patientName.getContent().get(5);
        if (given.getValue() instanceof GivenType) {
            givenName = (GivenType) given.getValue();
        } else {
            throw new TransformException("Given Name could not be found for patient: " + patientBuilder.getResourceId());
        }

        if (patientBuilder.getNames().isEmpty()) {
            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            nameBuilder.addGiven(givenName.getValue());
            nameBuilder.addFamily(familyName.getValue());
        }
    }

    public Enumerations.AdministrativeGender convertGender(String gender) throws Exception {
        Enumerations.AdministrativeGender g = Enumerations.AdministrativeGender.fromCode(gender.toLowerCase());
        if (g.toCode().equals("?")) {
            throw new TransformException("Unexpected gender [" + gender + "]");
        }

        return g;
    }

    public EthnicCategory convertEthnicCategory(String aliasNhsCdAlias) {
        //except for 99 or Unknown, which means "not stated"
        if (aliasNhsCdAlias.equalsIgnoreCase("99")
                || aliasNhsCdAlias.equalsIgnoreCase("Unknown")) {
            return EthnicCategory.NOT_STATED;

        } else {
            if(aliasNhsCdAlias.length() > 1){
                aliasNhsCdAlias = String.valueOf(aliasNhsCdAlias.charAt(0));
            }
            EthnicCategory[] ethnicCategories = EthnicCategory.values();
            for(EthnicCategory e : ethnicCategories){
                if(aliasNhsCdAlias.equalsIgnoreCase(e.getCode())){
                    return EthnicCategory.fromCode(aliasNhsCdAlias);
                }
            }
        }
        return EthnicCategory.NOT_STATED;
    }

    public ExtensionType findPatientExtension(PatientType patient, String extensionURL)
    {
        //loop through patient extensions and match on URL
        if (null != patient.getExtensions()) {
            for (int i=0; i<patient.getExtensions().size(); i++) {
                ExtensionType extension = patient.getExtensions().get(i);
                if (extension.getUrl().equals(extensionURL)) {
                    return extension;
                }
            }
        }
        return null;
    }

    public static void validateBatch(boolean valid){
        validBatch = valid;
    }

    public static boolean getValidBatch() {
        return validBatch;
    }
}