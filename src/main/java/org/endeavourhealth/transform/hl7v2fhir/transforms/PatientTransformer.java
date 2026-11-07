package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.segment.MRG;
import ca.uhn.hl7v2.model.v23.segment.PID;
import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.Religion;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.ResourceMergeMapHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.endeavourhealth.transform.fhirhl7v2.FhirHl7v2Filer;
import org.endeavourhealth.transform.hl7v2fhir.helpers.AcuteHL7Helper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class PatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    private static final ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    /**
     *
     * @param pid
     * @param patientBuilder
     * @param fhirResourceFiler
     * @param acuteHL7Helper
     * @return
     * @throws Exception
     */
    public static PatientBuilder transformPIDToPatient(PID pid, PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, AcuteHL7Helper acuteHL7Helper, String msgType, String odsCode, String msgId) throws Exception {
        String id = AcuteHL7Helper.getPatientGUID(pid);
        String nhsNumber = AcuteHL7Helper.getPatientNHSNumber(pid);
        String idURL="https://fhir.hl7.org.uk/Id/"+odsCode.toLowerCase();
        createIdentifier(patientBuilder, fhirResourceFiler, nhsNumber, Identifier.IdentifierUse.OFFICIAL, "http://fhir.nhs.net/Id/nhs-number");

        //store the patient ID to the patient resource
        createIdentifier(patientBuilder, fhirResourceFiler, id, Identifier.IdentifierUse.SECONDARY, idURL);

        TS dob = pid.getDateOfBirth();
        if (!dob.isEmpty()) {
            String dtB = String.valueOf(dob.getTimeOfAnEvent());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(dtB.substring(0,4)+"-"+dtB.substring(4,6)+"-"+dtB.substring(6,8));
            patientBuilder.setDateOfBirth(date);
        }

        TS dod = pid.getPatientDeathDateAndTime();
        if (!dod.isEmpty() && !(dod.getTimeOfAnEvent().toString().endsWith("\"") && dod.getTimeOfAnEvent().toString().startsWith("\""))) {
            String dtD = String.valueOf(dod.getTimeOfAnEvent());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(dtD.substring(0,4)+"-"+dtD.substring(4,6)+"-"+dtD.substring(6,8));
            patientBuilder.setDateOfDeath(date);
        } else {
            patientBuilder.clearDateOfDeath();
        }

        IS sex = pid.getSex();
        if (!Strings.isNullOrEmpty(pid.getSex().getValue()) && !pid.getSex().getValue().equalsIgnoreCase("\"\"")) {
            String receivedGender = String.valueOf(sex);
            if(!(receivedGender.equals(String.valueOf('U')) || receivedGender.equals(String.valueOf('M')) || receivedGender.equals(String.valueOf('F')) || receivedGender.equals(String.valueOf('I')))){
                String alert_message ="Received Invalid Gender type : " +receivedGender + " for Organization = " + odsCode + " ,Message ID = " + msgId +" ,Exchange ID = "+ acuteHL7Helper.getExchangeId().toString() + " and message type = " + msgType;
                TransformWarnings.log(LOG,acuteHL7Helper,
                        "Received Invalid Gender for message type={}, Segment={}, MRN={}, MessageID={}",
                        msgType,alert_message, AcuteHL7Helper.getPatientGUID(pid),msgId);

                LOG.debug(alert_message);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, alert_message);
                receivedGender= String.valueOf('U');
            }
            VocSex sexEnum = VocSex.fromValue(receivedGender);
            Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(sexEnum);
            patientBuilder.setGender(gender);
        }
        String ethnicity = pid.getEthnicGroup().getValue();
        addEthnicity(ethnicity, patientBuilder, msgType, acuteHL7Helper, odsCode, msgId,pid);

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
        contactPointBuilder.removeExistingContactPoints(patientBuilder);
        XTN[] phones = pid.getPhoneNumberHome();
        addPhoneData(phones, patientBuilder, fhirResourceFiler, acuteHL7Helper);

        XTN[] businessPhones = pid.getPhoneNumberBusiness();
        addPhoneData(businessPhones, patientBuilder, fhirResourceFiler, acuteHL7Helper);

        createName(patientBuilder, pid, fhirResourceFiler);
        createAddress(patientBuilder, pid, fhirResourceFiler);

        //clear all care provider records, before we start adding more
        patientBuilder.clearCareProvider();

        return patientBuilder;
    }
    /*
     * Add ethnicity to Patient resource as an extension
     */
    private static void addEthnicity(String ethnicity, PatientBuilder patientBuilder, String msgType,
                                     AcuteHL7Helper acuteHL7Helper, String odsCode, String msgId,PID pid) throws Exception {

        if (!Strings.isNullOrEmpty(ethnicity) && !ethnicity.equalsIgnoreCase("\"\"")) {
            MapResponse valueResponse = acuteHL7Helper.retrieveIMLookupMap(odsCode, "ethnicity", ethnicity);
            if (valueResponse.getWasCreated()) {
                acuteHL7Helper.raiseMissingAlert(ethnicity, "ethnicity", odsCode, msgType, acuteHL7Helper.getExchangeId().toString(),msgId,AcuteHL7Helper.getPatientGUID(pid),acuteHL7Helper);
            }
            ExtensionConverter.removeExtension(patientBuilder.getResource(),FhirExtensionUri.PATIENT_ETHNICITY);
            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme()).setDisplay(ethnicity);

            Extension extension = ExtensionConverter.createExtension( FhirExtensionUri.PATIENT_ETHNICITY, ccValue) ;
            patientBuilder.getResource().addExtension(extension);
        }
    }

    private static void addPhoneData(XTN[] phones, PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, AcuteHL7Helper acuteHL7Helper) throws Exception {
        for(XTN phone : phones) {
            ID useCd = phone.getTelecommunicationUseCode();
            //  String phoneString = String.valueOf(phone);
            String phoneNumber=phone.get9999999X99999CAnyText().toString();
            if("PRN".equalsIgnoreCase(String.valueOf(useCd))) {
                if ( phoneNumber !=null && !phoneNumber.isEmpty() ) {
                    createContact(patientBuilder, fhirResourceFiler, acuteHL7Helper, phoneNumber, ContactPoint.ContactPointUse.HOME, ContactPoint.ContactPointSystem.PHONE);
                }

            } else if("PRS".equalsIgnoreCase(String.valueOf(useCd))) {
                if (phoneNumber !=null && !phoneNumber.isEmpty()) {
                    createContact(patientBuilder, fhirResourceFiler, acuteHL7Helper, phoneNumber, ContactPoint.ContactPointUse.MOBILE, ContactPoint.ContactPointSystem.PHONE);
                }
            }
            else if("WPN".equalsIgnoreCase(String.valueOf(useCd))) {
                if (phoneNumber !=null && !phoneNumber.isEmpty()) {
                    createContact(patientBuilder, fhirResourceFiler, acuteHL7Helper, phoneNumber, ContactPoint.ContactPointUse.WORK, ContactPoint.ContactPointSystem.PHONE);
                }
            }
        }
    }

    /**
     * A34 messages merge all content from one patient (minor patient) to another (the major patient)
     */
    public static void performA34PatientMerge(FhirHl7v2Filer.AdtResourceFiler filer, PID pid, MRG mrg, AcuteHL7Helper acuteHL7Helper,boolean existingPatient) throws Exception {

        String majorPId = AcuteHL7Helper.getPatientGUID(pid);
        String minorPId = AcuteHL7Helper.getMergeMRN(mrg,"MRN");

        UUID majPatientId = IdHelper.getEdsResourceId(filer.getServiceId(), ResourceType.Patient, majorPId);
        String majorPatientId = String.valueOf(majPatientId);

        UUID minPatientId = IdHelper.getEdsResourceId(filer.getServiceId(), ResourceType.Patient, minorPId);
        String minorPatientId = String.valueOf(minPatientId);

        LOG.debug("Doing A34 merge from minor patient " + minorPatientId + " to major patient " + majorPatientId);

        Map<String, String> idMappings = new HashMap<>();

        //add the minor and major patient IDs to the ID map, so we change the patient references in our resources too
        String majorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, majorPatientId);
        String minorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, minorPatientId);
        idMappings.put(minorPatientReference, majorPatientReference);

        Patient existingMajorPatient = null;
        existingMajorPatient = (Patient) acuteHL7Helper.retrieveResource(majorPId, ResourceType.Patient);

        Patient existingMinorPatient = null;
        existingMinorPatient = (Patient) acuteHL7Helper.retrieveResource(minorPId, ResourceType.Patient);

        if((existingMajorPatient != null || existingPatient) && existingMinorPatient != null) {
            List<ResourceWrapper> minorPatientResources = resourceRepository.getResourcesByPatient(filer.getServiceId(), UUID.fromString(minorPatientId));

            //copy the resources to the major patient
            for (ResourceWrapper minorPatientResource: minorPatientResources) {

                String json = minorPatientResource.getResourceData();
                Resource fhirOriginal = ParserPool.getInstance().parse(json);

                if (fhirOriginal instanceof Patient) {
                    //we don't want to move patient resources, so just delete it
                    filer.deletePatientResource(new GenericBuilder(fhirOriginal));

                }
                else {
                    //for all other resources, re-map the IDs and save to the DB
                    try {
                        //FHIR copy functions don't copy the ID or Meta, so deserialise twice instead
                        IdHelper.applyExternalReferenceMappings(fhirOriginal, idMappings, false);
                        filer.savePatientResource(new GenericBuilder(fhirOriginal));

                    } catch (Exception ex) {
                        throw new Exception("Failed to save amended " + minorPatientResource.getResourceType() + " which originally had ID " + minorPatientResource.getResourceId() + " and now has " + fhirOriginal.getId(), ex);
                    }

                    LOG.debug("Moved " + fhirOriginal.getResourceType() + " " + fhirOriginal.getId());
                }
            }

            //need to wait here until all resources have been saved, otherwise if we let the below fn save the resource
            //mappings (of old patient to new patient) then the mapping gets applied when deleting the old patient (since
            //the delete is in a different thread). End result is that we end up KEEPING the minor patient and DELETING
            //the one we want to keep!
            filer.waitUntilEverythingIsSaved();

            //save these resource mappings for the future
            ResourceMergeMapHelper.saveResourceMergeMapping(filer.getServiceId(), idMappings);
        }
    }

    private static Parameters findParameters(Bundle bundle) throws Exception {
        for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
            Resource resource = entry.getResource();
            if (resource.getResourceType() == ResourceType.Parameters) {
                return (Parameters)resource;
            }
        }

        throw new TransformException("Failed to find Parameters resource in Bundle");
    }

    private static String findParameterValue(Parameters parameters, String name) throws Exception {
        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase(name)) {
                    StringType value = (StringType)component.getValue();
                    return value.getValue();
                }
            }
        }

        throw new TransformException("Failed to find parameter [" + name + "] in Parameters resource");
    }

    //returns a map of old Ids to new, formatted as FHIR references (e.g. Patient/<guid>)
    private static Map<String, String> findIdMappings(Parameters parameters) throws Exception {

        Map<String, String> referenceIdMap = new HashMap<>();

        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase("OldToNewResourceMap")) {

                    for (Parameters.ParametersParameterComponent part: component.getPart()) {
                        String name = part.getName();
                        String value = ((StringType)part.getValue()).toString();

                        referenceIdMap.put(name, value);
                    }
                }
            }
        }

        LOG.debug("Id mappings are");
        for (String key: referenceIdMap.keySet()) {
            String value = referenceIdMap.get(key);
            LOG.debug(key + " -> " + value);
        }

        return referenceIdMap;
    }

    /**
     *
     * @param patientBuilder
     * @param fhirResourceFiler
     * @param cell
     * @param use
     * @param system
     * @throws Exception
     */
    private static void createIdentifier(PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, String cell,
                                         Identifier.IdentifierUse use, String system) throws Exception {
        if (null!=cell && !cell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(use);
            identifierBuilder.setSystem(system);
            identifierBuilder.setValue(cell);

            //make sure to de-duplicate the identifier if we've just added a new instance with the same value
            IdentifierBuilder.deDuplicateLastIdentifier(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            //if cell is empty, make sure to end any previous records
            IdentifierBuilder.endIdentifiers(patientBuilder, fhirResourceFiler.getDataDate(), system, use);
        }
    }

    /**
     *
     * @param patientBuilder
     * @param pid
     * @param fhirResourceFiler
     * @throws Exception
     */
    private static void createAddress(PatientBuilder patientBuilder, PID pid, FhirResourceFiler fhirResourceFiler) throws Exception {

        XAD[] patientAddress = pid.getPatientAddress();
        if (patientAddress != null && patientAddress.length > 0) {

            ST streetAddress = patientAddress[0].getStreetAddress();
            ST city = patientAddress[0].getCity();
            ST state = patientAddress[0].getStateOrProvince();
            ST postcode = patientAddress[0].getZipOrPostalCode();
            ID country = patientAddress[0].getCountry();
            ST otherdesignation = patientAddress[0].getOtherDesignation();
            ST otherGeoDesignation = patientAddress[0].getOtherGeographicDesignation();

            if (!streetAddress.isEmpty()
                    || !city.isEmpty()
                    || !state.isEmpty()
                    || !postcode.isEmpty()
                    || !country.isEmpty()) {

                AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
                addressBuilder.setUse(Address.AddressUse.HOME);
                addressBuilder.addLine(String.valueOf(streetAddress)+","+String.valueOf(otherdesignation)+","+String.valueOf(otherGeoDesignation));
                addressBuilder.addLine(String.valueOf(country));
                addressBuilder.setCity(String.valueOf(city));
                if(state != null && !state.isEmpty()) {
                    addressBuilder.setDistrict(String.valueOf(state));
                }
                addressBuilder.setPostcode(String.valueOf(postcode));

                AddressBuilder.deDuplicateLastAddress(patientBuilder, fhirResourceFiler.getDataDate());

            } else {
                AddressBuilder.endAddresses(patientBuilder, fhirResourceFiler.getDataDate(), Address.AddressUse.HOME);
            }
        }
    }

    /**
     *
     * @param patientBuilder
     * @param pid
     * @param fhirResourceFiler
     * @throws Exception
     */
    private static void createName(PatientBuilder patientBuilder, PID pid, FhirResourceFiler fhirResourceFiler) throws Exception {
        XPN[] patientName = pid.getPatientName();
        ST familyName = patientName[0].getFamilyName();
        ST middleName = patientName[0].getMiddleInitialOrName();
        ST givenName = patientName[0].getGivenName();
        ID nameTypeCode = patientName[0].getNameTypeCode();
        ST prefix = patientName[0].getPrefixEgDR();

        if (!prefix.isEmpty() || !givenName.isEmpty() || !familyName.isEmpty()) {
            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            nameBuilder.addPrefix(String.valueOf(prefix));
            nameBuilder.addGiven(String.valueOf(givenName));
            nameBuilder.addGiven(String.valueOf(middleName));
            nameBuilder.addFamily(String.valueOf(familyName));

            NameBuilder.deDuplicateLastName(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            NameBuilder.endNames(patientBuilder, fhirResourceFiler.getDataDate(), HumanName.NameUse.OFFICIAL);
        }
    }

    /**
     *
     * @param patientBuilder
     * @param fhirResourceFiler
     * @param acuteHL7Helper
     * @param cell
     * @param use
     * @param system
     * @throws Exception
     */
    private static void createContact(PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, AcuteHL7Helper acuteHL7Helper, String cell,
                                      ContactPoint.ContactPointUse use, ContactPoint.ContactPointSystem system) throws Exception {

        if (!cell.isEmpty()) {
            for (ContactPoint cp :patientBuilder.getContactPoint()) {
                if  (cell.equalsIgnoreCase(cp.getValue())) {
                    return;
                }
            }
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(use);
            contactPointBuilder.setSystem(system);
            contactPointBuilder.setValue(cell);
        }
    }

    private static Religion mapReligion(CernerCodeValueRef cvref) throws Exception {
        String s = cvref.getAliasNhsCdAlias();

        switch (s) {
            case "BAPTIST":
                return Religion.BAPTIST;
            case "BUDDHISM":
                return Religion.BUDDHIST;
            case "ROMANCATHOLIC":
                return Religion.ROMAN_CATHOLIC;
            case "HINDU":
                return Religion.HINDU;
            case "JEHOVAHSWITNESS":
                return Religion.JEHOVAHS_WITNESS;
            case "LUTHERAN":
                return Religion.LUTHERAN;
            case "METHODIST":
                return Religion.METHODIST;
            case "ISLAM":
                return Religion.MUSLIM;
            case "SEVENTHDAYADVENTIST":
                return Religion.SEVENTH_DAY_ADVENTIST;
            case "OTHER":
                return Religion.RELIGION_OTHER;
            case "JUDAISMJEWISHHEBREW":
                return Religion.JEWISH;
            case "PRESBYTERIAN":
                return Religion.PRESBYTERIAN;
            case "NAZARENE":
                return Religion.NAZARENE_CHURCH;
            case "LATTERDAYSAINTS":
                return Religion.MORMON;
            case "GREEKORTHODOX":
                return Religion.GREEK_ORTHODOX;
            case "PENTECOSTAL":
                return Religion.PENTECOSTALIST;
            case "CHURCHOFENGLAND":
                return Religion.CHURCH_OF_ENGLAND;
            case "ETHIOPIANORTHODOXRASTAFARIAN":
                return Religion.RASTAFARI;
            case "SIKH":
                return Religion.SIKH;
            case "PLYMOUTHBRETHREN":
                return Religion.PLYMOUTH_BRETHREN;
            case "CHRISTIANCHURCH":
                return Religion.CHRISTIAN;
            case "CHURCHOFGOD":
                return Religion.CHURCH_OF_GOD_OF_PROPHECY;
            case "ASSEMBLIESOFGOD":
                return null;
            case "CHURCHOFCHRIST":
                return null;
            case "EPISCOPAL":
                return null;
            default:
                throw new Exception("Unmapped religion " + s);
        }
    }

    public static PatientBuilder transformPatient(AcuteHL7Helper acuteHL7Helper, PractitionerBuilder practitionerBuilderDemographic, OrganizationBuilder organizationBuilder, OrganizationBuilder organizationBuilderDemographic,
                                                  FhirResourceFiler fhirResourceFiler, PID pid, String msgType, String odsCode, String msgId) throws Exception {
        //Patient
        PatientBuilder patientBuilder = null;
        String patientGuid = AcuteHL7Helper.getPatientGUID(pid);
        boolean newPatient = false;
        Patient existingPatient = null;
        existingPatient = (Patient) acuteHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
        if (existingPatient != null) {
            patientBuilder = new PatientBuilder(existingPatient);
        } else {
            patientBuilder = new PatientBuilder();
            acuteHL7Helper.setUniqueId(patientBuilder, patientGuid, null);
            newPatient = true;
        }
        patientBuilder = PatientTransformer.transformPIDToPatient(pid, patientBuilder, fhirResourceFiler, acuteHL7Helper, msgType, odsCode, msgId);
        Reference practitionerAddCareProviderReference = null;
        if (newPatient) {
            patientBuilder.setManagingOrganisation(acuteHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            if (practitionerBuilderDemographic.getResourceId()!=null) {
                practitionerAddCareProviderReference = acuteHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
                patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            }
            if (organizationBuilderDemographic.getResourceId()!=null) {
                patientBuilder.addCareProvider(acuteHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));
            }
            //add in additional extended data as Parameters resource with additional extension
            PractitionerTransformer.addPractitioner(patientBuilder, practitionerAddCareProviderReference);
            UUID EdsId = acuteHL7Helper.mapLocalIdToEDSId(patientBuilder.getResource().getId(),ResourceType.Patient);
            acuteHL7Helper.MapRemainingSourceIdsWithEDSId(AcuteHL7Helper.getRemainingSourceids(pid,"Patient"),String.valueOf(EdsId),ResourceType.Patient);
            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        } else {
            Reference organisationReference = acuteHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, acuteHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);
            if (practitionerBuilderDemographic.getResourceId()!=null) {
                practitionerAddCareProviderReference = acuteHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
                practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, acuteHL7Helper);
                patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            }
            if (organizationBuilderDemographic.getResourceId()!=null) {
                Reference OrgAddCareProviderReference = acuteHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
                OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, acuteHL7Helper);
                patientBuilder.addCareProvider(OrgAddCareProviderReference);
            }
            //add in additional extended data as Parameters resource with additional extension
            PractitionerTransformer.addPractitioner(patientBuilder, practitionerAddCareProviderReference);
            acuteHL7Helper.MapRemainingSourceIdsWithEDSId(AcuteHL7Helper.getRemainingSourceids(pid,"Patient"),patientBuilder.getResource().getId(),ResourceType.Patient);
            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        return patientBuilder;
    }

    /* public static MessageHeader transformPIDToMsgHeader(MSH msh) throws Exception {
        MessageHeader messageHeader = new MessageHeader();
        HD sendingApplication = msh.getSendingApplication();
        HD sendingFacility = msh.getSendingFacility();
        HD receivingApplication = msh.getReceivingApplication();
        HD receivingFacility = msh.getReceivingFacility();
        TS dateTime = msh.getDateTimeOfMessage();
        CM_MSG msgType = msh.getMessageType();
        ST msgCtrlId = msh.getMessageControlID();
        ID versionId = msh.getVersionID();
        String msgTrigger = msh.getMessageType().getTriggerEvent().getValue();
        messageHeader.getSource().setSoftware(String.valueOf(sendingApplication));
        messageHeader.getSource().setName(String.valueOf(sendingFacility));
        //messageHeader.getSource().addExtension().setValue((IBaseDatatype) receivingApplication);
        messageHeader.addDestination().setName(String.valueOf(receivingFacility));
        //Date dt = dateTime;
        //messageHeader.setTimestamp(dateTime);
        messageHeader.getEvent().setCode(String.valueOf(msgType));
        //messageHeader.addExtension().setValue((IBaseDatatype) msgCtrlId);
        messageHeader.getEvent().setVersion(String.valueOf(versionId));
        return messageHeader;
    } */
}
