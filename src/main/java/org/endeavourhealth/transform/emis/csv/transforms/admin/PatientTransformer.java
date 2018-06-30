package org.endeavourhealth.transform.emis.csv.transforms.admin;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.*;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.CodeAndDate;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Patient;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.TransformException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Patient.class);
        while (parser.nextRecord()) {

            try {
                createResource((Patient) parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(Patient parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper,
                                      String version) throws Exception {

        //this transform creates two resources
        PatientBuilder patientBuilder = new PatientBuilder();
        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();

        CsvCell patientGuid = parser.getPatientGuid();
        CsvCell organisationGuid = parser.getOrganisationGuid();

        EmisCsvHelper.setUniqueId(patientBuilder, patientGuid, null);
        EmisCsvHelper.setUniqueId(episodeBuilder, patientGuid, null); //use the patient GUID as the ID for the episode

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        episodeBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            //Emis send us a delete for a patient WITHOUT a corresponding delete for all other data, so
            //we need to manually delete all dependant resources
            deleteEntirePatientRecord(fhirResourceFiler, csvHelper, parser.getCurrentState(), patientBuilder, episodeBuilder);
            return;
        }

        CsvCell nhsNumber = parser.getNhsNumber();
        if (!nhsNumber.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setValue(nhsNumber.getString(), nhsNumber);
        }

        //store the patient GUID and patient number to the patient resource
        if (!patientGuid.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_PATIENT_GUID);
            identifierBuilder.setValue(patientGuid.getString(), patientGuid);
        }

        CsvCell patientNumber = parser.getPatientNumber();
        if (!patientNumber.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_PATIENT_NUMBER);
            identifierBuilder.setValue(patientNumber.getString(), patientNumber);
        }

        CsvCell dob = parser.getDateOfBirth();
        patientBuilder.setDateOfBirth(dob.getDate(), dob);

        CsvCell dod = parser.getDateOfDeath();
        if (!dod.isEmpty()) {
            patientBuilder.setDateOfDeath(dod.getDate(), dod);
        }

        //EMIS provides "sex" and FHIR requires "gender", but will treat as the same concept for this transformation
        CsvCell sex = parser.getSex();
        VocSex sexEnum = VocSex.fromValue(sex.getString());
        Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(sexEnum);
        patientBuilder.setGender(gender, sex);

        CsvCell title = parser.getTitle();
        CsvCell givenName = parser.getGivenName();
        CsvCell middleNames = parser.getMiddleNames();
        CsvCell surname = parser.getSurname();

        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addPrefix(title.getString(), title);
        nameBuilder.addGiven(givenName.getString(), givenName);
        nameBuilder.addGiven(middleNames.getString(), middleNames);
        nameBuilder.addFamily(surname.getString(), surname);

        //we need to know the registration type to work out the address use
        CsvCell patientType = parser.getPatientTypedescription();
        CsvCell dummyType = parser.getDummyType();
        RegistrationType registrationType = convertRegistrationType(patientType.getString(), dummyType.getBoolean(), parser);

        CsvCell houseNameFlat = parser.getHouseNameFlatNumber();
        CsvCell numberAndStreet = parser.getNumberAndStreet();
        CsvCell village = parser.getVillage();
        CsvCell town = parser.getTown();
        CsvCell county = parser.getCounty();
        CsvCell postcode = parser.getPostcode();

        //apparently if the patient is a temp patient, then the address supplied will be the temporary address,
        //rather than home. Emis Web stores the home address for these patients in a table we don't get in the extract
        //Address.AddressUse use = Address.AddressUse.HOME;
        Address.AddressUse use = null;
        if (registrationType == RegistrationType.TEMPORARY) {
            use = Address.AddressUse.TEMP;
        } else {
            use = Address.AddressUse.HOME;
        }

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setUse(use);
        addressBuilder.addLine(houseNameFlat.getString(), houseNameFlat);
        addressBuilder.addLine(numberAndStreet.getString(), numberAndStreet);
        addressBuilder.addLine(village.getString(), village);
        addressBuilder.setTown(town.getString(), town);
        addressBuilder.setDistrict(county.getString(), county);
        addressBuilder.setPostcode(postcode.getString(), postcode);

        CsvCell residentialInstituteCode = parser.getResidentialInstituteCode();
        if (!residentialInstituteCode.isEmpty()) {
            patientBuilder.setResidentialInstituteCode(residentialInstituteCode.getString(), residentialInstituteCode);
        }

        CsvCell nhsNumberStatus = parser.getNHSNumberStatus();
        if (!nhsNumberStatus.isEmpty()) {

            //convert the String to one of the official statuses. If it can't be converted, insert free-text in the codeable concept
            NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(nhsNumberStatus.getString());
            patientBuilder.setNhsNumberVerificationStatus(verificationStatus, nhsNumberStatus);
        }

        CsvCell homePhone = parser.getHomePhone();
        if (!homePhone.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setValue(homePhone.getString(), homePhone);
        }

        CsvCell mobilePhone = parser.getMobilePhone();
        if (!mobilePhone.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.MOBILE);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setValue(mobilePhone.getString(), mobilePhone);
        }

        CsvCell email = parser.getEmailAddress();
        if (!email.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setValue(email.getString(), email);
        }

        Reference organisationReference = csvHelper.createOrganisationReference(organisationGuid);
        patientBuilder.setManagingOrganisation(organisationReference, organisationGuid);

        //create a second reference, since it's not an immutable object
        organisationReference = csvHelper.createOrganisationReference(organisationGuid);
        episodeBuilder.setManagingOrganisation(organisationReference, organisationGuid);


        CsvCell carerName = parser.getCarerName();
        CsvCell carerRelationship = parser.getCarerRelation();
        if (!carerName.isEmpty() || !carerRelationship.isEmpty()) {

            //add a new empty contact object to the patient which the following lines will populate
            PatientContactBuilder contactBuilder = new PatientContactBuilder(patientBuilder);

            if (!carerName.isEmpty()) {
                HumanName humanName = NameConverter.convert(carerName.getString());
                contactBuilder.addContactName(humanName, carerName);
            }

            if (!carerRelationship.isEmpty()) {
                //FHIR spec states that we should map to their relationship types if possible, but if
                //not possible, then send as a textual codeable concept
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, CodeableConceptBuilder.Tag.Patient_Contact_Relationship);

                try {
                    ContactRelationship fhirContactRelationship = ContactRelationship.fromCode(carerRelationship.getString());

                    codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_CONTACT_RELATIONSHIP);
                    codeableConceptBuilder.setCodingCode(fhirContactRelationship.getCode(), carerRelationship);
                    codeableConceptBuilder.setCodingDisplay(fhirContactRelationship.getDescription());

                } catch (IllegalArgumentException ex) {
                    codeableConceptBuilder.setText(carerRelationship.getString(), carerRelationship);
                }
            }
        }

        CsvCell spineSensitive = parser.getSpineSensitive();
        if (spineSensitive.getBoolean()) {
            patientBuilder.setSpineSensitive(true, spineSensitive);
        }

        episodeBuilder.setRegistrationType(registrationType, patientType, dummyType);

        //HL7 have clarified that the care provider field is for the patient's general practitioner, NOT
        //for the patient's carer at a specific organisation. That being the case, we store the local carer
        //on the episode_of_care and the general practitioner on the patient.
        CsvCell usualGpGuid = parser.getUsualGpUserInRoleGuid();

        //the care manager on the episode is the person who cares for the patient AT THIS ORGANISATION,
        //so ignore the external... fields which refer to clinicians elsewhere
        if (!usualGpGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(usualGpGuid);
            episodeBuilder.setCareManager(practitionerReference, usualGpGuid);
        }

        //the care provider field on the patient is ONLY for the patients usual GP, so only set the Emis usual
        //GP field in it if the patient is a GMS patient, otherwise use the "external" GP fields on the parser
        if (!usualGpGuid.isEmpty()
                && registrationType == RegistrationType.REGULAR_GMS) {

            Reference reference = csvHelper.createPractitionerReference(usualGpGuid);
            patientBuilder.addCareProvider(reference, usualGpGuid);
        }

        CsvCell externalGpGuid = parser.getExternalUsualGPGuid();
        if (!externalGpGuid.isEmpty()) {
            Reference reference = csvHelper.createPractitionerReference(externalGpGuid);
            patientBuilder.addCareProvider(reference, externalGpGuid);

        } else {

            //have to handle the mis-spelling of the column name in EMIS test pack
            //String externalOrgGuid = patientParser.getExternalUsualGPOrganisation();
            CsvCell externalOrgGuid = null;
            if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                    || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
                externalOrgGuid = parser.getExternalUsusalGPOrganisation();
            } else {
                externalOrgGuid = parser.getExternalUsualGPOrganisation();
            }

            if (!externalOrgGuid.isEmpty()) {
                Reference reference = csvHelper.createOrganisationReference(externalOrgGuid);
                patientBuilder.addCareProvider(reference, externalOrgGuid);
            }
        }

        transformEthnicityAndMaritalStatus(patientBuilder, patientGuid, csvHelper, fhirResourceFiler);

        CsvCell regDate = parser.getDateOfRegistration();
        if (!regDate.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(regDate.getDate(), regDate);
        }

        CsvCell dedDate = parser.getDateOfDeactivation();
        if (!dedDate.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(dedDate.getDate(), dedDate);
        }

        boolean active = dedDate.isEmpty() || dedDate.getDate().after(new Date());
        patientBuilder.setActive(active, dedDate);

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            //add the confidential flag to BOTH resources
            patientBuilder.setConfidential(true, confidential);
            episodeBuilder.setConfidential(true, confidential);
        }

        //save both resources together, so the patient is defintiely saved before the episode
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder);
    }

    /*public static void createResource(Patient parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper,
                                       String version) throws Exception {

        //create Patient Resource
        org.hl7.fhir.instance.model.Patient fhirPatient = new org.hl7.fhir.instance.model.Patient();
        fhirPatient.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PATIENT));

        String patientGuid = parser.getPatientGuid();
        String organisationGuid = parser.getOrganisationGuid();

        EmisCsvHelper.setUniqueId(fhirPatient, patientGuid, null);

        //create Episode of Care Resource
        EpisodeOfCare fhirEpisode = new EpisodeOfCare();
        fhirEpisode.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_EPISODE_OF_CARE));

        EmisCsvHelper.setUniqueId(fhirEpisode, patientGuid, null);

        fhirEpisode.setPatient(csvHelper.createPatientReference(patientGuid.toString()));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getDeleted()) {
            //Emis send us a delete for a patient WITHOUT a corresponding delete for all other data, so
            //we need to manually delete all dependant resources
            deleteEntirePatientRecord(fhirResourceFiler, csvHelper, parser.getCurrentState(), patientGuid, fhirPatient, fhirEpisode);
            return;
        }

        String nhsNumber = parser.getNhsNumber();
        if (!Strings.isNullOrEmpty(nhsNumber)) {
            fhirPatient.addIdentifier(IdentifierHelper.createNhsNumberIdentifier(nhsNumber));
        }

        //store the patient GUID and patient number to the patient resource
        int patientNumber = parser.getPatientNumber();
        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_PATIENT_GUID, patientGuid));
        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_PATIENT_NUMBER, "" + patientNumber));

        Date dob = parser.getDateOfBirth();
        fhirPatient.setBirthDate(dob);

        Date dod = parser.getDateOfDeath();
        if (dod != null) {
            //wrong data type
            fhirPatient.setDeceased(new DateTimeType(dod));
            //fhirPatient.setDeceased(new DateType(dod));
        }

        //EMIS only provides sex but FHIR requires gender, but will treat as the same concept
        VocSex vocSex = VocSex.fromValue(parser.getSex());
        Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(vocSex);
        fhirPatient.setGender(gender);

        String title = parser.getTitle();
        String givenName = parser.getGivenName();
        String middleNames = parser.getMiddleNames();
        String surname = parser.getSurname();

        //the test CSV data has at least one patient with no surname, so treat the given name as surname
        if (Strings.isNullOrEmpty(surname)) {
            surname = givenName;
            givenName = "";
        }

        String forenames = (givenName + " " + middleNames).trim();

        List<HumanName> fhirNames = NameConverter.convert(title, forenames, surname, null, null, null);
        if (fhirNames != null) {
            fhirNames.forEach(fhirPatient::addName);
        }

        String houseNameFlat = parser.getHouseNameFlatNumber();
        String numberAndStreet = parser.getNumberAndStreet();
        String village = parser.getVillage();
        String town = parser.getTown();
        String county = parser.getCounty();
        String postcode = parser.getPostcode();

        Address fhirAddress = AddressHelper.createAddress(Address.AddressUse.HOME, houseNameFlat, numberAndStreet, village, town, county, postcode);
        fhirPatient.addAddress(fhirAddress);

        String residentialInstituteCode = parser.getResidentialInstituteCode();
        if (!Strings.isNullOrEmpty(residentialInstituteCode)) {
            fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_RESIDENTIAL_INSTITUTE_CODE, new StringType(residentialInstituteCode)));
        }

        String nhsNumberStatus = parser.getNHSNumberStatus();
        if (!Strings.isNullOrEmpty(nhsNumberStatus)) {
            CodeableConcept fhirCodeableConcept = null;

            //convert the String to one of the official statuses. If it can't be converted, insert free-text in the codeable concept
            NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(nhsNumberStatus);
            if (verificationStatus != null) {
                fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(verificationStatus);

            } else {
                fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(nhsNumberStatus);
            }

            fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS, fhirCodeableConcept));
        }

        String homePhone = parser.getHomePhone();
        ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.HOME, homePhone);
        fhirPatient.addTelecom(fhirContact);

        String mobilePhone = parser.getMobilePhone();
        fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.MOBILE, mobilePhone);
        fhirPatient.addTelecom(fhirContact);

        String email = parser.getEmailAddress();
        fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.EMAIL, ContactPoint.ContactPointUse.HOME, email);
        fhirPatient.addTelecom(fhirContact);

        fhirPatient.setManagingOrganization(csvHelper.createOrganisationReference(organisationGuid));

        String carerName = parser.getCarerName();
        String carerRelationship = parser.getCarerRelation();
        if (!Strings.isNullOrEmpty(carerName)) {

            org.hl7.fhir.instance.model.Patient.ContactComponent fhirContactComponent = new org.hl7.fhir.instance.model.Patient.ContactComponent();
            fhirContactComponent.setName(NameConverter.convert(carerName));

            if (!Strings.isNullOrEmpty(carerRelationship)) {
                //FHIR spec states that we should map to their relationship types if possible, but if
                //not possible, then send as a textual codeable concept
                try {
                    ContactRelationship fhirContactRelationship = ContactRelationship.fromCode(carerRelationship);
                    fhirContactComponent.addRelationship(CodeableConceptHelper.createCodeableConcept(fhirContactRelationship));
                } catch (IllegalArgumentException ex) {
                    fhirContactComponent.addRelationship(CodeableConceptHelper.createCodeableConcept(carerRelationship));
                }
            }

            fhirPatient.addContact(fhirContactComponent);
        }

        boolean spineSensitive = parser.getSpineSensitive();
        if (spineSensitive) {
            fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_SPINE_SENSITIVE, new BooleanType(spineSensitive)));
        }

        RegistrationType registrationType = convertRegistrationType(parser.getPatientTypedescription(), parser.getDummyType());
        //the registration type is a property of a patient's stay at an organisation, so add to that resource instead
        fhirEpisode.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_REGISTRATION_TYPE, CodingHelper.createCoding(registrationType)));
        //fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_REGISTRATION_TYPE, CodingHelper.createCoding(registrationType)));

        //HL7 have clarified that the care provider field is for the patient's general practitioner, NOT
        //for the patient's carer at a specific organisation. That being the case, we store the local carer
        //on the episode_of_care and the general practitioner on the patient.
        String usualGpGuid = parser.getUsualGpUserInRoleGuid();
        if (!Strings.isNullOrEmpty(usualGpGuid)
                && registrationType == RegistrationType.REGULAR_GMS) { //if they're not registered for GMS, then this isn't their usual GP
            fhirPatient.addCareProvider(csvHelper.createPractitionerReference(usualGpGuid));
        }

        String externalGpGuid = parser.getExternalUsualGPGuid();
        if (!Strings.isNullOrEmpty(externalGpGuid)) {
            fhirPatient.addCareProvider(csvHelper.createPractitionerReference(externalGpGuid));

        } else {

            //have to handle the mis-spelling of the column name in EMIS test pack
            //String externalOrgGuid = patientParser.getExternalUsualGPOrganisation();
            String externalOrgGuid = null;
            if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                    || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
                externalOrgGuid = parser.getExternalUsusalGPOrganisation();
            } else {
                externalOrgGuid = parser.getExternalUsualGPOrganisation();
            }

            if (!Strings.isNullOrEmpty(externalOrgGuid)) {
                fhirPatient.addCareProvider(csvHelper.createOrganisationReference(externalOrgGuid));
            }
        }

        transformEthnicityAndMaritalStatus(fhirPatient, patientGuid, csvHelper, fhirResourceFiler);

        String orgUuid = parser.getOrganisationGuid();
        fhirEpisode.setManagingOrganization(csvHelper.createOrganisationReference(orgUuid));

        //the care manager on the episode is the person who cares for the patient AT THIS ORGANISATION,
        //so ignore the external... fields which refer to clinicians elsewhere
        if (!Strings.isNullOrEmpty(usualGpGuid)) {
            fhirEpisode.setCareManager(csvHelper.createPractitionerReference(usualGpGuid));
        }

        Date regDate = parser.getDateOfRegistration();
        Date dedDate = parser.getDateOfDeactivation();
        Period fhirPeriod = PeriodHelper.createPeriod(regDate, dedDate);
        fhirEpisode.setPeriod(fhirPeriod);

        boolean active = PeriodHelper.isActive(fhirPeriod);
        fhirPatient.setActive(active);
        if (active) {
            fhirEpisode.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
        } else {
            fhirEpisode.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
        }

        if (parser.getIsConfidential()) {
            //add the confidential flag to BOTH resources
            fhirPatient.addExtension(ExtensionConverter.createBooleanExtension(FhirExtensionUri.IS_CONFIDENTIAL, true));
            fhirEpisode.addExtension(ExtensionConverter.createBooleanExtension(FhirExtensionUri.IS_CONFIDENTIAL, true));
        }

        //save both resources together, so the patient is defintiely saved before the episode
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirPatient, fhirEpisode);
    }*/

    /**
     * Emis send us a delete for a patient WITHOUT a corresponding delete for all other data, so
     * we need to manually delete all dependant resources
     */
    private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper,
                                                  CsvCurrentState currentState,
                                                  PatientBuilder patientBuilder, EpisodeOfCareBuilder episodeBuilder) throws Exception {

        //find the discovery UUIDs for the patient and episode of care that we'll have previously saved to the DB
        Resource fhirPatient = patientBuilder.getResource();
        Resource fhirEpisode = episodeBuilder.getResource();

        UUID edsPatientId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), fhirPatient.getResourceType(), fhirPatient.getId());
        UUID edsEpisodeId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), fhirEpisode.getResourceType(), fhirEpisode.getId());

        //only go into this if we've had something for the patient before
        if (edsPatientId != null) {

            String edsPatientIdStr = edsPatientId.toString();
            String patientGuid = fhirPatient.getId();

            //the episode ID MAY be null if we've received something for the patient (e.g. an observation) before
            //we actually received the patient record itself
            String edsEpisodeIdStr = null;
            if (edsEpisodeId != null) {
                edsEpisodeIdStr = edsEpisodeId.toString();
            }

            List<Resource> resources = csvHelper.retrieveAllResourcesForPatient(patientGuid, fhirResourceFiler);
            if (resources != null) {
                for (Resource resource : resources) {

                    //if this resource is our patient or episode resource, then skip deleting it here, as we'll just delete them at the end
                    if ((resource.getResourceType() == fhirPatient.getResourceType()
                            && resource.getId().equals(edsPatientIdStr))
                            || (edsEpisodeId != null
                            && resource.getResourceType() == fhirEpisode.getResourceType()
                            && resource.getId().equals(edsEpisodeIdStr))) {
                        continue;
                    }

                    //do not delete Appointment resources either. If Emis delete and subsequently un-delete a patient
                    //they do not re-send the Appointments, so we shouldn't delete them in the first place.
                    if (resource.getResourceType() == ResourceType.Appointment) {
                        continue;
                    }

                    //wrap the resource in generic builder so we can save it
                    GenericBuilder genericBuilder = new GenericBuilder(resource);
                    fhirResourceFiler.deletePatientResource(currentState, false, genericBuilder);
                }
            }
        }

        //and delete the patient and episode
        fhirResourceFiler.deletePatientResource(currentState, patientBuilder, episodeBuilder);
    }

    /*private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper,
                                                                                                    CsvCurrentState currentState, String patientGuid,
																									org.hl7.fhir.instance.model.Patient fhirPatient, EpisodeOfCare fhirEpisode) throws Exception {

        UUID edsPatientId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirPatient.getResourceType(), fhirPatient.getId());
        UUID edsEpisodeId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirEpisode.getResourceType(), fhirEpisode.getId());

        //only go into this if we've had something for the patient before
        if (edsPatientId != null) {

            String edsPatientIdStr = edsPatientId.toString();

            //the episode ID MAY be null if we've received something for the patient (e.g. an observation) before
            //we actually received the patient record itself
            String edsEpisodeIdStr = null;
            if (edsEpisodeId != null) {
                edsEpisodeIdStr = edsEpisodeId.toString();
            }

            List<Resource> resources = csvHelper.retrieveAllResourcesForPatient(patientGuid, fhirResourceFiler);
            if (resources != null) {
                for (Resource resource : resources) {

                    //if this resource is our patient or episode resource, then skip deleting it here, as we'll just delete them at the end
                    if ((resource.getResourceType() == fhirPatient.getResourceType()
                            && resource.getId().equals(edsPatientIdStr))
                            || (edsEpisodeId != null
                            && resource.getResourceType() == fhirEpisode.getResourceType()
                            && resource.getId().equals(edsEpisodeIdStr))) {
                        continue;
                    }

                    //do not delete Appointment resources either. If Emis delete and subsequently un-delete a patient
                    //they do not re-send the Appointments, so we shouldn't delete them in the first place.
                    if (resource.getResourceType() == ResourceType.Appointment) {
                        continue;
                    }

                    fhirResourceFiler.deletePatientResource(currentState, false, resource);
                }
            }
        }

        //and delete the patient and episode
        fhirResourceFiler.deletePatientResource(currentState, fhirPatient, fhirEpisode);
    }*/

    private static void transformEthnicityAndMaritalStatus(PatientBuilder patientBuilder,
                                                           CsvCell patientGuid,
                                                           EmisCsvHelper csvHelper,
                                                           FhirResourceFiler fhirResourceFiler) throws Exception {

        CodeAndDate newEthnicity = csvHelper.findEthnicity(patientGuid);
        CodeAndDate newMaritalStatus = csvHelper.findMaritalStatus(patientGuid);

        //if we don't have an ethnicity or marital status already cached, we may be performing a delta transform
        //so need to carry over any codeable concept already stored on the DB
        if (newEthnicity == null || newMaritalStatus == null) {
            org.hl7.fhir.instance.model.Patient existingPatient = (org.hl7.fhir.instance.model.Patient) csvHelper.retrieveResource(patientGuid.getString(), ResourceType.Patient);
            if (existingPatient != null) {

                if (newEthnicity == null) {
                    CodeableConcept oldEthnicity = (CodeableConcept) ExtensionConverter.findExtensionValue(existingPatient, FhirExtensionUri.PATIENT_ETHNICITY);
                    if (oldEthnicity != null) {
                        String oldEthnicityCode = CodeableConceptHelper.findCodingCode(oldEthnicity, FhirValueSetUri.VALUE_SET_ETHNIC_CATEGORY);
                        if (!Strings.isNullOrEmpty(oldEthnicityCode)) {
                            EthnicCategory ethnicCategory = EthnicCategory.fromCode(oldEthnicityCode);
                            patientBuilder.setEthnicity(ethnicCategory);
                        }
                    }
                }

                if (newMaritalStatus == null
                        && existingPatient.hasMaritalStatus()) {
                    CodeableConcept oldMaritalStatus = existingPatient.getMaritalStatus();

                    String oldMaritalStatusCode = CodeableConceptHelper.findCodingCode(oldMaritalStatus, FhirValueSetUri.VALUE_SET_MARITAL_STATUS);
                    if (!Strings.isNullOrEmpty(oldMaritalStatusCode)) {
                        MaritalStatus maritalStatus = MaritalStatus.fromCode(oldMaritalStatusCode);
                        patientBuilder.setMaritalStatus(maritalStatus);
                    }
                }
            }
        }

        if (newEthnicity != null) {
            EmisCsvCodeMap codeMapping = newEthnicity.getCodeMapping();
            CsvCell[] additionalSourceCells = newEthnicity.getAdditionalSourceCells();
            EmisCodeHelper.applyEthnicity(patientBuilder, codeMapping, additionalSourceCells);
        }

        if (newMaritalStatus != null) {
            EmisCsvCodeMap codeMapping = newMaritalStatus.getCodeMapping();
            CsvCell[] additionalSourceCells = newMaritalStatus.getAdditionalSourceCells();
            EmisCodeHelper.applyMaritalStatus(patientBuilder, codeMapping, additionalSourceCells);
        }
    }

    /**
     * converts free-text NHS number status to one of the official NHS statuses
     */
    private static NhsNumberVerificationStatus convertNhsNumberVeriticationStatus(String nhsNumberStatus) throws TransformException {
        //note: no idea what possible values will come from EMIS in this field, and there's no content
        //in the column on the two live extracts seen. So this is more of a placeholder until we get some more info.
        if (nhsNumberStatus.equalsIgnoreCase("Verified")) {
            return NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;
        } else {
            throw new TransformException("Unsupported NHS number verification status [" + nhsNumberStatus + "]");
        }
    }

    /**
     * converts the patientDescription String from the CSV to the FHIR registration type
     * possible registration types based on the VocPatientType enum from EMIS Open
     */
    private static RegistrationType convertRegistrationType(String csvRegType, boolean dummyRecord, ParserI parserI) throws Exception {

        //EMIS both test and Live data has leading spaces
        csvRegType = csvRegType.trim();

        if (dummyRecord || csvRegType.equalsIgnoreCase("Dummy")) {
            return RegistrationType.DUMMY;
        } else if (csvRegType.equalsIgnoreCase("Emg")
                || csvRegType.equalsIgnoreCase("Emergency")) {
            return RegistrationType.EMERGENCY;
        } else if (csvRegType.equalsIgnoreCase("Immediately necessary")) {
            return RegistrationType.IMMEDIATELY_NECESSARY;
        } else if (csvRegType.equalsIgnoreCase("Private")) {
            return RegistrationType.PRIVATE;
        } else if (csvRegType.equalsIgnoreCase("Regular")) {
            return RegistrationType.REGULAR_GMS;
        } else if (csvRegType.equalsIgnoreCase("Temporary")) {
            return RegistrationType.TEMPORARY;
        } else if (csvRegType.equalsIgnoreCase("Community Registered")) {
            return RegistrationType.COMMUNITY;
        } else if (csvRegType.equalsIgnoreCase("Walk-In Patient")) {
            return RegistrationType.WALK_IN;
        } else if (csvRegType.equalsIgnoreCase("Other")) {
            return RegistrationType.OTHER;
        } else {
            if (TransformConfig.instance().isEmisAllowUnmappedRegistrationTypes()) {
                TransformWarnings.log(LOG, parserI, "Unhandled Emis registration type {}", csvRegType);
                return RegistrationType.OTHER;

            } else {
                throw new TransformException("Unsupported registration type " + csvRegType);
            }

            /*TransformWarnings.log(LOG, parserI, "Unhandled Emis registration type {}", csvRegType);
            return RegistrationType.OTHER;*/
        }

        /**
         * This is the FULL list of registration types from Emis Web
         *
         Immediately Necessary
         Private
         Regular
         Temporary
         Community Registered
         Dummy
         Other
         Walk-In Patient

         Contraceptive Services
         Maternity Services
         Child Health Services
         Minor Surgery
         Sexual Health
         Pre Registration
         Yellow Fever
         Dermatology
         Diabetic
         Rheumatology
         Chiropody
         Coronary Health Checks
         Ultrasound
         BCG Clinic
         Vasectomy
         Acupuncture
         Reflexology
         Hypnotherapy
         Out of Hours
         Rehabilitation
         Antenatal
         Audiology
         Gynaecology
         Doppler
         Secondary Registration
         Urgent and Emergency Care
         Externally Registered

         */
    }

}
