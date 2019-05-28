package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Patient;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Patient.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((Patient) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(Patient parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      VisionCsvHelper csvHelper,
                                      String version) throws Exception {

        //create Patient Resource builder
        PatientBuilder patientBuilder = new PatientBuilder();
        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();

        CsvCell patientID = parser.getPatientID();
        VisionCsvHelper.setUniqueId(patientBuilder, patientID, null);
        VisionCsvHelper.setUniqueId(episodeBuilder, patientID, null); //use the patient GUID as the ID for the episode

        Reference patientReference = csvHelper.createPatientReference(patientID);
        episodeBuilder.setPatient(patientReference, patientID);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell patientActionCell = parser.getPatientAction();
        if (patientActionCell.getString().equalsIgnoreCase("D")) {
            //we need to manually delete all dependant resources
            deleteEntirePatientRecord(fhirResourceFiler, csvHelper, parser.getCurrentState(), patientBuilder, episodeBuilder, patientActionCell);
            return;
        }

        CsvCell nhsNumber = parser.getNhsNumber();
        if (!nhsNumber.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setValue(nhsNumber.getString(), nhsNumber);
        }

        //store the patient ID and patient number to the patient resource
        if (!patientID.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_PATIENT_GUID);
            identifierBuilder.setValue(patientID.getString(), patientID);
        }

        CsvCell patientNumber = parser.getPatientNumber();
        if (!patientNumber.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_PATIENT_NUMBER);
            identifierBuilder.setValue(patientNumber.getString(), patientNumber);
        }

        CsvCell dob = parser.getDateOfBirth();
        patientBuilder.setDateOfBirth(dob.getDate(), dob);

        CsvCell dod = parser.getDateOfDeath();
        if (!dod.isEmpty()) {
            patientBuilder.setDateOfDeath(dod.getDate(), dod);
        }

        CsvCell sex = parser.getSex();
        VocSex sexEnum = VocSex.fromValue(sex.getString());
        Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(sexEnum);
        patientBuilder.setGender(gender, sex);

        CsvCell title = parser.getTitle();
        CsvCell givenName = parser.getGivenName();
        CsvCell surname = parser.getSurname();
        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addPrefix(title.getString(), title);
        nameBuilder.addGiven(givenName.getString(), givenName);
        nameBuilder.addFamily(surname.getString(), surname);

        CsvCell houseNameFlat = parser.getHouseNameFlatNumber();
        CsvCell numberAndStreet = parser.getNumberAndStreet();
        CsvCell village = parser.getVillage();
        CsvCell town = parser.getTown();
        CsvCell county = parser.getCounty();
        CsvCell postcode = parser.getPostcode();

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setUse(Address.AddressUse.HOME);
        addressBuilder.addLine(houseNameFlat.getString(), houseNameFlat);
        addressBuilder.addLine(numberAndStreet.getString(), numberAndStreet);
        addressBuilder.addLine(village.getString(), village);
        addressBuilder.setCity(town.getString(), town);
        addressBuilder.setDistrict(county.getString(), county);
        addressBuilder.setPostcode(postcode.getString(), postcode);

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

        CsvCell email = parser.getEmail();
        if (!email.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setValue(email.getString(), email);
        }

        CsvCell organisationID = parser.getOrganisationID();
        Reference organisationReference = csvHelper.createOrganisationReference(organisationID.getString());
        patientBuilder.setManagingOrganisation(organisationReference, organisationID);

        //create a second reference for the Episode, since it's not an immutable object
        organisationReference = csvHelper.createOrganisationReference(organisationID.getString());
        episodeBuilder.setManagingOrganisation(organisationReference, organisationID);

        //the registration type is a property of a patient's stay at an organisation, so add to that resource instead
        RegistrationType registrationType = convertRegistrationType(parser.getPatientTypeCode().getString());
        episodeBuilder.setRegistrationType(registrationType);

        //the care manager on the episode is the person who cares for the patient AT THIS ORGANISATION,
        //so ignore the external... fields which refer to clinicians elsewhere
        CsvCell usualGpID = parser.getUsualGpID();
        if (!usualGpID.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(usualGpID.getString());
            episodeBuilder.setCareManager(practitionerReference, usualGpID);
        }

        if (!usualGpID.isEmpty()
                && registrationType == RegistrationType.REGULAR_GMS) { //if they're not registered for GMS, then this isn't their usual GP
            patientBuilder.addCareProvider(csvHelper.createPractitionerReference(usualGpID.getString()));
        }

        CsvCell externalGpID = parser.getExternalUsualGPID();
        if (!externalGpID.isEmpty()) {
            patientBuilder.addCareProvider(csvHelper.createPractitionerReference(externalGpID.getString()));
        }

        CsvCell externalOrgID = parser.getExternalUsualGPOrganisation();
        if (!externalOrgID.isEmpty()) {
            patientBuilder.addCareProvider(csvHelper.createOrganisationReference(externalOrgID.getString()));
        }

        CsvCell maritalStatusCSV = parser.getMaritalStatus();
        MaritalStatus maritalStatus = convertMaritalStatus (maritalStatusCSV.getString());
        if (maritalStatus != null) {
            patientBuilder.setMaritalStatus(maritalStatus);
        } else {
            //Nothing in patient record, try coded item check from pre-transformer
            CodeableConcept fhirMartialStatus = csvHelper.findMaritalStatus(patientID);
            if (fhirMartialStatus != null) {
                String maritalStatusCode = CodeableConceptHelper.getFirstCoding(fhirMartialStatus).getCode();
                if (!Strings.isNullOrEmpty(maritalStatusCode)) {
                    patientBuilder.setMaritalStatus(MaritalStatus.fromCode(maritalStatusCode));
                }
            }
        }

        //try and get Ethnicity from Journal pre-transformer
        CodeableConcept fhirEthnicity = csvHelper.findEthnicity(patientID);

        //it might be a delta transform without ethnicity in the Journal pre-transformer, so try and get existing ethnicity from DB
        if (fhirEthnicity == null) {
            org.hl7.fhir.instance.model.Patient existingPatient
                    = (org.hl7.fhir.instance.model.Patient) csvHelper.retrieveResource(patientID.getString(), ResourceType.Patient, fhirResourceFiler);
            if (existingPatient != null) {

                CodeableConcept oldEthnicity
                        = (CodeableConcept) ExtensionConverter.findExtensionValue(existingPatient, FhirExtensionUri.PATIENT_ETHNICITY);
                if (oldEthnicity != null) {

                    String oldEthnicityCode
                            = CodeableConceptHelper.findCodingCode(oldEthnicity, FhirValueSetUri.VALUE_SET_ETHNIC_CATEGORY);
                    if (!Strings.isNullOrEmpty(oldEthnicityCode)) {
                        EthnicCategory ethnicCategory = EthnicCategory.fromCode(oldEthnicityCode);
                        patientBuilder.setEthnicity(ethnicCategory);
                    }
                }
            }
        } else {

            //otherwise, set the new and latest ethnicity
            String ethnicityCode = CodeableConceptHelper.getFirstCoding(fhirEthnicity).getCode();
            if (!Strings.isNullOrEmpty(ethnicityCode)) {
                patientBuilder.setEthnicity(EthnicCategory.fromCode(ethnicityCode));
            }
        }

        CsvCell regDate = parser.getDateOfRegistration();
        if (!regDate.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(regDate.getDate(),regDate);
        }

        CsvCell dedDate = parser.getDateOfDeactivation();
        if (!dedDate.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(dedDate.getDate(),dedDate);
        }

        boolean active = dedDate.isEmpty() || dedDate.getDate().after(new Date());
        patientBuilder.setActive(active, dedDate);

        //save both resources together, so the patient is saved before the episode
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder);
    }

    /**
     * Vision - do they send us a delete for a patient WITHOUT a corresponding delete for all other data?,
     * if so we need to manually delete all dependant resources
     */
    private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler, VisionCsvHelper csvHelper,
                                                  CsvCurrentState currentState,
                                                  PatientBuilder patientBuilder, EpisodeOfCareBuilder episodeBuilder,
                                                  CsvCell patientActionCell) throws Exception {

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

                    //wrap the resource in generic builder so we can save it
                    GenericBuilder genericBuilder = new GenericBuilder(resource);
                    genericBuilder.setDeletedAudit(patientActionCell);
                    fhirResourceFiler.deletePatientResource(currentState, false, genericBuilder);
                }
            }
        }

        //and delete the patient and episode
        patientBuilder.setDeletedAudit(patientActionCell);
        episodeBuilder.setDeletedAudit(patientActionCell);
        fhirResourceFiler.deletePatientResource(currentState, patientBuilder, episodeBuilder);
    }

    private static RegistrationType convertRegistrationType(String csvRegTypeCode) {
        switch (csvRegTypeCode) {
            case "R": return RegistrationType.REGULAR_GMS;
            case "T": return RegistrationType.TEMPORARY;
            case "P": return RegistrationType.PRIVATE;
            case "S": return RegistrationType.OTHER;
            default: return RegistrationType.OTHER;
        }
    }

    private static MaritalStatus convertMaritalStatus(String statusCode) throws Exception {
        switch (statusCode) {
            case "S": return MaritalStatus.NEVER_MARRIED;
            case "M": return MaritalStatus.MARRIED;
            case "D": return MaritalStatus.DIVORCED;
            case "P": return MaritalStatus.LEGALLY_SEPARATED;
            case "C": return MaritalStatus.DOMESTIC_PARTNER;   //"Cohabiting";
            case "W": return MaritalStatus.WIDOWED;
            case "U": return null; //"Unspecified";
            default: throw new TransformException("Unexpected Patient Marital Status Code: [" + statusCode + "]");
        }
    }
}
