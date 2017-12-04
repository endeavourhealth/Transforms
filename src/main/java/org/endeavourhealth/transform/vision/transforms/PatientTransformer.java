package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.emis.csv.CsvCurrentState;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
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
        while (parser.nextRecord()) {

            try {
                createResource((Patient)parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(Patient parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      VisionCsvHelper csvHelper,
                                       String version) throws Exception {

        //create Patient Resource
        org.hl7.fhir.instance.model.Patient fhirPatient = new org.hl7.fhir.instance.model.Patient();
        fhirPatient.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PATIENT));

        String patientID = parser.getPatientID();
        VisionCsvHelper.setUniqueId(fhirPatient, patientID, null);

        //create Episode of Care Resource
        EpisodeOfCare fhirEpisode = new EpisodeOfCare();
        fhirEpisode.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_EPISODE_OF_CARE));
        VisionCsvHelper.setUniqueId(fhirEpisode, patientID, null);
        fhirEpisode.setPatient(csvHelper.createPatientReference(patientID.toString()));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getPatientAction().equalsIgnoreCase("D")) {
            //we need to manually delete all dependant resources
            deleteEntirePatientRecord(fhirResourceFiler, csvHelper, parser.getCurrentState(), patientID, fhirPatient, fhirEpisode);
            return;
        }

        String nhsNumber = parser.getNhsNumber();
        if (!Strings.isNullOrEmpty(nhsNumber)) {
            fhirPatient.addIdentifier(IdentifierHelper.createNhsNumberIdentifier(nhsNumber));
        }

        //store the patient GUID and patient number to the patient resource
        int patientNumber = parser.getPatientNumber();
        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirUri.IDENTIFIER_SYSTEM_VISION_PATIENT_GUID, patientID));
        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirUri.IDENTIFIER_SYSTEM_VISION_PATIENT_NUMBER, "" + patientNumber));

        Date dob = parser.getDateOfBirth();
        fhirPatient.setBirthDate(dob);

        Date dod = parser.getDateOfDeath();
        if (dod != null) {
            fhirPatient.setDeceased(new DateTimeType(dod));
        }

        VocSex vocSex = VocSex.fromValue(parser.getSex());
        Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(vocSex);
        fhirPatient.setGender(gender);

        String title = parser.getTitle();
        String givenName = parser.getGivenName();
        String surname = parser.getSurname();

        if (Strings.isNullOrEmpty(surname)) {
            surname = givenName;
            givenName = "";
        }

        String forenames = givenName;

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

        Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME, houseNameFlat, numberAndStreet, village, town, county, postcode);
        fhirPatient.addAddress(fhirAddress);

        RegistrationType registrationType = convertRegistrationType(parser.getPatientTypeCode());
        //the registration type is a property of a patient's stay at an organisation, so add to that resource instead
        fhirEpisode.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_REGISTRATION_TYPE, CodingHelper.createCoding(registrationType)));
        //fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_REGISTRATION_TYPE, CodingHelper.createCoding(registrationType)));

        //HL7 have clarified that the care provider field is for the patient's general practitioner, NOT
        //for the patient's carer at a specific organisation. That being the case, we store the local carer
        //on the episode_of_care and the general practitioner on the patient.
        String usualGpID = parser.getUsualGpID();
        if (!Strings.isNullOrEmpty(usualGpID)
                && registrationType == RegistrationType.REGULAR_GMS) { //if they're not registered for GMS, then this isn't their usual GP
            fhirPatient.addCareProvider(csvHelper.createPractitionerReference(usualGpID));
        }

        String externalGpID = parser.getExternalUsualGPID();
        if (!Strings.isNullOrEmpty(externalGpID)) {
            fhirPatient.addCareProvider(csvHelper.createPractitionerReference(externalGpID));
        }

        String externalOrgID = parser.getExternalUsualGPOrganisation();
        if (!Strings.isNullOrEmpty(externalOrgID)) {
            fhirPatient.addCareProvider(csvHelper.createOrganisationReference(externalOrgID));
        }

        String maritalStatusCSV = parser.getMaritalStatus();
        MaritalStatus maritalStatus = convertMaritalStatus (maritalStatusCSV);
        if (maritalStatus != null) {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(maritalStatus);
            fhirPatient.setMaritalStatus(codeableConcept);
        } else {
            //Nothing in patient record, try coded item check from pre-transformer
            CodeableConcept codeableConcept = csvHelper.findMaritalStatus(patientID);
            if (codeableConcept != null)
                fhirPatient.setMaritalStatus(codeableConcept);
        }

        String homePhone = parser.getHomePhone();
        ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.HOME, homePhone);
        fhirPatient.addTelecom(fhirContact);

        String mobilePhone = parser.getMobilePhone();
        fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.MOBILE, mobilePhone);
        fhirPatient.addTelecom(fhirContact);

        String email = parser.getEmail();
        fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.EMAIL, ContactPoint.ContactPointUse.HOME, email);
        fhirPatient.addTelecom(fhirContact);

        //get Ethnicity from Journal pre-transformer
        CodeableConcept fhirEthnicity = csvHelper.findEthnicity(patientID);
        if (fhirEthnicity != null) {
            fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_ETHNICITY, fhirEthnicity));
        }

        String organisationID = parser.getOrganisationID();
        fhirEpisode.setManagingOrganization(csvHelper.createOrganisationReference(organisationID));

        //the care manager on the episode is the person who cares for the patient AT THIS ORGANISATION,
        //so ignore the external... fields which refer to clinicians elsewhere
        if (!Strings.isNullOrEmpty(usualGpID)) {
            fhirEpisode.setCareManager(csvHelper.createPractitionerReference(usualGpID));
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

        //save both resources together, so the patient is saved before the episode
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirPatient, fhirEpisode);
    }

    /**
     * Vision - do they send us a delete for a patient WITHOUT a corresponding delete for all other data?,
     * if so we need to manually delete all dependant resources
     */
    private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler, VisionCsvHelper csvHelper,
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

                    fhirResourceFiler.deletePatientResource(currentState, false, patientGuid, resource);
                }
            }
        }

        //and delete the patient and episode
        fhirResourceFiler.deletePatientResource(currentState, patientGuid, fhirPatient, fhirEpisode);
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

    private static MaritalStatus convertMaritalStatus(String statusCode) {
        switch (statusCode) {
            case "S": return MaritalStatus.NEVER_MARRIED;
            case "M": return MaritalStatus.MARRIED;
            case "D": return MaritalStatus.DIVORCED;
            case "P": return MaritalStatus.LEGALLY_SEPARATED;
            case "C": return null; //"Cohabiting";
            case "W": return MaritalStatus.WIDOWED;
            case "U": return null; //"Unspecified";
            default: return null;
        }
    }
}
