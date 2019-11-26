package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
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

        CsvCell patientIdCell = parser.getPatientID();
        VisionCsvHelper.setUniqueId(patientBuilder, patientIdCell, null);

        //factor the registration start date into the unique ID for the episode, so a change in registration start
        //date correctly results in a new episode of care being created
        CsvCell regDateCell = parser.getDateOfRegistration();
        VisionCsvHelper.setUniqueId(episodeBuilder, patientIdCell, regDateCell);

        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        episodeBuilder.setPatient(patientReference, patientIdCell);

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
        if (!patientIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_PATIENT_GUID);
            identifierBuilder.setValue(patientIdCell.getString(), patientIdCell);
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
        //null check because the cell doesn't exist in the test data
        if (homePhone != null && !homePhone.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setValue(homePhone.getString(), homePhone);
        }

        CsvCell mobilePhone = parser.getMobilePhone();
        //null check because the cell doesn't exist in the test data
        if (mobilePhone != null && !mobilePhone.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.MOBILE);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setValue(mobilePhone.getString(), mobilePhone);
        }

        CsvCell email = parser.getEmail();
        //null check because the cell doesn't exist in the test data
        if (email != null && !email.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setValue(email.getString(), email);
        }

        CsvCell organisationIdCell = parser.getOrganisationID();
        Reference organisationReference = csvHelper.createOrganisationReference(organisationIdCell.getString());
        patientBuilder.setManagingOrganisation(organisationReference, organisationIdCell);

        //create a second reference for the Episode, since it's not an immutable object
        organisationReference = csvHelper.createOrganisationReference(organisationIdCell.getString());
        episodeBuilder.setManagingOrganisation(organisationReference, organisationIdCell);

        //the registration type is a property of a patient's stay at an organisation, so add to that resource instead
        CsvCell patientTypeCell = parser.getPatientTypeCode();
        RegistrationType registrationType = convertRegistrationType(patientTypeCell, csvHelper, patientIdCell);
        episodeBuilder.setRegistrationType(registrationType, patientTypeCell);

        //the care manager on the episode is the person who cares for the patient AT THIS ORGANISATION,
        //so ignore the external... fields which refer to clinicians elsewhere
        CsvCell usualGpID = parser.getUsualGpId();
        if (!usualGpID.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(usualGpID.getString());
            episodeBuilder.setCareManager(practitionerReference, usualGpID);
        }

        //This is set and used at the beginning for the ID value for the episode
        if (!regDateCell.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(regDateCell.getDate(), regDateCell);
        }

        //and cache the start date in the helper since we'll need this when linking Encounters to Episodes
        //note we must do this AFTER the above set, otherwise we'll fail to end episodes when patients are deducted and re-register on the same day
        csvHelper.cacheLatestEpisodeStartDate(patientIdCell, regDateCell);

        endOtherEpisodes(patientIdCell, regDateCell, fhirResourceFiler);

        /*if (!usualGpID.isEmpty()
                && registrationType == RegistrationType.REGULAR_GMS) { //if they're not registered for GMS, then this isn't their usual GP
            patientBuilder.addCareProvider(csvHelper.createPractitionerReference(usualGpID.getString()));
        }

        CsvCell externalGpID = parser.getRegisteredGpId();
        if (!externalGpID.isEmpty()) {
            patientBuilder.addCareProvider(csvHelper.createPractitionerReference(externalGpID.getString()));
        }

        CsvCell externalOrgID = parser.getExternalUsualGPOrganisation();
        if (!externalOrgID.isEmpty()) {
            patientBuilder.addCareProvider(csvHelper.createOrganisationReference(externalOrgID.getString()));
        }*/

        CsvCell maritalStatusCSV = parser.getMaritalStatus();
        MaritalStatus maritalStatus = convertMaritalStatus(maritalStatusCSV.getString());
        if (maritalStatus != null) {
            patientBuilder.setMaritalStatus(maritalStatus);
        } else {
            //Nothing in patient record, try coded item check from pre-transformer
            CodeableConcept fhirMartialStatus = csvHelper.findMaritalStatus(patientIdCell);
            if (fhirMartialStatus != null) {
                String maritalStatusCode = CodeableConceptHelper.getFirstCoding(fhirMartialStatus).getCode();
                if (!Strings.isNullOrEmpty(maritalStatusCode)) {
                    patientBuilder.setMaritalStatus(MaritalStatus.fromCode(maritalStatusCode));
                }
            }
        }

        //try and get Ethnicity from Journal pre-transformer
        CodeableConcept fhirEthnicity = csvHelper.findEthnicity(patientIdCell);

        //it might be a delta transform without ethnicity in the Journal pre-transformer, so try and get existing ethnicity from DB
        if (fhirEthnicity == null) {
            org.hl7.fhir.instance.model.Patient existingPatient
                    = (org.hl7.fhir.instance.model.Patient) csvHelper.retrieveResource(patientIdCell.getString(), ResourceType.Patient);
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

        CsvCell dedDate = parser.getDateOfDeactivation();
        if (!dedDate.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(dedDate.getDate(), dedDate);
        }

        //setting the above dates on the episode calculates the active state of the episode, so carry over to the patient
        boolean active = episodeBuilder.getStatus() == EpisodeOfCare.EpisodeOfCareStatus.ACTIVE;
        patientBuilder.setActive(active, dedDate);

        //if GMS at this service, set the careProvider (i.e. registered practice / GP) on the FHIR patient
        //note we don't factor in the "active" status of the record because this is the best information we
        //have irrespective of the patient being deducted or not. If we have a separate publisher with the new active
        //registration then that will give us the new details anyway.
        if (registrationType == RegistrationType.REGULAR_GMS) {

            Reference registeredPracticeReference = csvHelper.createOrganisationReference(organisationIdCell.getString());
            patientBuilder.addCareProvider(registeredPracticeReference, organisationIdCell);

            if (!usualGpID.isEmpty()) {
                Reference usualGpReference = csvHelper.createPractitionerReference(usualGpID.getString());
                patientBuilder.addCareProvider(usualGpReference, usualGpID);
            }
        }

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

    private static RegistrationType convertRegistrationType(CsvCell patientTypeCell, VisionCsvHelper csvHelper, CsvCell patientIdCell) throws Exception {

        String csvRegTypeCode = patientTypeCell.getString();

        switch (csvRegTypeCode) {
            case "R": //Currently registered for GMS
                return RegistrationType.REGULAR_GMS;
            case "T": //Temporary
                return RegistrationType.TEMPORARY;
            case "P": //Private patient
                return RegistrationType.PRIVATE;
            case "S": //Not registered for GMS but is registered for another service category (e.g. contraception or child health)
                return RegistrationType.OTHER;
            case "D": //Deceased.
                LOG.debug("Registration type is D so will look up previous registration type for patient " + patientIdCell.getString());
                return findPreviousRegistrationType(csvHelper, patientIdCell);
            case "L": //Left practice (no longer registered)
                LOG.debug("Registration type is L so will look up previous registration type for patient " + patientIdCell.getString());
                return findPreviousRegistrationType(csvHelper, patientIdCell);
            default:
                throw new TransformException("Unexpected patient type code: [" + csvRegTypeCode + "]");
                //return RegistrationType.OTHER;
        }
    }

    /**
     * when a Vision patient leaves or dies, the registration type (i.e. ACTIVE field) is changed from whatever it
     * was to L or D respectively (this in addition to the deduction date and date of death being set). So we don't
     * lose the previous registration type, if a patient has one of these values, we attempt to carry over the
     * previous registration type from the previously saved EpisodeOfCare
     */
    private static RegistrationType findPreviousRegistrationType(VisionCsvHelper csvHelper, CsvCell patientIdCell) throws Exception {

        String localId = VisionCsvHelper.createUniqueId(patientIdCell, null);
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)csvHelper.retrieveResource(localId, ResourceType.EpisodeOfCare);

        //if no previous instance of the episode, we have no idea what the registration type used to be
        if (episodeOfCare == null) {
            return RegistrationType.OTHER;
        }

        EpisodeOfCareBuilder builder = new EpisodeOfCareBuilder(episodeOfCare);
        RegistrationType previousType = builder.getRegistrationType();

        //we should never have a previous episode without a type, but just in case
        if (previousType == null) {
            return RegistrationType.OTHER;
        }

        return previousType;
    }

    private static MaritalStatus convertMaritalStatus(String statusCode) throws Exception {
        switch (statusCode) {
            case "S":
                return MaritalStatus.NEVER_MARRIED;
            case "M":
                return MaritalStatus.MARRIED;
            case "D":
                return MaritalStatus.DIVORCED;
            case "P":
                return MaritalStatus.LEGALLY_SEPARATED;
            case "C":
                return MaritalStatus.DOMESTIC_PARTNER;   //"Cohabiting";
            case "W":
                return MaritalStatus.WIDOWED;
            case "U":
                return null; //"Unspecified";
            default:
                throw new TransformException("Unexpected Patient Marital Status Code: [" + statusCode + "]");
        }
    }

    /**
     * if a patient was deducted and re-registered on the same day, we don't ever receive the end date for the previous
     * registration, so we need to manually check for any active episode with a different start date and end them
     */
    private static void endOtherEpisodes(CsvCell patientGuidCell, CsvCell thisStartDateCell, FhirResourceFiler fhirResourceFiler) throws Exception {

        String sourceId = VisionCsvHelper.createUniqueId(patientGuidCell, null);
        UUID globallyUniqueId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, sourceId);
        if (globallyUniqueId == null) {
            return;
        }

        Date thisStartDate = thisStartDateCell.getDate();

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> episodeWrappers
                = resourceDal.getResourcesByPatient(fhirResourceFiler.getServiceId(), globallyUniqueId, ResourceType.EpisodeOfCare.toString());

        for (ResourceWrapper episodeWrapper: episodeWrappers) {
            EpisodeOfCare episode = (EpisodeOfCare) FhirSerializationHelper.deserializeResource(episodeWrapper.getResourceData());
            if (!episode.hasPeriod()) {
                throw new Exception("Episode " + episode.getId() + " doesn't have period");
            }

            Period period = episode.getPeriod();
            if (!PeriodHelper.isActive(period)) {
                continue;
            }

            Date startDate = period.getStart();
            if (startDate == null) {
                throw new Exception("Episode " + episode.getId() + " doesn't have start date");
            }

            //if we're re-processing old files, we will end up processing records for old start dates,
            //and we need to ensure we don't accidentially end episodes from AFTER
            if (!startDate.before(thisStartDate)) {
                continue;
            }

            EpisodeOfCareBuilder builder = new EpisodeOfCareBuilder(episode);
            builder.setRegistrationEndDate(thisStartDate, thisStartDateCell);

            fhirResourceFiler.savePatientResource(null, false, builder);
        }
    }
}
