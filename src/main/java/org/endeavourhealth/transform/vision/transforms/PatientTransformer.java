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
import org.endeavourhealth.transform.vision.helpers.VisionCodeHelper;
import org.endeavourhealth.transform.vision.helpers.VisionMappingHelper;
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

    private static final String MARITAL_STATUS_UNIQUE_SUFFIX = "MaritalStatus";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Patient.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResources((Patient) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResources(Patient parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {


        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell patientActionCell = parser.getPatientAction();

        boolean isDeleted = patientActionCell.getString().equalsIgnoreCase("D");
        if (isDeleted) {
            //we need to manually delete all dependant resources
            deleteEntirePatientRecord(fhirResourceFiler, parser);
            return;
        }

        //this transform creates two resources
        PatientBuilder patientBuilder = createPatientResource(parser, csvHelper, fhirResourceFiler);
        EpisodeOfCareBuilder episodeBuilder = createEpisodeResource(parser, csvHelper, fhirResourceFiler);
        ObservationBuilder maritalStatusObservationBuilder = createMaritalStatusObservation(parser, csvHelper, fhirResourceFiler);

        if (patientBuilder.isIdMapped()) {

            //if patient has previously been saved, we need to save them separately because the patient will be ID mapped (i.e. was retrieved off DB)
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, patientBuilder);
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), episodeBuilder);

            //if the marital status observation has a codeableConcept then save it, otherwise delete it
            if (maritalStatusObservationBuilder.hasMainCodeableConcept()) {
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), maritalStatusObservationBuilder);
            } else {
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), maritalStatusObservationBuilder);
            }

        } else {
            //save all resources together, so the patient is definitely saved before the episode
            //if the marital status observation has a codeable concept then save it, otherwise discard it (since the patient is brand new, there's no point deleting it)
            if (maritalStatusObservationBuilder.hasMainCodeableConcept()) {
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder, maritalStatusObservationBuilder);
            } else {
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder);
            }
        }

    }

    /**
     * the Marital Status data isn't in the Journal file so won't get transformed into a FHIR Observation. For consistency
     * with other publishers, we create a FHIR Observation
     */
    private static ObservationBuilder createMaritalStatusObservation(Patient parser, VisionCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell maritalStatusCell = parser.getMaritalStatus();
        CsvCell patientIdCell = parser.getPatientID();

        ObservationBuilder observationBuilder = new ObservationBuilder();
        observationBuilder.setId(VisionCsvHelper.createUniqueId(patientIdCell, CsvCell.factoryDummyWrapper(MARITAL_STATUS_UNIQUE_SUFFIX)));
        observationBuilder.setPatient(VisionCsvHelper.createPatientReference(patientIdCell), patientIdCell);

        String maritalStatusReadCode = VisionMappingHelper.mapMaritalStatusReadCode(maritalStatusCell);
        if (!Strings.isNullOrEmpty(maritalStatusReadCode)) {
            String maritalStatusTerm = VisionMappingHelper.mapMaritalStatusTerm(maritalStatusCell);
            String maritalStatusSnomedCode = VisionMappingHelper.mapMaritalStatusSnomedCode(maritalStatusCell);

            //create a FHIR Observation with the marital status code
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
            VisionCodeHelper.populateCodeableConcept(
                    CsvCell.factoryDummyWrapper(maritalStatusSnomedCode),
                    CsvCell.factoryWithNewValue(maritalStatusCell, maritalStatusReadCode), //copy original cell with new value so auditing works
                    CsvCell.factoryDummyWrapper(maritalStatusTerm),
                    codeableConceptBuilder, csvHelper);
        }

        return observationBuilder;
    }

    private static EpisodeOfCareBuilder createEpisodeResource(Patient parser, VisionCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();

        //factor the registration start date into the unique ID for the episode, so a change in registration start
        //date correctly results in a new episode of care being created
        CsvCell patientIdCell = parser.getPatientID();
        CsvCell regDateCell = parser.getDateOfRegistration();
        VisionCsvHelper.setUniqueId(episodeBuilder, patientIdCell, regDateCell);

        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        episodeBuilder.setPatient(patientReference, patientIdCell);

        CsvCell organisationIdCell = parser.getOrganisationID();
        Reference organisationReference = csvHelper.createOrganisationReference(organisationIdCell.getString());
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

        CsvCell dedDate = parser.getDateOfDeactivation();
        if (!dedDate.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(dedDate.getDate(), dedDate);
        }

        //and cache the start date in the helper since we'll need this when linking Encounters to Episodes
        //note we must do this AFTER the above set, otherwise we'll fail to end episodes when patients are deducted and re-register on the same day
        csvHelper.cacheLatestEpisodeStartDate(patientIdCell, regDateCell);

        endOtherEpisodes(patientIdCell, regDateCell, fhirResourceFiler);

        return episodeBuilder;
    }


    private static PatientBuilder createPatientResource(Patient parser, VisionCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        //create Patient Resource builder
        PatientBuilder patientBuilder = getPatientBuilder(parser, csvHelper);

        CsvCell nhsNumber = parser.getNhsNumber();
        createIdentifier(patientBuilder, fhirResourceFiler, nhsNumber, Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);

        //store the patient ID and patient number to the patient resource
        CsvCell patientIdCell = parser.getPatientID();
        createIdentifier(patientBuilder, fhirResourceFiler, patientIdCell, Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_PATIENT_GUID);

        CsvCell patientNumber = parser.getPatientNumber();
        createIdentifier(patientBuilder, fhirResourceFiler, patientNumber, Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_PATIENT_NUMBER);

        CsvCell dob = parser.getDateOfBirth();
        patientBuilder.setDateOfBirth(dob.getDate(), dob);

        CsvCell dod = parser.getDateOfDeath();
        if (!dod.isEmpty()) {
            patientBuilder.setDateOfDeath(dod.getDate(), dod);
        } else {
            patientBuilder.clearDateOfDeath();
        }

        CsvCell sex = parser.getSex();
        VocSex sexEnum = VocSex.fromValue(sex.getString());
        Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(sexEnum);
        patientBuilder.setGender(gender, sex);

        createName(patientBuilder, parser, fhirResourceFiler);
        createAddress(patientBuilder, parser, fhirResourceFiler);

        CsvCell homePhone = parser.getHomePhone();
        if (homePhone != null) { //null check because the cell doesn't exist in the test data
            createContact(patientBuilder, fhirResourceFiler, homePhone, ContactPoint.ContactPointUse.HOME, ContactPoint.ContactPointSystem.PHONE);
        }

        CsvCell mobilePhone = parser.getMobilePhone();
        if (mobilePhone != null) { //null check because the cell doesn't exist in the test data
            createContact(patientBuilder, fhirResourceFiler, mobilePhone, ContactPoint.ContactPointUse.MOBILE, ContactPoint.ContactPointSystem.PHONE);
        }

        CsvCell email = parser.getEmail();
        if (email != null) { //null check because the cell doesn't exist in the test data
            createContact(patientBuilder, fhirResourceFiler, email, ContactPoint.ContactPointUse.HOME, ContactPoint.ContactPointSystem.EMAIL);
        }

        CsvCell organisationIdCell = parser.getOrganisationID();
        Reference organisationReference = csvHelper.createOrganisationReference(organisationIdCell.getString());
        if (patientBuilder.isIdMapped()) {
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
        }
        patientBuilder.setManagingOrganisation(organisationReference, organisationIdCell);

        transformMaritalStatus(patientBuilder, parser, csvHelper, fhirResourceFiler);

        //the ETHNIC field on the Patient record is just a truncated version of the term of a Journal record,
        //so get the Ethnicity from the pre-cached Journal pre-transformer
        VisionCsvHelper.DateAndEthnicityCategory cached = csvHelper.findEthnicity(patientIdCell);
        VisionMappingHelper.applyEthnicityCode(cached, patientBuilder);

        //calculate patient active state based on deduction status
        CsvCell dedDate = parser.getDateOfDeactivation();
        boolean active = dedDate.isEmpty() || dedDate.getDate().after(new Date());
        patientBuilder.setActive(active, dedDate);

        //if GMS at this service, set the careProvider (i.e. registered practice / GP) on the FHIR patient
        //note we don't factor in the "active" status of the record because this is the best information we
        //have irrespective of the patient being deducted or not. If we have a separate publisher with the new active
        //registration then that will give us the new details anyway.

        //clear all care provider records, before we start adding more
        patientBuilder.clearCareProvider();

        CsvCell patientTypeCell = parser.getPatientTypeCode();
        RegistrationType registrationType = convertRegistrationType(patientTypeCell, csvHelper, patientIdCell);
        if (registrationType == RegistrationType.REGULAR_GMS) {

            Reference registeredPracticeReference = csvHelper.createOrganisationReference(organisationIdCell.getString());
            if (patientBuilder.isIdMapped()) {
                registeredPracticeReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(registeredPracticeReference, csvHelper);
            }
            patientBuilder.addCareProvider(registeredPracticeReference, organisationIdCell);

            CsvCell usualGpID = parser.getUsualGpId();
            if (!usualGpID.isEmpty()) {
                Reference usualGpReference = csvHelper.createPractitionerReference(usualGpID.getString());
                if (patientBuilder.isIdMapped()) {
                    usualGpReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(usualGpReference, csvHelper);
                }
                patientBuilder.addCareProvider(usualGpReference, usualGpID);
            }
        }

        return patientBuilder;
    }

    /**
     * Vision do not use the Journal for marital status, and just have a code on the Patient record,
     * so carry this over to the FHIR Patient but also create a FHIR Observation too
     */
    private static void transformMaritalStatus(PatientBuilder patientBuilder, Patient parser, VisionCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell maritalStatusCell = parser.getMaritalStatus();

        MaritalStatus maritalStatus = VisionMappingHelper.mapMaritalStatusToValueSet(maritalStatusCell);
        if (maritalStatus == null) {

            //remove flag from Patient resource
            patientBuilder.setMaritalStatus(null, maritalStatusCell);
        } else {

            //set on Patient resource
            patientBuilder.setMaritalStatus(maritalStatus, maritalStatusCell);
        }
    }

    private static void createContact(PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, CsvCell cell,
                                      ContactPoint.ContactPointUse use, ContactPoint.ContactPointSystem system) throws Exception {

        if (!cell.isEmpty()) {

            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(use);
            contactPointBuilder.setSystem(system);
            contactPointBuilder.setValue(cell.getString(), cell);

            ContactPointBuilder.deDuplicateLastContactPoint(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            ContactPointBuilder.endContactPoints(patientBuilder, fhirResourceFiler.getDataDate(), system, use);
        }
    }

    private static void createAddress(PatientBuilder patientBuilder, Patient parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell houseNameFlat = parser.getHouseNameFlatNumber();
        CsvCell numberAndStreet = parser.getNumberAndStreet();
        CsvCell village = parser.getVillage();
        CsvCell town = parser.getTown();
        CsvCell county = parser.getCounty();
        CsvCell postcode = parser.getPostcode();

        if (!houseNameFlat.isEmpty()
                || !numberAndStreet.isEmpty()
                || !village.isEmpty()
                || !town.isEmpty()
                || !county.isEmpty()
                || !postcode.isEmpty()) {

            AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
            addressBuilder.setUse(Address.AddressUse.HOME);
            addressBuilder.addLine(houseNameFlat.getString(), houseNameFlat);
            addressBuilder.addLine(numberAndStreet.getString(), numberAndStreet);
            addressBuilder.addLine(village.getString(), village);
            addressBuilder.setCity(town.getString(), town);
            addressBuilder.setDistrict(county.getString(), county);
            addressBuilder.setPostcode(postcode.getString(), postcode);

            AddressBuilder.deDuplicateLastAddress(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            AddressBuilder.endAddresses(patientBuilder, fhirResourceFiler.getDataDate(), Address.AddressUse.HOME);
        }
    }

    private static void createName(PatientBuilder patientBuilder, Patient parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell title = parser.getTitle();
        CsvCell givenName = parser.getGivenName();
        CsvCell surname = parser.getSurname();

        if (!title.isEmpty()
                || !givenName.isEmpty()
                || !surname.isEmpty()) {

            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            nameBuilder.addPrefix(title.getString(), title);
            nameBuilder.addGiven(givenName.getString(), givenName);
            nameBuilder.addFamily(surname.getString(), surname);

            NameBuilder.deDuplicateLastName(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            NameBuilder.endNames(patientBuilder, fhirResourceFiler.getDataDate(), HumanName.NameUse.OFFICIAL);
        }
    }

    private static void createIdentifier(PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, CsvCell cell, Identifier.IdentifierUse use, String system) throws Exception {
        if (!cell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(use);
            identifierBuilder.setSystem(system);
            identifierBuilder.setValue(cell.getString(), cell);

            IdentifierBuilder.deDuplicateLastIdentifier(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            IdentifierBuilder.endIdentifiers(patientBuilder, fhirResourceFiler.getDataDate(), system, use);
        }
    }

    private static PatientBuilder getPatientBuilder(Patient parser, VisionCsvHelper csvHelper) throws Exception {

        PatientBuilder ret = null;

        CsvCell patientIdCell = parser.getPatientID();
        String uniqueId = csvHelper.createUniqueId(patientIdCell, null);
        org.hl7.fhir.instance.model.Patient existingResource = (org.hl7.fhir.instance.model.Patient)csvHelper.retrieveResource(uniqueId, ResourceType.Patient);
        if (existingResource != null) {
            ret = new PatientBuilder(existingResource);
        } else {
            ret = new PatientBuilder();
            VisionCsvHelper.setUniqueId(ret, patientIdCell, null);
        }

        return ret;
    }

    /**
     * Vision - do they send us a delete for a patient WITHOUT a corresponding delete for all other data?,
     * if so we need to manually delete all dependant resources
     */
    private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler,
                                                  Patient parser) throws Exception {

        CsvCurrentState currentState = parser.getCurrentState();
        CsvCell patientIdCell = parser.getPatientID();
        CsvCell patientActionCell = parser.getPatientAction();
        String sourceId = VisionCsvHelper.createUniqueId(patientIdCell, null);

        PatientDeleteHelper.deleteAllResourcesForPatient(sourceId, fhirResourceFiler, currentState, patientActionCell);
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
