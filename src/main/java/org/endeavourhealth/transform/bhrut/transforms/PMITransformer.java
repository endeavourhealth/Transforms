package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.PMI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PMITransformer {


    private static final Logger LOG = LoggerFactory.getLogger(PMITransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(PMI.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResources((PMI) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResources(PMI parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {



        CsvCell patientActionCell = parser.getLineStatus();
        if (patientActionCell.getString().equalsIgnoreCase("delete")) {
            //we need to manually delete all dependant resources
            deleteEntirePatientRecord(fhirResourceFiler, csvHelper, parser.getCurrentState(), parser);
            return;
        }


        PatientBuilder patientBuilder = createPatientResource(parser, csvHelper);
        EpisodeOfCareBuilder episodeBuilder = createEpisodeResource(parser, csvHelper, fhirResourceFiler);

        if (patientBuilder.isIdMapped()) {
            //if patient has previously been saved, we need to save them separately
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, patientBuilder);
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), episodeBuilder);

        } else {
            //save both resources together, so the patient is definitely saved before the episode
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder);
        }


    }

    private static PatientBuilder createPatientResource(PMI parser,
                                                        BhrutCsvHelper csvHelper) throws Exception {

        //create Patient Resource builder
        PatientBuilder patientBuilder = getPatientBuilder(parser, csvHelper);

        CsvCell nhsNumber = parser.getNhsNumber();
        createIdentifier(patientBuilder, csvHelper, nhsNumber, Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);

        //store the patient ID and patient number to the patient resource
        CsvCell patientIdCell = parser.getPasId();
        createIdentifier(patientBuilder, csvHelper, patientIdCell, Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_PATIENT_GUID);

        //CsvCell patientNumber = parser.getPatientExternalId();
        //createIdentifier(patientBuilder, csvHelper, patientNumber, Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_VISION_PATIENT_NUMBER);


        CsvCell dob = parser.getDateOfBirth();
        patientBuilder.setDateOfBirth(dob.getDate(), dob);

        CsvCell dod = parser.getDateOfDeath();
        if (!dod.isEmpty()) {
            patientBuilder.setDateOfDeath(dod.getDate(), dod);
        } else {
            patientBuilder.clearDateOfDeath();
        }

        CsvCell sex = parser.getGenderCode();
        VocSex sexEnum = VocSex.fromValue(sex.getString());
        Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(sexEnum);
        patientBuilder.setGender(gender, sex);

        createName(patientBuilder, parser, csvHelper);
        createAddress(patientBuilder, parser, csvHelper);

        CsvCell homePhone = parser.getHomePhoneNumber();
        if (homePhone != null) {
            createContact(patientBuilder, csvHelper, homePhone, ContactPoint.ContactPointUse.HOME, ContactPoint.ContactPointSystem.PHONE);
        }

        CsvCell mobilePhone = parser.getMobilePhoneNumber();
        if (mobilePhone != null) {
            createContact(patientBuilder, csvHelper, mobilePhone, ContactPoint.ContactPointUse.MOBILE, ContactPoint.ContactPointSystem.PHONE);
        }

        //Todo ETHNICITY_CODE in PMI.  It looks like EthnicCategory is from the fhir package, but need that confirmed.
        //try and get Ethnicity
        CodeableConcept fhirEthnicity = csvHelper.findEthnicity(patientIdCell);
        if (fhirEthnicity != null) {
            String ethnicityCode = CodeableConceptHelper.getFirstCoding(fhirEthnicity).getCode();
            if (!Strings.isNullOrEmpty(ethnicityCode)) {
                patientBuilder.setEthnicity(EthnicCategory.fromCode(ethnicityCode));
            }
        }

        CsvCell spineSensitive = parser.getSensitivePdsFlag();
        if (spineSensitive.getBoolean()) {
            patientBuilder.setSpineSensitive(true, spineSensitive);
        } else {
            patientBuilder.setSpineSensitive(false, spineSensitive);
        }

        //clear all care provider records, before we start adding more
        patientBuilder.clearCareProvider();

        // Todo need confirmation PatientTypCode column.
//        CsvCell patientTypeCell = parser.getPatientTypeCode();
        /*CsvCell patientTypeCell = parser.getLineStatus();
        RegistrationType registrationType = convertRegistrationType(patientTypeCell, csvHelper, patientIdCell);
        if (registrationType == RegistrationType.REGULAR_GMS) {

            Reference registeredPracticeReference = csvHelper.createOrganisationReference(organisationIdCell.getString());
            if (patientBuilder.isIdMapped()) {
                registeredPracticeReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(registeredPracticeReference, csvHelper);
            }
            patientBuilder.addCareProvider(registeredPracticeReference, organisationIdCell);
        }*/

        return patientBuilder;
    }

    private static PatientBuilder getPatientBuilder(PMI parser, BhrutCsvHelper csvHelper) throws Exception {

        PatientBuilder ret = null;
        CsvCell patientIdCell = parser.getPasId();
        String uniqueId = csvHelper.createUniqueId(patientIdCell, null);
        org.hl7.fhir.instance.model.Patient existingResource = (org.hl7.fhir.instance.model.Patient) csvHelper.retrieveResource(uniqueId, ResourceType.Patient);
        if (existingResource != null) {
            ret = new PatientBuilder(existingResource);
        } else {
            ret = new PatientBuilder();
            VisionCsvHelper.setUniqueId(ret, patientIdCell, null);
        }

        return ret;
    }

    private static void createIdentifier(PatientBuilder patientBuilder, BhrutCsvHelper csvHelper, CsvCell cell, Identifier.IdentifierUse use, String system) throws Exception {
        if (!cell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(use);
            identifierBuilder.setSystem(system);
            identifierBuilder.setValue(cell.getString(), cell);

            IdentifierBuilder.deDuplicateLastIdentifier(patientBuilder, csvHelper.getDataDate());

        } else {
            IdentifierBuilder.endIdentifiers(patientBuilder, csvHelper.getDataDate(), system, use);
        }
    }

    private static void createName(PatientBuilder patientBuilder, PMI parser, BhrutCsvHelper csvHelper) throws Exception {

        //CsvCell title = parser.getTitle();
        CsvCell givenName = parser.getForename();
        CsvCell surname = parser.getSurname();

        if (!givenName.isEmpty()
                || !surname.isEmpty()) {

            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            //nameBuilder.addPrefix(title.getString(), title);
            nameBuilder.addGiven(givenName.getString(), givenName);
            nameBuilder.addFamily(surname.getString(), surname);

            NameBuilder.deDuplicateLastName(patientBuilder, csvHelper.getDataDate());

        } else {
            NameBuilder.endNames(patientBuilder, csvHelper.getDataDate(), HumanName.NameUse.OFFICIAL);
        }
    }

    private static void createAddress(PatientBuilder patientBuilder, PMI parser, BhrutCsvHelper csvHelper) throws Exception {

        CsvCell houseNameFlat = parser.getAddress1();
        CsvCell numberAndStreet = parser.getAddress2();
        CsvCell village = parser.getAddress3();
        CsvCell town = parser.getAddress4();
        CsvCell county = parser.getAddress5();
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

            AddressBuilder.deDuplicateLastAddress(patientBuilder, csvHelper.getDataDate());

        } else {
            AddressBuilder.endAddresses(patientBuilder, csvHelper.getDataDate(), Address.AddressUse.HOME);
        }
    }

    private static void createContact(PatientBuilder patientBuilder, BhrutCsvHelper csvHelper, CsvCell cell,
                                      ContactPoint.ContactPointUse use, ContactPoint.ContactPointSystem system) throws Exception {

        if (!cell.isEmpty()) {

            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(use);
            contactPointBuilder.setSystem(system);
            contactPointBuilder.setValue(cell.getString(), cell);

            ContactPointBuilder.deDuplicateLastContactPoint(patientBuilder, csvHelper.getDataDate());

        } else {
            ContactPointBuilder.endContactPoints(patientBuilder, csvHelper.getDataDate(), system, use);
        }
    }

    private static RegistrationType convertRegistrationType(CsvCell patientTypeCell, BhrutCsvHelper csvHelper, CsvCell patientIdCell) throws Exception {

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

        }
    }

    private static RegistrationType findPreviousRegistrationType(BhrutCsvHelper csvHelper, CsvCell patientIdCell) throws Exception {

        String localId = VisionCsvHelper.createUniqueId(patientIdCell, null);
        EpisodeOfCare episodeOfCare = (EpisodeOfCare) csvHelper.retrieveResource(localId, ResourceType.EpisodeOfCare);

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


    /**
     * Bhrut - do they send us a delete for a patient WITHOUT a corresponding delete for all other data?,
     * if so we need to manually delete all dependant resources
     */
    private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler,
                                                  BhrutCsvHelper csvHelper,
                                                  CsvCurrentState currentState,
                                                  PMI parser) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        CsvCell patientActionCell = parser.getLineStatus();

        String sourceId = VisionCsvHelper.createUniqueId(patientIdCell, null);


        List<Resource> resources = csvHelper.retrieveAllResourcesForPatient(sourceId, fhirResourceFiler);
        if (resources == null) {
            return;
        }

        for (Resource resource : resources) {

            //wrap the resource in generic builder so we can save it
            GenericBuilder genericBuilder = new GenericBuilder(resource);
            genericBuilder.setDeletedAudit(patientActionCell);
            fhirResourceFiler.deletePatientResource(currentState, false, genericBuilder);
        }
    }

    private static EpisodeOfCareBuilder createEpisodeResource(PMI parser, BhrutCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();
        CsvCell patientIdCell = parser.getPasId();

        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        episodeBuilder.setPatient(patientReference, patientIdCell);


        //Todo need to confirm PatientTypeId column
        //the registration type is a property of a patient's stay at an organisation, so add to that resource instead
        //CsvCell patientTypeCell = parser.getPatientTypeCode();
        //CsvCell patientTypeCell = parser.getLineStatus();
        //RegistrationType registrationType = convertRegistrationType(patientTypeCell, csvHelper, patientIdCell);
        //episodeBuilder.setRegistrationType(registrationType, patientTypeCell);

        return episodeBuilder;
    }


}