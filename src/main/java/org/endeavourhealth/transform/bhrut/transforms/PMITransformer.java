package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.PMI;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
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
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder);
    }

    private static PatientBuilder createPatientResource(PMI parser,
                                                        BhrutCsvHelper csvHelper) throws Exception {

        PatientBuilder patientBuilder = getPatientBuilder(parser, csvHelper);

        CsvCell nhsNumber = parser.getNhsNumber();
        createIdentifier(patientBuilder, csvHelper, nhsNumber, Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);

        //store the PAS ID as a secondary identifier
        CsvCell patientIdCell = parser.getPasId();
        createIdentifier(patientBuilder, csvHelper, patientIdCell, Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_BHRUT_PAS_ID);

        CsvCell dob = parser.getDateOfBirth();
        if (!dob.isEmpty()) {
            patientBuilder.setDateOfBirth(dob.getDate(), dob);
        }

        CsvCell dod = parser.getDateOfDeath();
        if (!dod.isEmpty()) {
            patientBuilder.setDateOfDeath(dod.getDate(), dod);
        } else {
            patientBuilder.clearDateOfDeath();
        }

        CsvCell sex = parser.getGenderCode();
        if (!sex.isEmpty()) {
            VocSex sexEnum = VocSex.fromValue(sex.getString());
            Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(sexEnum);
            patientBuilder.setGender(gender, sex);
        }

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

        CsvCell ethnicCodeCell = parser.getEthnicityCode();
        if (ethnicCodeCell != null) {

            EthnicCategory ethnicCategory = convertEthnicCategory (ethnicCodeCell.getString());
            patientBuilder.setEthnicity(ethnicCategory, ethnicCodeCell);
        }

        CsvCell spineSensitive = parser.getSensitivePdsFlag();
        if (spineSensitive.getBoolean()) {
            patientBuilder.setSpineSensitive(true, spineSensitive);
        } else {
            patientBuilder.setSpineSensitive(false, spineSensitive);
        }

        //clear all care provider records, before we start adding more
        patientBuilder.clearCareProvider();

        //TODO - patient will need a managing organisation care provider.
        // will need pre-transforming using odscode look similar to Adastra method
        //Reference organisationReference = csvHelper.createOrganisationReference(odsCodeBhrut);
        //if (patientBuilder.isIdMapped()) {
        //    organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
        //}
        //patientBuilder.setManagingOrganisation(organisationReference);

        //TODO - registered GP coming soon on extract file.
        // will need pre-transforming using odscode look similar to Adastra PROVIDER method
        //Reference gpOrganisationReference = csvHelper.createOrganisationReference(odsCodeGPPractice);
        //if (patientBuilder.isIdMapped()) {
        //    gpOrganisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
        //}
        //patientBuilder.addCareProvider(gpOrganisationReference);

        return patientBuilder;
    }

    private static PatientBuilder getPatientBuilder(PMI parser, BhrutCsvHelper csvHelper) throws Exception {

        PatientBuilder ret = null;
        CsvCell patientIdCell = parser.getPasId();
        String uniqueId = csvHelper.createUniqueId(patientIdCell, null);
        org.hl7.fhir.instance.model.Patient existingResource
                = (org.hl7.fhir.instance.model.Patient) csvHelper.retrieveResource(uniqueId, ResourceType.Patient);
        if (existingResource != null) {
            ret = new PatientBuilder(existingResource);
        } else {
            ret = new PatientBuilder();
            csvHelper.setUniqueId(ret, patientIdCell, null);
        }

        return ret;
    }

    private static EthnicCategory convertEthnicCategory(String aliasNhsCdAlias) {

          //except for 99 or Unknown, which means "not stated"
        if (aliasNhsCdAlias.equalsIgnoreCase("99")
                || aliasNhsCdAlias.equalsIgnoreCase("Unknown") ) {
            return EthnicCategory.NOT_STATED;

        } else {
            return EthnicCategory.fromCode(aliasNhsCdAlias);
        }
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

        CsvCell givenName = parser.getForename();
        CsvCell surname = parser.getSurname();

        if (!givenName.isEmpty()
                || !surname.isEmpty()) {

            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
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


    private static RegistrationType findPreviousRegistrationType(BhrutCsvHelper csvHelper, CsvCell patientIdCell) throws Exception {

        String localId = csvHelper.createUniqueId(patientIdCell, null);
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

        String sourceId = csvHelper.createUniqueId(patientIdCell, null);


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

}
