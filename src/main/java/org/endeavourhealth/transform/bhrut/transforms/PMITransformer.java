package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
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


        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        if (dataUpdateStatusCell.getString().equalsIgnoreCase("Deleted")) {
            //we need to manually delete all dependant resources
            deleteEntirePatientRecord(fhirResourceFiler, csvHelper, parser.getCurrentState(), parser);
            return;
        }

        PatientBuilder patientBuilder = createPatientResource(parser, csvHelper, fhirResourceFiler);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder);
    }

    private static PatientBuilder createPatientResource(PMI parser,
                                                        BhrutCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

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

        Reference mainCauseOfDeathReference = null;
        CsvCell causeOfDeathCell = parser.getCauseOfDeath();
        if (!causeOfDeathCell.isEmpty()) {
            ObservationBuilder observationBuilder = new ObservationBuilder();
            ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(observationBuilder);
            containedParametersBuilder.removeContainedParameters();
            observationBuilder.setPatient(csvHelper.createPatientReference(parser.getPasId()));
            DateTimeType dateTimeType = new DateTimeType(parser.getDateOfDeath().getDateTime());
            observationBuilder.setEffectiveDate(dateTimeType, parser.getDateOfDeath());
            String obsId = parser.getID() + "CAUSEOFDEATH";
            containedParametersBuilder.addParameter(obsId, causeOfDeathCell.getString(), causeOfDeathCell);
            mainCauseOfDeathReference = csvHelper.createObservationReference(obsId, patientIdCell.getString());
        }

        CsvCell causeOfDeath1BCell = parser.getCauseOfDeath1B();
        if (!causeOfDeath1BCell.isEmpty()) {
            ObservationBuilder observationBuilder = new ObservationBuilder();
            ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(observationBuilder);
            containedParametersBuilder.removeContainedParameters();
            observationBuilder.setPatient(csvHelper.createPatientReference(parser.getPasId()));
            DateTimeType dateTimeType = new DateTimeType(parser.getDateOfDeath().getDateTime());
            observationBuilder.setEffectiveDate(dateTimeType, parser.getDateOfDeath());
            String obsId = parser.getID() + "CAUSEOFDEATH_1B";
            containedParametersBuilder.addParameter(obsId, causeOfDeath1BCell.getString(), causeOfDeath1BCell);
            observationBuilder.setParentResource(mainCauseOfDeathReference);
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
        }
        CsvCell causeOfDeath1CCell = parser.getCauseOfDeath1C();
        if (!causeOfDeath1CCell.isEmpty()) {
            ObservationBuilder observationBuilder = new ObservationBuilder();
            ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(observationBuilder);
            containedParametersBuilder.removeContainedParameters();
            observationBuilder.setPatient(csvHelper.createPatientReference(parser.getPasId()));
            DateTimeType dateTimeType = new DateTimeType(parser.getDateOfDeath().getDateTime());
            observationBuilder.setEffectiveDate(dateTimeType, parser.getDateOfDeath());
            String obsId = parser.getID() + "CAUSEOFDEATH_1c";
            containedParametersBuilder.addParameter(obsId, causeOfDeath1CCell.getString(), causeOfDeath1CCell);
            observationBuilder.setParentResource(mainCauseOfDeathReference);
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
        }
        CsvCell causeOfDeath2CCell = parser.getCauseOfDeath2();
        if (!causeOfDeath2CCell.isEmpty()) {
            ObservationBuilder observationBuilder = new ObservationBuilder();
            ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(observationBuilder);
            containedParametersBuilder.removeContainedParameters();
            observationBuilder.setPatient(csvHelper.createPatientReference(parser.getPasId()));
            DateTimeType dateTimeType = new DateTimeType(parser.getDateOfDeath().getDateTime());
            observationBuilder.setEffectiveDate(dateTimeType, parser.getDateOfDeath());
            String obsId = parser.getID() + "CAUSEOFDEATH_2";
            containedParametersBuilder.addParameter(obsId, causeOfDeath2CCell.getString(), causeOfDeath2CCell);
            observationBuilder.setParentResource(mainCauseOfDeathReference);
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
        }

        //Todo Need to verify the InfectionStatus
        CsvCell infectionStatusCell = parser.getInfectionStatus();
        if (!infectionStatusCell.isEmpty()) {
            ObservationBuilder observationBuilder = new ObservationBuilder();
            observationBuilder.setPatient(csvHelper.createPatientReference(parser.getPasId()));
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Component_Code);
            codeableConceptBuilder.setText(infectionStatusCell.getString());

        }

        CsvCell sex = parser.getGender();
        if (!sex.isEmpty()) {
            int genderCode = sex.getInt();
            switch (genderCode) {
                case 0:  patientBuilder.setGender(Enumerations.AdministrativeGender.UNKNOWN, sex);
                break;
                case 1:  patientBuilder.setGender(Enumerations.AdministrativeGender.MALE, sex);
                    break;
                case 2:  patientBuilder.setGender(Enumerations.AdministrativeGender.FEMALE, sex);
                    break;
                case 9:  patientBuilder.setGender(Enumerations.AdministrativeGender.UNKNOWN, sex);
                    break;
            }

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

            EthnicCategory ethnicCategory = convertEthnicCategory(ethnicCodeCell.getString());
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

        //set the managing organisation to Bhrut. This resource is created during PMIPreTransformer
        Reference organisationReference
                = csvHelper.createOrganisationReference(BhrutCsvToFhirTransformer.BHRUT_ORG_ODS_CODE);
        if (patientBuilder.isIdMapped()) {
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
        }
        patientBuilder.setManagingOrganisation(organisationReference);

        // set the patient's registered GP.  This resource is created during PMIPreTransformer
        CsvCell gpPracticeCode = parser.getRegisteredGpPracticeCode();
        if (!gpPracticeCode.isEmpty()) {
            Reference gpOrganisationReference = csvHelper.createOrganisationReference(gpPracticeCode.getString());
            if (patientBuilder.isIdMapped()) {
                gpOrganisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
            }
            patientBuilder.addCareProvider(gpOrganisationReference);
        }
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
                || aliasNhsCdAlias.equalsIgnoreCase("Unknown")) {
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


        }
    }

    private static void createContact(PatientBuilder patientBuilder, BhrutCsvHelper csvHelper, CsvCell cell,
                                      ContactPoint.ContactPointUse use, ContactPoint.ContactPointSystem system) throws Exception {

        if (!cell.isEmpty()) {

            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(use);
            contactPointBuilder.setSystem(system);
            contactPointBuilder.setValue(cell.getString(), cell);

          }
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
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();

        String sourceId = csvHelper.createUniqueId(patientIdCell, null);


        List<Resource> resources = csvHelper.retrieveAllResourcesForPatient(sourceId, fhirResourceFiler);
        if (resources == null) {
            return;
        }

        for (Resource resource : resources) {

            //wrap the resource in generic builder so we can save it
            GenericBuilder genericBuilder = new GenericBuilder(resource);
            genericBuilder.setDeletedAudit(dataUpdateStatusCell);
            fhirResourceFiler.deletePatientResource(currentState, false, genericBuilder);
        }
    }

}
