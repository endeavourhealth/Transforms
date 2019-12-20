package org.endeavourhealth.transform.emis.csv.transforms.admin;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.*;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.CodeAndDate;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisMappingHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Patient;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.TransformException;
import java.util.*;

public class PatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Patient.class);
        while (parser != null && parser.nextRecord()) {

            try {
                createResources((Patient) parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResources(Patient parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper) throws Exception {

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            //Emis send us a delete for a patient WITHOUT a corresponding delete for all other data, so
            //we need to manually delete all dependant resources
            deleteEntirePatientRecord(fhirResourceFiler, csvHelper, parser);
            return;
        }

        //this transform creates two resources
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

    private static EpisodeOfCareBuilder createEpisodeResource(Patient parser, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();

        //factor the registration start date into the unique ID for the episode, so a change in registration start
        //date correctly results in a new episode of care being created
        CsvCell patientGuid = parser.getPatientGuid();
        CsvCell regDateCell = parser.getDateOfRegistration();
        regDateCell = fixRegDateCell(regDateCell);
        EmisCsvHelper.setUniqueId(episodeBuilder, patientGuid, regDateCell);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        episodeBuilder.setPatient(patientReference, patientGuid);

        //create a second reference, since it's not an immutable object
        CsvCell organisationGuid = parser.getOrganisationGuid();
        Reference organisationReference = csvHelper.createOrganisationReference(organisationGuid);
        episodeBuilder.setManagingOrganisation(organisationReference, organisationGuid);

        CsvCell patientType = parser.getPatientTypeDescription();
        CsvCell dummyType = parser.getDummyType();
        RegistrationType registrationType = convertRegistrationType(patientType.getString(), dummyType.getBoolean(), parser);
        episodeBuilder.setRegistrationType(registrationType, patientType, dummyType);

        episodeBuilder.setRegistrationStartDate(regDateCell.getDate(), regDateCell);

        //and cache the start date in the helper since we'll need this when linking Encounters to Episodes
        //note we must do this AFTER the above check, otherwise we'll fail to end episodes when patients are deducted and re-register on the same day
        csvHelper.cacheLatestEpisodeStartDate(patientGuid, regDateCell);

        CsvCell dedDateCell = parser.getDateOfDeactivation();
        if (!dedDateCell.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(dedDateCell.getDate(), dedDateCell);
        }

        endOtherEpisodes(patientGuid, regDateCell, fhirResourceFiler);

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            //add the confidential flag to BOTH resources
            episodeBuilder.setConfidential(true, confidential);
        }

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

        //and carry over any registration statuses that we've received in our custom extract
        List<List_.ListEntryComponent> statusList = csvHelper.getExistingRegistrationStatuses(episodeBuilder.getResourceId());
        if (statusList != null) {
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);

            for (List_.ListEntryComponent status: statusList) {
                CodeableConcept codeableConcept = status.getFlag();
                containedListBuilder.addCodeableConcept(codeableConcept);
                if (status.hasDate()) {
                    Date d = status.getDate();
                    containedListBuilder.addDateToLastItem(d);
                }
            }
        }

        return episodeBuilder;
    }

    /**
     * the test pack includes a handful of records with a missing reg start date. This never happens
     * with live data. The transform requires a start date, as we use that as part of the unique key.
     * To get around this, just use a dummy date for the cell.
     */
    private static CsvCell fixRegDateCell(CsvCell regDateCell) {
        if (!regDateCell.isEmpty()) {
            return regDateCell;
        }

        return CsvCell.factoryWithNewValue(regDateCell, "1900-01-01");
    }

    /**
     * if a patient was deducted and re-registered on the same day, we don't ever receive the end date for the previous
     * registration, so we need to manually check for any active episode with a different start date and end them
     */
    private static void endOtherEpisodes(CsvCell patientGuidCell, CsvCell thisStartDateCell, FhirResourceFiler fhirResourceFiler) throws Exception {

        String sourceId = EmisCsvHelper.createUniqueId(patientGuidCell, null);
        UUID globallyUniqueId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, sourceId);
        if (globallyUniqueId == null) {
            return;
        }

        Date thisStartDate = thisStartDateCell.getDate();

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> episodeWrappers = resourceDal.getResourcesByPatient(fhirResourceFiler.getServiceId(), globallyUniqueId, ResourceType.EpisodeOfCare.toString());

        for (ResourceWrapper episodeWrapper: episodeWrappers) {
            EpisodeOfCare episode = (EpisodeOfCare)FhirSerializationHelper.deserializeResource(episodeWrapper.getResourceData());
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


    /**
     * if we detect the patient was deducted and re-registered on the same day, then we want to manually end
     * the previous Episode, setting the end date to the new start date
     */
    /*private static void endLastEpisodeOfCare(CsvCell patientGuid, CsvCell previousStartDate, CsvCell newStartDate,
                                             FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper) throws Exception {

        String sourceId = EmisCsvHelper.createUniqueId(patientGuid, previousStartDate);
        EpisodeOfCare lastEpisode = (EpisodeOfCare)csvHelper.retrieveResource(sourceId, ResourceType.EpisodeOfCare);

        EpisodeOfCareBuilder builder = new EpisodeOfCareBuilder(lastEpisode);
        if (builder.getRegistrationEndDate() != null) {
            //if we were informed of the previous deduction, then nothing for us to do
            return;
        }

        builder.setRegistrationEndDate(newStartDate.getDate(), newStartDate);

        fhirResourceFiler.savePatientResource(null, false, builder);
    }*/

    private static PatientBuilder createPatientResource(Patient parser, EmisCsvHelper csvHelper) throws Exception {

        PatientBuilder patientBuilder = getPatientBuilder(parser, csvHelper);

        CsvCell nhsNumberCell = parser.getNhsNumber();
        createIdentifier(patientBuilder, csvHelper, nhsNumberCell, Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);

        //store the patient GUID and patient number to the patient resource
        CsvCell patientGuidCell = parser.getPatientGuid();
        createIdentifier(patientBuilder, csvHelper, patientGuidCell, Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_PATIENT_GUID);

        CsvCell patientNumberCell = parser.getPatientNumber();
        createIdentifier(patientBuilder, csvHelper, patientNumberCell, Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_PATIENT_NUMBER);

        CsvCell dobCell = parser.getDateOfBirth();
        patientBuilder.setDateOfBirth(dobCell.getDate(), dobCell);

        CsvCell dodCell = parser.getDateOfDeath();
        if (!dodCell.isEmpty()) {
            patientBuilder.setDateOfDeath(dodCell.getDate(), dodCell);
        } else {
            patientBuilder.clearDateOfDeath();
        }

        //EMIS provides "sex" and FHIR requires "gender", but will treat as the same concept for this transformation
        CsvCell sex = parser.getSex();
        VocSex sexEnum = VocSex.fromValue(sex.getString());
        Enumerations.AdministrativeGender gender = SexConverter.convertSexToFhir(sexEnum);
        patientBuilder.setGender(gender, sex);

        transformName(patientBuilder, parser, csvHelper);

        //we need to know the registration type to work out the address use
        CsvCell patientType = parser.getPatientTypeDescription();
        CsvCell dummyType = parser.getDummyType();

        //dummy flag can be one of two places
        if (dummyType.getBoolean()
                || patientType.getString().equalsIgnoreCase("Dummy")) {
            patientBuilder.setTestPatient(true, dummyType, patientType);
        } else {
            patientBuilder.setTestPatient(false, dummyType, patientType);
        }

        RegistrationType registrationType = convertRegistrationType(patientType.getString(), dummyType.getBoolean(), parser);
        transformAddress(patientBuilder, parser, csvHelper, registrationType);

        CsvCell homePhone = parser.getHomePhone();
        createContactPoint(patientBuilder, csvHelper, homePhone, ContactPoint.ContactPointUse.HOME, ContactPoint.ContactPointSystem.PHONE);

        CsvCell mobilePhone = parser.getMobilePhone();
        createContactPoint(patientBuilder, csvHelper, mobilePhone, ContactPoint.ContactPointUse.MOBILE, ContactPoint.ContactPointSystem.PHONE);

        CsvCell email = parser.getEmailAddress();
        createContactPoint(patientBuilder, csvHelper, email, ContactPoint.ContactPointUse.HOME, ContactPoint.ContactPointSystem.EMAIL);

        transformCarer(patientBuilder, parser, csvHelper);
        transformCareProvider(patientBuilder, parser, csvHelper, registrationType);
        transformEthnicityAndMaritalStatus(patientBuilder, patientGuidCell, csvHelper);

        CsvCell residentialInstituteCodeCell = parser.getResidentialInstituteCode();
        if (!residentialInstituteCodeCell.isEmpty()) {
            patientBuilder.setResidentialInstituteCode(residentialInstituteCodeCell.getString(), residentialInstituteCodeCell);
        } else {
            patientBuilder.setResidentialInstituteCode(null, residentialInstituteCodeCell);
        }

        CsvCell nhsNumberStatusCell = parser.getNHSNumberStatus();
        if (!nhsNumberStatusCell.isEmpty()) {
            NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(nhsNumberStatusCell.getString());
            patientBuilder.setNhsNumberVerificationStatus(verificationStatus, nhsNumberStatusCell);

        } else {
            patientBuilder.setNhsNumberVerificationStatus(null, nhsNumberStatusCell);
        }

        CsvCell organisationGuid = parser.getOrganisationGuid();
        Reference organisationReference = csvHelper.createOrganisationReference(organisationGuid);
        if (patientBuilder.isIdMapped()) {
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
        }
        patientBuilder.setManagingOrganisation(organisationReference, organisationGuid);

        CsvCell spineSensitive = parser.getSpineSensitive();
        if (spineSensitive.getBoolean()) {
            patientBuilder.setSpineSensitive(true, spineSensitive);
        } else {
            patientBuilder.setSpineSensitive(false, spineSensitive);
        }


        //patient is active if they're not deducted. We only get one record in this file for a patient's
        //most recent registration, so it's safe to just use this deduction date
        CsvCell dedDate = parser.getDateOfDeactivation();
        boolean active = dedDate.isEmpty() || dedDate.getDate().after(new Date());
        patientBuilder.setActive(active, dedDate);

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            //add the confidential flag to BOTH resources
            patientBuilder.setConfidential(true, confidential);
        } else {
            patientBuilder.setConfidential(false, confidential);
        }

        return patientBuilder;
    }

    private static void transformName(PatientBuilder patientBuilder, Patient parser, EmisCsvHelper csvHelper) throws Exception {

        CsvCell titleCell = parser.getTitle();
        CsvCell givenNameCell = parser.getGivenName();
        CsvCell middleNamesCell = parser.getMiddleNames();
        CsvCell surnameCell = parser.getSurname();
        if (!titleCell.isEmpty()
                || !givenNameCell.isEmpty()
                || !middleNamesCell.isEmpty()
                || !surnameCell.isEmpty()) {

            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            nameBuilder.addPrefix(titleCell.getString(), titleCell);
            nameBuilder.addGiven(givenNameCell.getString(), givenNameCell);
            nameBuilder.addGiven(middleNamesCell.getString(), middleNamesCell);
            nameBuilder.addFamily(surnameCell.getString(), surnameCell);

            //make sure to de-duplicate the name if we've just added a new instance with the same value
            NameBuilder.deDuplicateLastName(patientBuilder, csvHelper.getDataDate());

        } else {
            //not sure why we'd ever have no name fields, but handle it anyway, and end any existing name record
            NameBuilder.endNames(patientBuilder, csvHelper.getDataDate(), HumanName.NameUse.OFFICIAL);
        }

    }

    private static void transformAddress(PatientBuilder patientBuilder, Patient parser, EmisCsvHelper csvHelper, RegistrationType registrationType) throws Exception {
        //if the patient is a temp patient, then the address supplied will be the temporary address,
        //rather than home. Emis Web stores the home address for these patients in a table we don't get in the extract
        Address.AddressUse use = null;
        if (registrationType != null
                && registrationType == RegistrationType.TEMPORARY) {
            use = Address.AddressUse.TEMP;
        } else {
            use = Address.AddressUse.HOME;
        }

        CsvCell houseNameFlatCell = parser.getHouseNameFlatNumber();
        CsvCell numberAndStreetCell = parser.getNumberAndStreet();
        CsvCell villageCell = parser.getVillage();
        CsvCell townCell = parser.getTown();
        CsvCell countyCell = parser.getCounty();
        CsvCell postcodeCell = parser.getPostcode();

        if (!houseNameFlatCell.isEmpty()
                || !numberAndStreetCell.isEmpty()
                || !villageCell.isEmpty()
                || !townCell.isEmpty()
                || !countyCell.isEmpty()
                || !postcodeCell.isEmpty()) {

            AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
            addressBuilder.setUse(use);
            addressBuilder.addLine(houseNameFlatCell.getString(), houseNameFlatCell);
            addressBuilder.addLine(numberAndStreetCell.getString(), numberAndStreetCell);
            addressBuilder.addLine(villageCell.getString(), villageCell);
            addressBuilder.setCity(townCell.getString(), townCell);
            addressBuilder.setDistrict(countyCell.getString(), countyCell);
            addressBuilder.setPostcode(postcodeCell.getString(), postcodeCell);

            //make sure to de-duplicate the name if we've just added a new instance with the same value
            AddressBuilder.deDuplicateLastAddress(patientBuilder, csvHelper.getDataDate());

        } else {
            AddressBuilder.endAddresses(patientBuilder, csvHelper.getDataDate(), use);
        }

    }

    private static void transformCarer(PatientBuilder patientBuilder, Patient parser, EmisCsvHelper csvHelper) throws Exception {

        CsvCell carerNameCell = parser.getCarerName();
        CsvCell carerRelationshipCell = parser.getCarerRelation();
        if (!carerNameCell.isEmpty() || !carerRelationshipCell.isEmpty()) {

            //add a new empty contact object to the patient which the following lines will populate
            PatientContactBuilder contactBuilder = new PatientContactBuilder(patientBuilder);

            if (!carerNameCell.isEmpty()) {
                HumanName humanName = NameHelper.convert(carerNameCell.getString());
                contactBuilder.addContactName(humanName, carerNameCell);
            }

            List<String> relationshipTypes = new ArrayList<>();
            relationshipTypes.add("Carer"); //by definition, this is a carer relationship

            if (!carerRelationshipCell.isEmpty()) {
                //FHIR spec states that we should map to their relationship types if possible, but if
                //not possible, then send as a textual codeable concept
                relationshipTypes.add(carerRelationshipCell.getString());
            }

            String typeStr = String.join(", ", relationshipTypes);
            contactBuilder.setRelationship(typeStr, carerRelationshipCell);

            //make sure to de-duplicate the name if we've just added a new instance with the same value
            PatientContactBuilder.deDuplicateLastPatientContact(patientBuilder, csvHelper.getDataDate());

        } else {
            //if cell is empty, make sure to end any previous records
            PatientContactBuilder.endPatientContacts(patientBuilder, csvHelper.getDataDate());
        }

    }

    private static void transformCareProvider(PatientBuilder patientBuilder, Patient parser, EmisCsvHelper csvHelper, RegistrationType registrationType) throws Exception {

        //clear all care provider records, before we start adding more
        patientBuilder.clearCareProvider();

        //if the patient is GMS, then their registered practice is here and their usual GP is in the usual GP field
        if (registrationType == RegistrationType.REGULAR_GMS) {

            CsvCell orgCell = parser.getOrganisationGuid();
            Reference orgReference = csvHelper.createOrganisationReference(orgCell);
            if (patientBuilder.isIdMapped()) {
                orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference, csvHelper);
            }
            patientBuilder.addCareProvider(orgReference, orgCell);

            //the care provider field on the patient is ONLY for the patients usual GP, so only set the Emis usual
            //GP field in it if the patient is a GMS patient, otherwise use the "external" GP fields on the parser
            CsvCell usualGpGuidCell = parser.getUsualGpUserInRoleGuid();
            if (!usualGpGuidCell.isEmpty()) {

                Reference reference = csvHelper.createPractitionerReference(usualGpGuidCell);
                if (patientBuilder.isIdMapped()) {
                    reference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, csvHelper);
                }
                patientBuilder.addCareProvider(reference, usualGpGuidCell);
            }

        } else {
            //if the patient isn't GMS then look to the "external" fields

            //have to handle the mis-spelling of the column name in EMIS test pack
            //String externalOrgGuid = patientParser.getExternalUsualGPOrganisation();
            CsvCell externalOrgGuidCell = null;
            if (parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_0)
                    || parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
                externalOrgGuidCell = parser.getExternalUsusalGPOrganisation();
            } else {
                externalOrgGuidCell = parser.getExternalUsualGPOrganisation();
            }

            if (!externalOrgGuidCell.isEmpty()) {
                Reference reference = csvHelper.createOrganisationReference(externalOrgGuidCell);
                if (patientBuilder.isIdMapped()) {
                    reference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, csvHelper);
                }
                patientBuilder.addCareProvider(reference, externalOrgGuidCell);
            }

            CsvCell externalGpGuidCell = parser.getExternalUsualGPGuid();
            if (!externalGpGuidCell.isEmpty()) {
                Reference reference = csvHelper.createPractitionerReference(externalGpGuidCell);
                if (patientBuilder.isIdMapped()) {
                    reference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, csvHelper);
                }
                patientBuilder.addCareProvider(reference, externalGpGuidCell);
            }
        }
    }

    private static void createContactPoint(PatientBuilder patientBuilder, EmisCsvHelper csvHelper, CsvCell cell,
                                           ContactPoint.ContactPointUse use, ContactPoint.ContactPointSystem system) throws Exception {

        if (!cell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setUse(use);
            contactPointBuilder.setSystem(system);
            contactPointBuilder.setValue(cell.getString(), cell);

            //make sure to de-duplicate the name if we've just added a new instance with the same value
            ContactPointBuilder.deDuplicateLastContactPoint(patientBuilder, csvHelper.getDataDate());

        } else {
            //if cell is empty, make sure to end any previous records
            ContactPointBuilder.endContactPoints(patientBuilder, csvHelper.getDataDate(), system, use);
        }

    }

    private static void createIdentifier(PatientBuilder patientBuilder, EmisCsvHelper csvHelper, CsvCell cell,
                                         Identifier.IdentifierUse use, String system) throws Exception {

        if (!cell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(use);
            identifierBuilder.setSystem(system);
            identifierBuilder.setValue(cell.getString(), cell);

            //make sure to de-duplicate the identifier if we've just added a new instance with the same value
            IdentifierBuilder.deDuplicateLastIdentifier(patientBuilder, csvHelper.getDataDate());

        } else {
            //if cell is empty, make sure to end any previous records
            IdentifierBuilder.endIdentifiers(patientBuilder, csvHelper.getDataDate(), system, use);
        }
    }

    private static PatientBuilder getPatientBuilder(Patient parser, EmisCsvHelper csvHelper) throws Exception {

        PatientBuilder ret = null;

        CsvCell patientGuid = parser.getPatientGuid();
        String uniqueId = csvHelper.createUniqueId(patientGuid, null);
        org.hl7.fhir.instance.model.Patient existingResource = (org.hl7.fhir.instance.model.Patient)csvHelper.retrieveResource(uniqueId, ResourceType.Patient);
        if (existingResource != null) {
            ret = new PatientBuilder(existingResource);
        } else {
            ret = new PatientBuilder();
            EmisCsvHelper.setUniqueId(ret, patientGuid, null);
        }

        return ret;
    }


    /**
     * Emis send us a delete for a patient WITHOUT a corresponding delete for all other data, so
     * we need to manually delete all dependant resources
     */
    private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler,
                                                  EmisCsvHelper csvHelper,
                                                  Patient parser) throws Exception {
        //Emis send us rolling deletes for patients a year after deduction or death, which we want to ignore
        if (!shouldProcessDelete(parser, csvHelper)) {
            return;
        }

        CsvCurrentState currentState = parser.getCurrentState();
        CsvCell patientGuidCell = parser.getPatientGuid();
        CsvCell deletedCell = parser.getDeleted();

        //retrieve any resources that exist for the patient
        String sourceId = EmisCsvHelper.createUniqueId(patientGuidCell, null);
        List<Resource> resources = csvHelper.retrieveAllResourcesForPatient(sourceId, fhirResourceFiler);
        if (resources == null) {
            return;
        }

        for (Resource resource : resources) {

            ResourceType resourceType = resource.getResourceType();
            if (resourceType == ResourceType.Appointment) {
                //do not delete Appointment resources either. If Emis delete and subsequently un-delete a patient
                //they do not re-send the Appointments, so we shouldn't delete them in the first place.
                continue;

            } else if (resourceType == ResourceType.Encounter) {
                //if we're re-processing a limited file set, then only delete this kind of resource if we're doing those files
                if (!csvHelper.getParsers().containsKey(org.endeavourhealth.transform.emis.csv.schema.careRecord.Consultation.class)) {
                    continue;
                }

            } else if (resourceType == ResourceType.ProcedureRequest) {
                //if we're re-processing a limited file set, then only delete this kind of resource if we're doing those files
                if (!csvHelper.getParsers().containsKey(org.endeavourhealth.transform.emis.csv.schema.careRecord.Diary.class)) {
                    continue;
                }

            } else if (resourceType == ResourceType.Observation
                    || resourceType == ResourceType.Condition
                    || resourceType == ResourceType.Procedure
                    || resourceType == ResourceType.AllergyIntolerance
                    || resourceType == ResourceType.FamilyMemberHistory
                    || resourceType == ResourceType.Immunization
                    || resourceType == ResourceType.DiagnosticOrder
                    || resourceType == ResourceType.DiagnosticReport
                    || resourceType == ResourceType.Specimen
                    || resourceType == ResourceType.ReferralRequest) {

                //if we're re-processing a limited file set, then only delete this kind of resource if we're doing those files
                if (!csvHelper.getParsers().containsKey(org.endeavourhealth.transform.emis.csv.schema.careRecord.Observation.class)) {
                    continue;
                }

            } else if (resourceType == ResourceType.MedicationOrder) {

                //if we're re-processing a limited file set, then only delete this kind of resource if we're doing those files
                if (!csvHelper.getParsers().containsKey(org.endeavourhealth.transform.emis.csv.schema.prescribing.IssueRecord.class)) {
                    continue;
                }

            } else if (resourceType == ResourceType.MedicationStatement) {

                //if we're re-processing a limited file set, then only delete this kind of resource if we're doing those files
                if (!csvHelper.getParsers().containsKey(org.endeavourhealth.transform.emis.csv.schema.prescribing.DrugRecord.class)) {
                    continue;
                }

            } else if (resourceType == ResourceType.EpisodeOfCare
                    || resourceType == ResourceType.Patient) {
                //let these resources be deleted, since they come from this file

            } else {
                throw new Exception("Unexpected resource type " + resourceType);
            }

            //wrap the resource in generic builder so we can delete it
            GenericBuilder genericBuilder = new GenericBuilder(resource);
            genericBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(currentState, false, genericBuilder);
        }
    }

    /**
     * works out if a delete instruction should be ignored
     */
    private static boolean shouldProcessDelete(Patient parser, EmisCsvHelper csvHelper) throws Exception {

        //if the patient is neither deducted or deceased, then don't ignore the delete
        String patientId = csvHelper.createUniqueId(parser.getPatientGuid(), null);
        org.hl7.fhir.instance.model.Patient patient = (org.hl7.fhir.instance.model.Patient)csvHelper.retrieveResource(patientId, ResourceType.Patient);

        //if we've never transformed the patient or it's already deleted, then we could ignore it, but
        //it seems safter to just process the delete
        if (patient == null) {
            //LOG.debug("Patient is null, so process");
            return true;
        }

        //if the patient is active (i.e. not deducted) and not deceased, then we should process the delete
        if (patient.getActive()
                && patient.getDeceased() == null) {
            //LOG.debug("Patient active = " + (patient.getActive()) + ", deceased = " + patient.getDeceased() + ", so process");
            return true;
        }

        //if the patient is deducted or deleted, then only process if the extract is disabled
        //and we'll only be allowed to process a deleted extract if we know the service is actually finished with DDS
        if (csvHelper.isSharingAgreementDisabled()) {
            //.debug("Sharing agreement is NOT disabled, so process");
            return true;
        }

        //if we make it to here, then we want to allow the delete to happen
        LOG.info("Ignoring delete for patient " + patientId);
        return false;
    }

    /*private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper,
                                                  CsvCurrentState currentState,
                                                  PatientBuilder patientBuilder, EpisodeOfCareBuilder episodeBuilder,
                                                  CsvCell deletedCell) throws Exception {

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
                    genericBuilder.setDeletedAudit(deletedCell);
                    fhirResourceFiler.deletePatientResource(currentState, false, genericBuilder);
                }
            }
        }

        //and delete the patient and episode
        patientBuilder.setDeletedAudit(deletedCell);
        episodeBuilder.setDeletedAudit(deletedCell);
        fhirResourceFiler.deletePatientResource(currentState, patientBuilder, episodeBuilder);
    }*/


    private static void transformEthnicityAndMaritalStatus(PatientBuilder patientBuilder,
                                                           CsvCell patientGuid,
                                                           EmisCsvHelper csvHelper) throws Exception {

        CodeAndDate newEthnicity = csvHelper.findEthnicity(patientGuid);
        CodeAndDate newMaritalStatus = csvHelper.findMaritalStatus(patientGuid);

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

        //don't assign a reg type to any test patient records we get
        if (dummyRecord) {
            return null;
        }

        //EMIS both test and Live data has leading spaces
        csvRegType = csvRegType.trim();
        return EmisMappingHelper.findRegistrationType(csvRegType);
    }
    /*private static RegistrationType convertRegistrationType(String csvRegType, boolean dummyRecord, ParserI parserI) throws Exception {

        //EMIS both test and Live data has leading spaces
        csvRegType = csvRegType.trim();

        if (dummyRecord || csvRegType.equalsIgnoreCase("Dummy")) {
            //dummy registration types are now handled in the Patient resource, as the test-patient extension
            return null;
            //return RegistrationType.DUMMY;
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
        } else if (csvRegType.equalsIgnoreCase("Minor Surgery")) {
            return RegistrationType.MINOR_SURGERY;
        } else if (csvRegType.equalsIgnoreCase("Child Health Services")) {
            return RegistrationType.CHILD_HEALTH_SURVEILLANCE;
        } else if (csvRegType.equalsIgnoreCase("Contraceptive Services")) {
            return RegistrationType.CONTRACEPTIVE_SERVICES;
        } else if (csvRegType.equalsIgnoreCase("Maternity Services")) {
            return RegistrationType.MATERNITY_SERVICES;
        } else if (csvRegType.equalsIgnoreCase("Yellow Fever")) {
            return RegistrationType.YELLOW_FEVER;
        } else if (csvRegType.equalsIgnoreCase("Pre Registration")) {
            return RegistrationType.PRE_REGISTRATION;
        } else if (csvRegType.equalsIgnoreCase("Sexual Health")) {
            return RegistrationType.SEXUAL_HEALTH;
        } else if (csvRegType.equalsIgnoreCase("Vasectomy")) {
            return RegistrationType.VASECTOMY;
        } else if (csvRegType.equalsIgnoreCase("Out of Hours")) {
            return RegistrationType.OUT_OF_HOURS;
        } else if (csvRegType.equalsIgnoreCase("Rehabilitation")) {
            return RegistrationType.REHABILITATION;

        } else {
            if (TransformConfig.instance().isEmisAllowUnmappedRegistrationTypes()) {
                TransformWarnings.log(LOG, parserI, "Unhandled Emis registration type {}", csvRegType);
                return RegistrationType.OTHER;

            } else {
                throw new TransformException("Unsupported registration type " + csvRegType);
            }
        }*/

        /**
         * This is the FULL list of registration types from Emis Web
         *
         * THESE ONES ARE KNOWN TO BE USED
         Immediately Necessary
         Private
         Regular
         Temporary
         Community Registered
         Dummy
         Other
         Walk-In Patient
         Minor Surgery
         Child Health Services
         Contraceptive Services
         Maternity Services
         Yellow Fever
         Sexual Health
         Pre Registration
         Out of Hours
         Vasectomy

         THESE ONES TECHNICALLY EXIST BUT NO PATIENTS HAVE THEM
         Dermatology
         Diabetic
         Rheumatology
         Chiropody
         Coronary Health Checks
         Ultrasound
         BCG Clinic
         Acupuncture
         Reflexology
         Hypnotherapy
         Rehabilitation
         Antenatal
         Audiology
         Gynaecology
         Doppler
         Secondary Registration
         Urgent and Emergency Care
         Externally Registered

    }*/
}
