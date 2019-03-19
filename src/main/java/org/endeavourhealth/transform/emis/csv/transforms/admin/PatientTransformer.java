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

public class PatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Patient.class);
        while (parser.nextRecord()) {

            try {
                createResources((Patient) parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResources(Patient parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper,
                                      String version) throws Exception {

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            //Emis send us a delete for a patient WITHOUT a corresponding delete for all other data, so
            //we need to manually delete all dependant resources
            CsvCell patientGuid = parser.getPatientGuid();
            deleteEntirePatientRecord(fhirResourceFiler, csvHelper, parser.getCurrentState(), patientGuid, deletedCell);
            return;
        }

        //this transform creates two resources
        PatientBuilder patientBuilder = createPatientResource(parser, csvHelper, version);
        EpisodeOfCareBuilder episodeBuilder = createEpisodeResource(parser, csvHelper, fhirResourceFiler);

        //save both resources together, so the patient is defintiely saved before the episode
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder);
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

        CsvCell patientType = parser.getPatientTypedescription();
        CsvCell dummyType = parser.getDummyType();
        RegistrationType registrationType = convertRegistrationType(patientType.getString(), dummyType.getBoolean(), parser);
        episodeBuilder.setRegistrationType(registrationType, patientType, dummyType);

        episodeBuilder.setRegistrationStartDate(regDateCell.getDate(), regDateCell);

        //we store the latest start date for each patient, so see if it's changed since last transform.
        //If so, this means a patient was deducted and re-registered on the same day, so we need to manually
        //end the previous episode.
        CsvCell previousStartDate = csvHelper.getLatestEpisodeStartDate(patientGuid);
        if (previousStartDate != null) {
            String previousStr = previousStartDate.getString();
            String currentStr = regDateCell.getString();
            //we have to compare as Strings because can't call getDate() on a dummy cell
            if (!previousStr.equals(currentStr)) {
                endLastEpisodeOfCare(patientGuid, previousStartDate, regDateCell, fhirResourceFiler, csvHelper);
            }
        }

        //and cache the start date in the helper since we'll need this when linking Encounters to Episodes
        csvHelper.cacheLatestEpisodeStartDate(patientGuid, regDateCell);

        CsvCell dedDateCell = parser.getDateOfDeactivation();
        if (!dedDateCell.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(dedDateCell.getDate(), dedDateCell);
        }

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
     * if we detect the patient was deducted and re-registered on the same day, then we want to manually end
     * the previous Episode, setting the end date to the new start date
     */
    private static void endLastEpisodeOfCare(CsvCell patientGuid, CsvCell previousStartDate, CsvCell newStartDate,
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
    }

    private static PatientBuilder createPatientResource(Patient parser, EmisCsvHelper csvHelper, String version) throws Exception {

        PatientBuilder patientBuilder = new PatientBuilder();

        CsvCell patientGuid = parser.getPatientGuid();
        EmisCsvHelper.setUniqueId(patientBuilder, patientGuid, null);

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

        CsvCell organisationGuid = parser.getOrganisationGuid();
        Reference organisationReference = csvHelper.createOrganisationReference(organisationGuid);
        patientBuilder.setManagingOrganisation(organisationReference, organisationGuid);

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

        //the care provider field on the patient is ONLY for the patients usual GP, so only set the Emis usual
        //GP field in it if the patient is a GMS patient, otherwise use the "external" GP fields on the parser
        CsvCell usualGpGuid = parser.getUsualGpUserInRoleGuid();
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

        transformEthnicityAndMaritalStatus(patientBuilder, patientGuid, csvHelper);

        //patient is active if they're not deducted. We only get one record in this file for a patient's
        //most recent registration, so it's safe to just use this deduction date
        CsvCell dedDate = parser.getDateOfDeactivation();
        boolean active = dedDate.isEmpty() || dedDate.getDate().after(new Date());
        patientBuilder.setActive(active, dedDate);

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            //add the confidential flag to BOTH resources
            patientBuilder.setConfidential(true, confidential);
        }

        return patientBuilder;
    }


    /**
     * Emis send us a delete for a patient WITHOUT a corresponding delete for all other data, so
     * we need to manually delete all dependant resources
     */
    private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler,
                                                  EmisCsvHelper csvHelper,
                                                  CsvCurrentState currentState,
                                                  CsvCell patientGuidCell,
                                                  CsvCell deletedCell) throws Exception {

        //retrieve any resources that exist for the patient
        String sourceId = EmisCsvHelper.createUniqueId(patientGuidCell, null);
        List<Resource> resources = csvHelper.retrieveAllResourcesForPatient(sourceId, fhirResourceFiler);
        if (resources == null) {
            return;
        }

        for (Resource resource : resources) {

            //do not delete Appointment resources either. If Emis delete and subsequently un-delete a patient
            //they do not re-send the Appointments, so we shouldn't delete them in the first place.
            if (resource.getResourceType() == ResourceType.Appointment) {
                continue;
            }

            //wrap the resource in generic builder so we can delete it
            GenericBuilder genericBuilder = new GenericBuilder(resource);
            genericBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(currentState, false, genericBuilder);
        }
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

        } else {
            if (TransformConfig.instance().isEmisAllowUnmappedRegistrationTypes()) {
                TransformWarnings.log(LOG, parserI, "Unhandled Emis registration type {}", csvRegType);
                return RegistrationType.OTHER;

            } else {
                throw new TransformException("Unsupported registration type " + csvRegType);
            }
        }

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

         */
    }

}
