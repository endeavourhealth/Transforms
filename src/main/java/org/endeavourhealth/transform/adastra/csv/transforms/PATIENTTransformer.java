package org.endeavourhealth.transform.adastra.csv.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.PATIENT;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class PATIENTTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PATIENTTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(PATIENT.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((PATIENT) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(PATIENT parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell patientId = parser.getPatientId();
        CsvCell caseId = parser.getCaseId();

        //does the patient have a Case record?
        CsvCell casePatientId = csvHelper.findCasePatient(caseId.getString());
        if (casePatientId == null || !patientId.getString().equalsIgnoreCase(casePatientId.getString())) {
            TransformWarnings.log(LOG, parser, "No Case record match found for patient: {}, case {},  file: {}",
                    patientId.getString(), caseId.getString(), parser.getFilePath());
            return;
        }

        //has the patient already been created within this session - check the cache before it is removed?
        boolean patientCreatedInSession = csvHelper.getPatientCache().patientInCache(patientId);

        //get EpisodeofCare already populated from preceeding CASE transform
        EpisodeOfCareBuilder episodeBuilder
                = csvHelper.getEpisodeOfCareCache().getOrCreateEpisodeOfCareBuilder(caseId, csvHelper, fhirResourceFiler);

        //get or create Patient Resource builder.  If two patients in same file, first already created, so retrieve from cache
        PatientBuilder patientBuilder
                = csvHelper.getPatientCache().getOrCreatePatientBuilder(patientId, csvHelper, fhirResourceFiler);

        CsvCell nhsNumber = parser.getNHSNumber();
        if (!nhsNumber.isEmpty()) {

            //remove existing NHS number to prevent duplicate filing error
            IdentifierBuilder.removeExistingIdentifiersForSystem(patientBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);

            String nhsNumberValue = nhsNumber.getString();
            nhsNumberValue = nhsNumberValue.replace(" ", "");

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setValue(nhsNumberValue, nhsNumber);

            CsvCell nhsNumberTraceStatus = parser.getNHSNoTraceStatus();
            if (!nhsNumberTraceStatus.isEmpty()) {

                if (nhsNumberTraceStatus.getString().equalsIgnoreCase("V")) {
                    patientBuilder.setNhsNumberVerificationStatus(NhsNumberVerificationStatus.PRESENT_AND_VERIFIED, nhsNumberTraceStatus);
                } else {
                    patientBuilder.setNhsNumberVerificationStatus(NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED, nhsNumberTraceStatus);
                }
            }
        } else {
            patientBuilder.setNhsNumberVerificationStatus(NhsNumberVerificationStatus.NUMBER_NOT_PRESENT_NO_TRACE_REQUIRED);
        }

        CsvCell dob = parser.getDOB();
        if (!dob.isEmpty()) {
            patientBuilder.setDateOfBirth(dob.getDate(), dob);
        }

        CsvCell gender = parser.getGender();
        if (!gender.isEmpty()) {

            if (gender.getString().startsWith("M")) {
                patientBuilder.setGender(Enumerations.AdministrativeGender.MALE, gender);
            } else if (gender.getString().startsWith("F")) {
                patientBuilder.setGender(Enumerations.AdministrativeGender.FEMALE, gender);
            } else {
                patientBuilder.setGender(Enumerations.AdministrativeGender.UNKNOWN);
            }
        } else {
            patientBuilder.setGender(Enumerations.AdministrativeGender.UNKNOWN);
        }

        CsvCell givenName = parser.getForename();
        CsvCell surname = parser.getSurname();

        //remove existing name if set
        NameBuilder.removeExistingNameById(patientBuilder, patientId.getString());

        //use patientId for name builder identifier so it can be removed and updated if needed (see above)
        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setId(patientId.getString(), patientId);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addGiven(givenName.getString(), givenName);
        nameBuilder.addFamily(surname.getString(), surname);

        CsvCell addressBuilding = parser.getHomeAddressBuilding();
        CsvCell addressNoAndStreet = parser.getHomeAddressStreet();
        CsvCell addressLocality = parser.getHomeAddressLocality();
        CsvCell addressTown = parser.getHomeAddressTown();
        CsvCell addressPostcode = parser.getHomeAddressPostcode();

        //remove existing address if set
        AddressBuilder.removeExistingAddressById(patientBuilder, patientId.getString());

        //use patientId for address builder identifier so it can be removed and updated if needed (see above)
        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setId(patientId.getString(), patientId);
        addressBuilder.setUse(Address.AddressUse.HOME);
        addressBuilder.addLine(addressBuilding.getString(), addressBuilding);
        addressBuilder.addLine(addressNoAndStreet.getString(), addressNoAndStreet);
        addressBuilder.addLine(addressLocality.getString(), addressLocality);
        addressBuilder.setTown(addressTown.getString(), addressTown);
        addressBuilder.setPostcode(addressPostcode.getString(), addressPostcode);

        //remove all existing contact points for this patient
        ContactPointBuilder.removeExistingContactPoints(patientBuilder);

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

        //the registration type is a property of a patient's stay at an organisation, so add to that resource
        CsvCell regType = parser.getRegistrationType();
        if (!regType.isEmpty()) {

            if (regType.getString().trim().equalsIgnoreCase("Registered")) {
                episodeBuilder.setRegistrationType(RegistrationType.REGULAR_GMS);
            } else {
                episodeBuilder.setRegistrationType(RegistrationType.OTHER);
            }
        }

        CsvCell language = parser.getLanguage();
        if (!language.isEmpty()) {

            CodeableConceptBuilder languageCodeableConceptBuilder
                    = new CodeableConceptBuilder(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language);
            languageCodeableConceptBuilder.setText(language.getString(), language);

            if (language.getString().equalsIgnoreCase("English")) {
                patientBuilder.setSpeaksEnglish(Boolean.TRUE, language);
            }
        }

        CsvCell ethnicity = parser.getEthnicity();
        if (!ethnicity.isEmpty()) {
            patientBuilder.setEthnicity(mapEthnicity(ethnicity.getString()), ethnicity);
        }

        Date registrationEndDate = episodeBuilder.getRegistrationEndDate();
        if (registrationEndDate != null) {
            boolean active = registrationEndDate.after(new Date());
            patientBuilder.setActive(active);
        }

        //the organization resource has been created already in CASEPreTransformer set the episode managing org reference
        UUID serviceId = parser.getServiceId();
        Reference organisationReference = csvHelper.createOrganisationReference(serviceId.toString());

        // if patient already ID mapped, get the mapped ID for the org
        boolean isResourceMapped = csvHelper.isResourceIdMapped(patientId.getString(), patientBuilder.getResource());
        if (isResourceMapped) {
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
        }

        // set the managing OOH organization
        patientBuilder.setManagingOrganisation(organisationReference);

        //set the patient's GP as a care provider
        CsvCell patientGPCareProviderCodeCell = csvHelper.findPatientCareProvider(patientId.getString());
        if (patientGPCareProviderCodeCell != null) {

            Reference gpOrganisationReference = csvHelper.createOrganisationReference(patientGPCareProviderCodeCell.getString());
            if (isResourceMapped) {
                gpOrganisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(gpOrganisationReference, fhirResourceFiler);
            }
            patientBuilder.addCareProvider(gpOrganisationReference);
        }

        if (!patientCreatedInSession) {
            //save both resources together, so the new patient is saved before the episode
            boolean mapPatientIds = !(csvHelper.isResourceIdMapped(patientId.getString(), patientBuilder.getResource()));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapPatientIds, patientBuilder);

            boolean mapEpisodeIds = !(csvHelper.isResourceIdMapped(caseId.getString(), episodeBuilder.getResource()));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapEpisodeIds, episodeBuilder);

            // return the builders back to their caches
            csvHelper.getPatientCache().returnPatientBuilder(patientId, patientBuilder);
            csvHelper.getEpisodeOfCareCache().returnEpisodeOfCareBuilder(caseId, episodeBuilder);
        } else {
            //patient already saved during session, so just file the new episode
            //determine if episode already has mapped Id, i.e. retrieved from DB
            boolean mapEpisodeIds = !(csvHelper.isResourceIdMapped(caseId.getString(), episodeBuilder.getResource()));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapEpisodeIds, episodeBuilder);

            // return the builder back to the cache
            csvHelper.getEpisodeOfCareCache().returnEpisodeOfCareBuilder(caseId, episodeBuilder);
        }
    }

    //try Ethnicity matching using text input string
    private static EthnicCategory mapEthnicity(String ethnicity) {
        if (Strings.isNullOrEmpty(ethnicity)) {
            return EthnicCategory.NOT_STATED;
        }

        //if single character code sent in v2 use that, i.e. A-Z
        if (ethnicity.trim().length() == 1) {
            return EthnicCategory.fromCode(ethnicity);
        }

        //otherwise, use text matching
        if (ethnicity.contains("White")
                && (ethnicity.contains("English") || ethnicity.contains("Scottish") || ethnicity.contains("Welsh"))) {
            return EthnicCategory.WHITE_BRITISH;
        } else if (ethnicity.contains("White") && ethnicity.contains("Irish")) {
            return EthnicCategory.WHITE_IRISH;
        } else if (ethnicity.equalsIgnoreCase("Chinese")) {
            return EthnicCategory.CHINESE;
        } else if (ethnicity.contains("Asian") && ethnicity.contains("Bangladeshi")) {
            return EthnicCategory.ASIAN_BANGLADESHI;
        } else if (ethnicity.contains("Asian") && ethnicity.contains("Pakistani")) {
            return EthnicCategory.ASIAN_PAKISTANI;
        } else if (ethnicity.contains("Asian") && ethnicity.contains("Indian")) {
            return EthnicCategory.ASIAN_INDIAN;
        } else if (ethnicity.contains("Mixed") && ethnicity.contains("Caribbean")) {
            return EthnicCategory.MIXED_CARIBBEAN;
        } else if (ethnicity.contains("Mixed") && ethnicity.contains("African")) {
            return EthnicCategory.MIXED_AFRICAN;
        } else if (ethnicity.contains("Mixed") && ethnicity.contains("Asian")) {
            return EthnicCategory.MIXED_ASIAN;
        } else if (ethnicity.contains("Black") && ethnicity.contains("Caribbean")) {
            return EthnicCategory.BLACK_CARIBBEAN;
        } else if (ethnicity.contains("Black") && ethnicity.contains("African")) {
            return EthnicCategory.BLACK_AFRICAN;
        } else {
            return EthnicCategory.OTHER;
        }
    }
}

