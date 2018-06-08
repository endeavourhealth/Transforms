package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.PATIENT;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.openhr.schema.VocSex;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
    }

    public static void createResource(PATIENT parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        //create Patient Resource builder
        PatientBuilder patientBuilder = new PatientBuilder();
        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();

        CsvCell patientId = parser.getPatientId();
        VisionCsvHelper.setUniqueId(patientBuilder, patientId, null);
        VisionCsvHelper.setUniqueId(episodeBuilder, patientId, null); //use the patient GUID as the ID for the episode

        Reference patientReference = csvHelper.createPatientReference(patientId);
        episodeBuilder.setPatient(patientReference, patientId);

        CsvCell nhsNumber = parser.getNHSNumber();
        if (!nhsNumber.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setValue(nhsNumber.getString(), nhsNumber);

            CsvCell nhsNumberTraceStatus = parser.getNHSNoTraceStatus();
            if (!nhsNumberTraceStatus.isEmpty()) {

                if (nhsNumberTraceStatus.getString().equalsIgnoreCase("V")) {
                    patientBuilder.setNhsNumberVerificationStatus(NhsNumberVerificationStatus.PRESENT_AND_VERIFIED);
                } else {
                    patientBuilder.setNhsNumberVerificationStatus(NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED);
                }
            }
        } else {
            patientBuilder.setNhsNumberVerificationStatus(NhsNumberVerificationStatus.NUMBER_NOT_PRESENT_NO_TRACE_REQUIRED);
        }

        CsvCell dob = parser.getDOB();
        patientBuilder.setDateOfBirth(dob.getDate(), dob);

        CsvCell gender = parser.getGender();
        if (!gender.isEmpty()) {
            VocSex sexEnum = VocSex.fromValue(gender.getString().substring(0).toUpperCase());
            Enumerations.AdministrativeGender genderEnum = SexConverter.convertSexToFhir(sexEnum);
            patientBuilder.setGender(genderEnum, gender);
        }

        CsvCell givenName = parser.getForename();
        CsvCell surname = parser.getSurname();
        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addGiven(givenName.getString(), givenName);
        nameBuilder.addFamily(surname.getString(), surname);

        CsvCell addressBuilding = parser.getHomeAddressBuilding();
        CsvCell addressNoAndStreet = parser.getHomeAddressStreet();
        CsvCell addressLocality = parser.getHomeAddressLocality();
        CsvCell addressTown = parser.getHomeAddressTown();
        CsvCell addressPostcode = parser.getHomeAddressPostcode();

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setUse(Address.AddressUse.HOME);
        addressBuilder.addLine(addressBuilding.getString(), addressBuilding);
        addressBuilder.addLine(addressNoAndStreet.getString(), addressNoAndStreet);
        addressBuilder.addLine(addressLocality.getString(), addressLocality);
        addressBuilder.setTown(addressTown.getString(), addressTown);
        addressBuilder.setPostcode(addressPostcode.getString(), addressPostcode);

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

            if (regType.getString().equalsIgnoreCase("Registered")) {
                episodeBuilder.setRegistrationType(RegistrationType.REGULAR_GMS);
            } else {
                episodeBuilder.setRegistrationType(RegistrationType.OTHER);
            }
        }

        CsvCell language = parser.getLanguage();
        if (!language.isEmpty()) {

            if (language.getString().equalsIgnoreCase("English")) {
                patientBuilder.setSpeaksEnglish(Boolean.TRUE, language);
            }
        }

        //TODO: cache patient and Case (Episode) details
        /*
        CsvCell ethnicity = parser.getEthnicity();
        if (!ethnicity.isEmpty()) {
            patientBuilder.setEthnicity(EthnicCategory.fromCode(ethnicityCode));
        }


        CsvCell regDate = parser.getDateOfRegistration();
        if (!regDate.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(regDate.getDate());
        }

        CsvCell dedDate = parser.getDateOfDeactivation();
        if (!dedDate.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(dedDate.getDate());
        }

        boolean active = dedDate.isEmpty() || dedDate.getDate().after(new Date());
        patientBuilder.setActive(active, dedDate);

        */

        //save both resources together, so the patient is saved before the episode
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder);
    }
}
