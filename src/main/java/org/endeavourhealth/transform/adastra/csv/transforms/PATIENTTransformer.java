package org.endeavourhealth.transform.adastra.csv.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.cache.EpisodeOfCareResourceCache;
import org.endeavourhealth.transform.adastra.cache.PatientResourceCache;
import org.endeavourhealth.transform.adastra.csv.schema.PATIENT;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

        CsvCell patientId = parser.getPatientId();
        CsvCell caseId = parser.getCaseId();

        //get EpisodeofCare already populated from preceeding CASE transform
        EpisodeOfCareBuilder episodeBuilder
                = EpisodeOfCareResourceCache.getOrCreateEpisodeOfCareBuilder(caseId, csvHelper, fhirResourceFiler);

        //get or create Patient Resource builder.  If two patients in same file, first already created, so retrieve from cache
        PatientBuilder patientBuilder
                = PatientResourceCache.getOrCreatePatientBuilder(patientId, csvHelper, fhirResourceFiler);

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

        CsvCell ethnicity = parser.getEthnicity();
        if (!ethnicity.isEmpty()) {
            patientBuilder.setEthnicity(mapEthnicity(ethnicity.getString()));
        }

        boolean active = episodeBuilder.getRegistrationEndDate().after(new Date());
        patientBuilder.setActive(active);

        //save both resources together, so the patient is saved before the episode
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder);
    }

    //try Ethnicity matching using text input string
    private static EthnicCategory mapEthnicity(String ethnicity) {
        if (Strings.isNullOrEmpty(ethnicity)) {
            return EthnicCategory.NOT_STATED;
        }
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
        } else if (ethnicity.contains("Mixed") && ethnicity.contains("African")) {
            return EthnicCategory.BLACK_AFRICAN;
        } else {
            return EthnicCategory.OTHER;
        }
    }
}
