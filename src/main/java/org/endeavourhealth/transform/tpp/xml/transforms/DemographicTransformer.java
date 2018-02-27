package org.endeavourhealth.transform.tpp.xml.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.FhirHelper;
import org.endeavourhealth.transform.tpp.xml.schema.Address;
import org.endeavourhealth.transform.tpp.xml.schema.*;
import org.hl7.fhir.instance.model.*;
import org.hl7.fhir.instance.model.Patient;

import javax.xml.datatype.XMLGregorianCalendar;
import java.time.Instant;
import java.util.Date;
import java.util.List;

public class DemographicTransformer {

    private static final String ENGLISH_MAIN_CODE_SNOMED = "315570003"; //Main spoken language English
    private static final String ENGLISH_MAIN_CODE_CTV3 = "XaG5t";
    private static final String ENGLISH_SECOND_CODE = "161140009";
    private static final String ENGLISH_SECOND_TERM = "English as a second language";

    public static void transform(String patientUid, Identity tppId, Demographics tppDemographics, List<Resource> fhirResources) throws TransformException {

        Patient fhirPatient = new Patient();
        fhirPatient.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PATIENT));
        fhirPatient.setId(patientUid);
        fhirResources.add(fhirPatient);

        fhirPatient.setManagingOrganization(FhirHelper.findAndCreateReference(Organization.class, fhirResources));

        transformIdentity(fhirPatient, patientUid, tppId);
        transformName(fhirPatient, tppDemographics);
        transformDob(fhirPatient, tppDemographics);
        transformDod(fhirPatient, tppDemographics);
        transformGender(fhirPatient, tppDemographics);
        transformMaritalStatus(fhirPatient, tppDemographics);
        transformEthnicity(fhirPatient, tppDemographics);
        transformLanguage(fhirPatient, tppDemographics);
        transformAddress(fhirPatient, tppDemographics);
        transformCommunications(fhirPatient, tppDemographics);
        transformUsualGp(fhirPatient, tppDemographics, fhirResources);
        EpisodeOfCare fhirEpisode = transformCareDates(fhirPatient, tppDemographics, fhirResources);
        transformRegistrationType(fhirEpisode, tppDemographics);
    }

    private static void transformRegistrationType(EpisodeOfCare fhirEpisode, Demographics tppDemographics) {

        //TODO - need to get proper object type for registrationType
        String registrationType = tppDemographics.getRegistrationType();

        Extension ext = ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_REGISTRATION_TYPE, new StringType(registrationType));
        //the registration type is a property of a patient's stay at an organisation, so add to that resource instead
        fhirEpisode.addExtension(ext);
        //fhirPatient.addExtension(ext);
    }

    private static EpisodeOfCare transformCareDates(Patient fhirPatient, Demographics tppDemographics, List<Resource> fhirResources) throws TransformException {

        XMLGregorianCalendar startDate = tppDemographics.getCareStartDate();
        XMLGregorianCalendar endDate = tppDemographics.getCareEndDate();

        //there may be future-dated end date, so compare against now and ignore if so
        if (endDate != null) {
            Instant ins = endDate.toGregorianCalendar().toInstant();
            if (!ins.isAfter(Instant.now())) {
                endDate = null;
            }
        }

        fhirPatient.setActive(endDate == null);

        //also need to create the EpisodeOfCare resource
        EpisodeOfCare fhirEpisode = new EpisodeOfCare();
        fhirEpisode.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_EPISODE_OF_CARE));
        fhirResources.add(fhirEpisode);

        Period fhirPeriod = PeriodHelper.createPeriod(startDate, endDate);
        fhirEpisode.setPeriod(fhirPeriod);

        if (endDate == null) {
            fhirEpisode.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
        } else {
            fhirEpisode.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
        }

        String patientId = fhirPatient.getId();
        fhirEpisode.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientId));

        fhirEpisode.setManagingOrganization(FhirHelper.findAndCreateReference(Organization.class, fhirResources));

        return fhirEpisode;
    }

    private static void transformUsualGp(Patient fhirPatient, Demographics tppDemographics, List<Resource> fhirResources) throws TransformException {

        String usualGpUserName = tppDemographics.getUsualGPUserName();
        if (Strings.isNullOrEmpty(usualGpUserName)) {
            return;
        }

        fhirPatient.addCareProvider(ReferenceHelper.createReference(ResourceType.Practitioner, usualGpUserName));
    }

    private static void transformCommunications(Patient fhirPatient, Demographics tppDemographics) {

        String homeTel = tppDemographics.getHomeTelephone();
        if (!Strings.isNullOrEmpty(homeTel)) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE,
                    ContactPoint.ContactPointUse.HOME, homeTel);
            fhirPatient.addTelecom(contactPoint);
        }

        String workTel = tppDemographics.getWorkTelephone();
        if (!Strings.isNullOrEmpty(workTel)) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE,
                    ContactPoint.ContactPointUse.WORK, workTel);
            fhirPatient.addTelecom(contactPoint);
        }

        String mobTel = tppDemographics.getMobileTelephone();
        if (!Strings.isNullOrEmpty(mobTel)) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE,
                    ContactPoint.ContactPointUse.MOBILE, mobTel);
            fhirPatient.addTelecom(contactPoint);
        }

        String altTel = tppDemographics.getAlternateTelephone();
        if (!Strings.isNullOrEmpty(altTel)) {
            //treat alternative number as a second home number
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE,
                    ContactPoint.ContactPointUse.HOME, altTel);
            fhirPatient.addTelecom(contactPoint);
        }

        String email = tppDemographics.getEmailAddress();
        if (!Strings.isNullOrEmpty(email)) {
            //assume the email address is a home email, rather than work
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.EMAIL,
                    ContactPoint.ContactPointUse.HOME, email);
            fhirPatient.addTelecom(contactPoint);
        }

        //TODO - store SMS consent in FHIR?
    }

    private static void transformAddress(Patient fhirPatient, Demographics tppDemographics) {
        Address tppAddress = tppDemographics.getAddress();
        org.hl7.fhir.instance.model.Address fhirAddress = AddressTransformer.tranformHomeAddress(tppAddress);
        fhirPatient.addAddress(fhirAddress);
    }

    private static void transformLanguage(Patient fhirPatient, Demographics tppDemographics) throws TransformException {

        Code tppCode = tppDemographics.getMainLanguage();
        if (tppCode != null) {
            CodeableConcept fhirConcept = CodeTransformer.transform(tppCode);
            Patient.PatientCommunicationComponent fhirCommunication = fhirPatient.addCommunication();
            fhirCommunication.setLanguage(fhirConcept);
            fhirCommunication.setPreferred(true); //if it's the MAIN language, then assume it's preferred
        }

        EnglishSpeaker tppEnglish = tppDemographics.getEnglishSpeaker();
        if (tppEnglish == EnglishSpeaker.YES) {
            //if an english speaker, ensure the English code is in the communication part of the FHIR resource
            //unless their main language is already recorded as English
            if (tppCode == null
                || (tppCode.getScheme() == CodeScheme.CTV_3 && !tppCode.getCode().equals(ENGLISH_MAIN_CODE_CTV3))
                || (tppCode.getScheme() == CodeScheme.SNOMED && !tppCode.getCode().equals(ENGLISH_MAIN_CODE_SNOMED))) {

                CodeableConcept fhirConcept = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, ENGLISH_SECOND_CODE, ENGLISH_SECOND_TERM);
                Patient.PatientCommunicationComponent fhirCommunication = fhirPatient.addCommunication();
                fhirCommunication.setLanguage(fhirConcept);
            }

        } else if (tppEnglish == EnglishSpeaker.NO
                || tppEnglish == EnglishSpeaker.UNKNOWN){
            //if not an English speaker or unkown, then don't add any data to the FHIR resource
        } else {
            throw new TransformException("Unsupported value for English speaker " + tppEnglish);
        }
    }

    private static void transformEthnicity(Patient fhirPatient, Demographics tppDemographics) {

        //TODO - does FHIR have anywhere for ethnicity?
    }


    private static void transformMaritalStatus(Patient fhirPatient, Demographics tppDemographics) throws TransformException {
        Code tppCode = tppDemographics.getMaritalStatus();
        if (tppCode != null) {
            CodeableConcept fhirConcept = CodeTransformer.transform(tppCode);
            fhirPatient.setMaritalStatus(fhirConcept);
        }
    }

    private static void transformGender(Patient fhirPatient, Demographics tppDemographics) throws TransformException {

        //TPP doesn't distinguish between gender and sex, and FHIR only supports gender, so just copy sex->gender
        Sex tppSex = tppDemographics.getSex();
        fhirPatient.setGender(SexTransformer.transform(tppSex));
    }

    private static void transformDod(Patient fhirPatient, Demographics tppDemographics) {

        XMLGregorianCalendar cal = tppDemographics.getDateOfDeath();
        if (cal == null) {
            return;
        }

        Date dod = cal.toGregorianCalendar().getTime();
        fhirPatient.setDeceased(new DateTimeType(dod));
    }

    private static void transformDob(Patient fhirPatient, Demographics tppDemographics) {

        XMLGregorianCalendar cal = tppDemographics.getDateOfBirth();
        Date dob = cal.toGregorianCalendar().getTime();

        fhirPatient.setBirthDate(dob);
    }

    private static void transformName(Patient fhirPatient, Demographics tppDemographics) {

        String title = tppDemographics.getTitle();
        String firstName = tppDemographics.getFirstName();
        String middleNames = tppDemographics.getMiddleNames();
        String surname = tppDemographics.getSurname();
        String knownAs = tppDemographics.getKnownAs();

        HumanName fhirName = NameConverter.createHumanName(HumanName.NameUse.OFFICIAL, title, firstName, middleNames, surname);
        fhirPatient.addName(fhirName);

        if (knownAs != null
                && !knownAs.equalsIgnoreCase(firstName)
                && !knownAs.equalsIgnoreCase(title + " " + firstName + " " + surname)) {
            fhirName = NameConverter.createHumanName(HumanName.NameUse.NICKNAME, null, knownAs, null, surname);
            fhirPatient.addName(fhirName);
        }
    }

    private static void transformIdentity(Patient fhirPatient, String patientUid, Identity tppId) {

        //NHS number OR psudeo number will be provided
        String nhsNumber = tppId.getNHSNumber();
        if (nhsNumber != null) {
            Identifier fhirIdentifier = IdentifierHelper.createNhsNumberIdentifier(nhsNumber);
            fhirPatient.addIdentifier(fhirIdentifier);
        } else {
            //the pseudo number is unique to TPP only, so no point adding to FHIR
            //String pseudoNumber = tppId.getPseudoNumber();
        }

        //add the local identifier as well (which we don't have a system for)
        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_PATIENT_ID, patientUid));

    }
}
