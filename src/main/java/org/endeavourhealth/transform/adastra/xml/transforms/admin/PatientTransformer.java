package org.endeavourhealth.transform.adastra.xml.transforms.admin;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.transform.adastra.AdastraXmlHelper;
import org.endeavourhealth.transform.adastra.xml.schema.*;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

import static org.endeavourhealth.transform.adastra.AdastraXmlHelper.uniqueIdMapper;

public class PatientTransformer {

    public static void transform(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        AdastraCaseDataExport.Patient patient = caseReport.getPatient();

        String firstName = patient.getFirstName();
        String lastName = patient.getLastName();

        Patient fhirPatient = new Patient();
        fhirPatient.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PATIENT));

        // set patient ID
        fhirPatient.setId(patient.getNationalNumber().getNumber() + ":" + patient.getDateOfBirth().getDobValue());
        uniqueIdMapper.put("patient", fhirPatient.getId());

        fhirPatient.getId();
        HumanName humanName = NameHelper.convert(firstName, lastName, null);
        fhirPatient.addName(humanName);

        DateOfBirthType dateOfBirthType = patient.getDateOfBirth();
        if (dateOfBirthType != null) {
            String type = dateOfBirthType.getDataOfBirthType();
            if (type.equals("Exact")) {
                Date dob = XmlDateHelper.convertDate(dateOfBirthType.getDobValue());
                fhirPatient.setBirthDate(dob);
            } else if (type.equals("AgeOnly")) {
                // Age Only sets the DOB as the mid point of the year of birth so treat like a normal DOB
                Date dob = XmlDateHelper.convertDate(dateOfBirthType.getDobValue());
                fhirPatient.setBirthDate(dob);
            } else {
                throw new Exception("Unexpected date of birth type [" + type + "]");
            }
        }

        String gender = patient.getGender();
        if (!Strings.isNullOrEmpty(gender)) {
            Enumerations.AdministrativeGender fhirGender = convertGender(gender);
            fhirPatient.setGender(fhirGender);
        }

        NationalNumberType nationalNumberType = patient.getNationalNumber();
        if (nationalNumberType != null) {
            //the national number is ALWAYS an NHS number
            String number = nationalNumberType.getNumber();
            fhirPatient.addIdentifier(IdentifierHelper.createNhsNumberIdentifier(number));

            String status = nationalNumberType.getNationalNumberStatus();
            NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(status);
            CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(verificationStatus);
            fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS, fhirCodeableConcept));
        }

        List<PatientAddressType> addresses = patient.getAddress();
        for (PatientAddressType address: addresses) {
            Address fhirAddress = convertAddress(address);
            fhirPatient.addAddress(fhirAddress);
        }

        List<PatientPhoneNumberType> phoneNumbers = patient.getPhone();
        for (PatientPhoneNumberType phoneNumber: phoneNumbers) {
            ContactPoint fhirContact = convertPhoneNumber(phoneNumber);
            fhirPatient.addTelecom(fhirContact);
        }

        AdastraCaseDataExport.Patient.GpRegistration registration = patient.getGpRegistration();

        AdastraXmlHelper.createOrganisationReference(registration.getSurgeryNationalCode());

        fhirResourceFiler.savePatientResource(null, new PatientBuilder(fhirPatient));
    }

    private static ContactPoint convertPhoneNumber(PatientPhoneNumberType phoneNumber) throws Exception {

        String number = phoneNumber.getNumber();
        String extension = phoneNumber.getExtension();

        //append the extension using standard http://www.itu.int/rec/T-REC-E.123-200102-I/e
        if (!Strings.isNullOrEmpty(extension)) {
            number += " ext. " + extension;
        }

        ContactPoint.ContactPointUse use = null;

        String type = phoneNumber.getNumberType();
        if (type.equalsIgnoreCase("Home")) {
            use = ContactPoint.ContactPointUse.HOME;

        } else if (type.equalsIgnoreCase("Work")) {
            use = ContactPoint.ContactPointUse.WORK;

        } else if (type.equalsIgnoreCase("Mobile")) {
            use = ContactPoint.ContactPointUse.MOBILE;

        } else if (type.equalsIgnoreCase("Other")) {
            use = ContactPoint.ContactPointUse.TEMP;

        } else {
            throw new Exception("Unexpected phone number type [" + type + "]");
        }

        return ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, use, number);
    }

    private static Address convertAddress(PatientAddressType address) throws Exception {

        String line1 = address.getLine1();
        String line2 = address.getLine2();
        String village = address.getLocality();
        String town = address.getTown();
        String county = address.getCounty();
        String postcode = address.getPostcode();

        Address.AddressUse addressUse = null;
        String type = address.getAddressType();
        if (type.equalsIgnoreCase("Home")) {
            addressUse = Address.AddressUse.HOME;

        } else if (type.equalsIgnoreCase("CurrentLocation")) {
            addressUse = Address.AddressUse.TEMP;

        } else {
            throw new Exception("Unexpected address type [" + type + "]");
        }

        return AddressHelper.createAddress(addressUse, line1, line2, village, town, county, postcode);
    }

    private static NhsNumberVerificationStatus convertNhsNumberVeriticationStatus(String status) throws Exception {
        if (status.equalsIgnoreCase("Confirmed")) {
            return NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;

        } else if (status.equalsIgnoreCase("Unconfirmed")) {
            return NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED;

        } else {
            throw new Exception("Unexpected national number status [" + status + "]");
        }
    }

    private static Enumerations.AdministrativeGender convertGender(String gender) throws Exception {
        if (gender.equalsIgnoreCase("Male")) {
            return Enumerations.AdministrativeGender.MALE;

        } else if (gender.equalsIgnoreCase("Female")) {
            return Enumerations.AdministrativeGender.FEMALE;

        } else if (gender.equalsIgnoreCase("Unknown")) {
            return Enumerations.AdministrativeGender.UNKNOWN;

        } else {
            throw new Exception("Unexpected gender [" + gender + "]");
        }
    }
}
