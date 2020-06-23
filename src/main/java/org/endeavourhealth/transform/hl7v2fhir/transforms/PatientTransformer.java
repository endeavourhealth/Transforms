package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    /**
     *
     * @param pid
     * @param patient
     * @return
     * @throws Exception
     */
    public static Patient transformPIDToPatient(PID pid, Patient patient) throws Exception {
        patient.getMeta().addProfile("http://endeavourhealth.org/fhir/StructureDefinition/primarycare-patient");

        CX[] patientIdList = pid.getPatientIDInternalID();
        ST id = patientIdList[0].getID();
        ST nhsNumber = patientIdList[1].getID();

        patient.addIdentifier().setValue(String.valueOf(id)).setSystem("http://imperial-uk.com/identifier/patient-id").setUse(Identifier.IdentifierUse.fromCode("secondary"));
        patient.addIdentifier().setSystem("http://fhir.nhs.net/Id/nhs-number")
                .setValue(String.valueOf(nhsNumber)).setUse(Identifier.IdentifierUse.fromCode("official"));

        XPN[] patientName = pid.getPatientName();
        ST familyName = patientName[0].getFamilyName();
        ST givenName = patientName[0].getGivenName();
        ID nameTypeCode = patientName[0].getNameTypeCode();
        ST prefix = patientName[0].getPrefixEgDR();
        patient.addName().addFamily(String.valueOf(familyName)).addPrefix(String.valueOf(prefix)).addGiven(String.valueOf(givenName)).setUse(HumanName.NameUse.OFFICIAL);

        IS gender = pid.getSex();
        switch(String.valueOf(gender)) {
            case "M":
                patient.setGender(Enumerations.AdministrativeGender.MALE);
                break;
            case "F":
                patient.setGender(Enumerations.AdministrativeGender.FEMALE);
                break;
            default:
                // code block
        }

        TS dob = pid.getDateOfBirth();
        if (!dob.isEmpty()) {
            String dtB = String.valueOf(dob.getTimeOfAnEvent());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(dtB.substring(0,4)+"-"+dtB.substring(4,6)+"-"+dtB.substring(6,8));
            patient.setBirthDate(date);
        }

        XAD[] patientAddress = pid.getPatientAddress();
        ID addType = patientAddress[0].getAddressType();
        ST city = patientAddress[0].getCity();
        ID country = patientAddress[0].getCountry();
        ST add = patientAddress[0].getStreetAddress();
        ST postCode = patientAddress[0].getZipOrPostalCode();

        Address address = new Address();
        if (String.valueOf(addType).equals("HOME")) {address.setUse(Address.AddressUse.HOME);}
        if (String.valueOf(addType).equals("TEMP")) {address.setUse(Address.AddressUse.TEMP);}
        if (String.valueOf(addType).equals("OLD")) {address.setUse(Address.AddressUse.OLD);}

        address.addLine(String.valueOf(add));
        address.setCountry(String.valueOf(country));
        address.setPostalCode(String.valueOf(postCode).replaceAll("\\s",""));
        address.setCity(String.valueOf(city));
        address.setDistrict("");
        patient.addAddress(address);

        TS dod = pid.getPatientDeathDateAndTime();
        if (!dod.isEmpty()) {
            BooleanType bool = new BooleanType();
            bool.setValue(true);
            patient.setDeceased(bool);
        }
        return patient;
    }

}
