package org.endeavourhealth.transform.emis.openhr.transforms.common;

import org.endeavourhealth.transform.emis.openhr.schema.OpenHR001Person;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.StringType;

import java.util.List;

public class NameConverter
{
    public static List<HumanName> convertName(OpenHR001Person sourcePerson)
    {
        return org.endeavourhealth.common.fhir.NameHelper.convert(
                sourcePerson.getTitle(),
                sourcePerson.getForenames(),
                sourcePerson.getSurname(),
                sourcePerson.getCallingName(),
                sourcePerson.getBirthSurname(),
                sourcePerson.getPreviousSurname());
    }

    public static boolean hasPrefix(HumanName humanName, String prefix) {
        if (!humanName.hasPrefix()) {
            return false;
        }

        for (StringType s: humanName.getPrefix()) {
            String str = s.getValue();
            if (prefix.equalsIgnoreCase(str)) {
                return true;
            }
        }

        return false;
    }

    public static boolean hasGivenName(HumanName humanName, String givenName) {
        if (!humanName.hasGiven()) {
            return false;
        }

        for (StringType s: humanName.getGiven()) {
            String str = s.getValue();
            if (givenName.equalsIgnoreCase(str)) {
                return true;
            }
        }

        return false;
    }

    public static boolean hasFamilyName(HumanName humanName, String familyName) {
        if (!humanName.hasFamily()) {
            return false;
        }

        for (StringType s: humanName.getFamily()) {
            String str = s.getValue();
            if (familyName.equalsIgnoreCase(str)) {
                return true;
            }
        }

        return false;
    }

    public static boolean hasSuffix(HumanName humanName, String suffix) {
        if (!humanName.hasSuffix()) {
            return false;
        }

        for (StringType s: humanName.getSuffix()) {
            String str = s.getValue();
            if (suffix.equalsIgnoreCase(str)) {
                return true;
            }
        }

        return false;
    }
}
