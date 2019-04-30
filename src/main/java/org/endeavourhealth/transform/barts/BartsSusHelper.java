package org.endeavourhealth.transform.barts;

import org.hl7.fhir.instance.model.Enumerations;

import java.util.ArrayList;
import java.util.List;

public class BartsSusHelper {


    public static Enumerations.AdministrativeGender convertSusGenderToFHIR(int gender) {
        if (gender == 1) {
            return Enumerations.AdministrativeGender.MALE;
        } else {
            if (gender == 2) {
                return Enumerations.AdministrativeGender.FEMALE;
            } else {
                if (gender == 9) {
                    return Enumerations.AdministrativeGender.OTHER;
                } else {
                    return Enumerations.AdministrativeGender.UNKNOWN;
                }
            }
        }
    }





    public static String getSusEthnicCategoryDisplay(String ethnicGroup) {
        if (ethnicGroup.compareToIgnoreCase("A") == 0) {
            return "British";
        } else if (ethnicGroup.compareToIgnoreCase("B") == 0) {
            return "Irish";
        } else if (ethnicGroup.compareToIgnoreCase("C") == 0) {
            return "Any other White background";
        } else if (ethnicGroup.compareToIgnoreCase("D") == 0) {
            return "White and Black Caribbean";
        } else if (ethnicGroup.compareToIgnoreCase("E") == 0) {
            return "White and Black African";
        } else if (ethnicGroup.compareToIgnoreCase("F") == 0) {
            return "White and Asian";
        } else if (ethnicGroup.compareToIgnoreCase("G") == 0) {
            return "Any other mixed background";
        } else if (ethnicGroup.compareToIgnoreCase("H") == 0) {
            return "Indian";
        } else if (ethnicGroup.compareToIgnoreCase("J") == 0) {
            return "Pakistani";
        } else if (ethnicGroup.compareToIgnoreCase("K") == 0) {
            return "Bangladeshi";
        } else if (ethnicGroup.compareToIgnoreCase("L") == 0) {
            return "Any other Asian background";
        } else if (ethnicGroup.compareToIgnoreCase("M") == 0) {
            return "Caribbean";
        } else if (ethnicGroup.compareToIgnoreCase("N") == 0) {
            return "African";
        } else if (ethnicGroup.compareToIgnoreCase("P") == 0) {
            return "Any other Black background";
        } else if (ethnicGroup.compareToIgnoreCase("R") == 0) {
            return "Chinese";
        } else if (ethnicGroup.compareToIgnoreCase("S") == 0) {
            return "Any other ethnic group";
        } else if (ethnicGroup.compareToIgnoreCase("Z") == 0) {
            return "Not stated";
        } else {
            return "";
        }
    }
    public static List<String> splitEqually(String text, int size) {
        // Give the list the right capacity to start with. You could use an array
        // instead if you wanted.
        List<String> ret = new ArrayList<>((text.length() + size - 1) / size);

        for (int start = 0; start < text.length(); start += size) {
            ret.add(text.substring(start, Math.min(text.length(), start + size)));
        }
        return ret;
    }
}
