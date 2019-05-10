package org.endeavourhealth.transform.ui.helpers;

import org.endeavourhealth.common.fhir.NameHelper;
import org.endeavourhealth.transform.ui.models.types.UIHumanName;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.StringType;

import java.util.List;
import java.util.stream.Collectors;

public class UINameHelper {

    public static UIHumanName getUsualOrOfficialName(Patient patient) {

        //got inconsistencies between this class and everywhere else, so changing
        //to use common function for getting "best" name for a patient, which also factors
        //in that names may have end dates
        HumanName hn = NameHelper.findName(patient);
        if (hn != null) {
            return transform(hn);
        } else {
            return null;
        }

        /*
        List<HumanName> names = patient.getName();
        HumanName name = getNameByUse(names, HumanName.NameUse.USUAL);

        if (name == null)
            name = getNameByUse(names, HumanName.NameUse.OFFICIAL);

        return transform(name);*/
    }

    public static UIHumanName transform(HumanName name) {
        HumanName.NameUse nameUse = name.getUse();
        if (nameUse == null)
            nameUse = HumanName.NameUse.NULL;

        return new UIHumanName()
                .setFamilyName(UINameHelper.getFirst(name.getFamily()))
                .setGivenNames(UINameHelper.getAll(name.getGiven()))
                .setPrefix(UINameHelper.getFirst(name.getPrefix()))
								.setText(name.getText())
								.setUse(nameUse.getDisplay());
    }

    private static HumanName getNameByUse(List<HumanName> names, HumanName.NameUse nameUse) {
        if (names == null)
            return null;

        for (HumanName name : names)
            if (name.getUse() != null)
                if (name.getUse() == nameUse)
                    return name;

        return null;
    }

    private static String getFirst(List<StringType> strings) {
        if (strings == null)
            return null;

        if (strings.size() == 0)
            return null;

        return strings.get(0).getValue();
    }

    private static List<String> getAll(List<StringType> strings) {
        if (strings == null)
            return null;

        return strings
                .stream()
                .map(t -> t.getValueNotNull())
                .collect(Collectors.toList());
    }
}
