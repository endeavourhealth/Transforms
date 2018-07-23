package org.endeavourhealth.transform.emis.emisopen.transforms.common;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.transform.emis.emisopen.schema.eommedicalrecord38.AddressType;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.StringType;

public class AddressConverter
{
    public static Address convert(AddressType addressType, Address.AddressUse addressUse) {
        return AddressHelper.createAddress(addressUse, addressType.getHouseNameFlat(), addressType.getStreet(), addressType.getVillage(), addressType.getTown(), addressType.getCounty(), addressType.getPostCode());
    }

    public static boolean hasLine(Address address, String line) {
        if (!address.hasLine()) {
            return false;
        }

        for (StringType s: address.getLine()) {
            String str = s.getValue();
            if (str.equalsIgnoreCase(line)) {
                return true;
            }
        }

        return false;
    }

    public static boolean hasCity(Address address, String city) {
        return address.hasCity()
                && address.getCity().equalsIgnoreCase(city);
    }

    public static boolean hasDistrict(Address address, String district) {
        return address.hasDistrict()
                && address.getDistrict().equalsIgnoreCase(district);
    }

    public static boolean hasPostcode(Address address, String postcode) {
        if (address.hasPostalCode()) {
            String s = address.getPostalCode();
            s = s.replace(" ", ""); //the ADT wrongly leave spaces in the postcode, so we need to remove them to compare
            if (s.equals(postcode)) {
                return true;
            }
        }

        return false;
    }
}
