package org.endeavourhealth.transform.emis.emisopen.transforms.common;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.transform.emis.emisopen.schema.eommedicalrecord38.AddressType;
import org.hl7.fhir.instance.model.Address;

public class EmisOpenAddressConverter
{
    public static Address convert(AddressType addressType, Address.AddressUse addressUse) {
        return AddressHelper.createAddress(addressUse, addressType.getHouseNameFlat(), addressType.getStreet(), addressType.getVillage(), addressType.getTown(), addressType.getCounty(), addressType.getPostCode());
    }

}
