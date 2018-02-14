package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Address;

public interface HasAddressI {

    void addAddress(Address.AddressUse use);
    void addAddressLine(String line, CsvCell... sourceCells);
    void addAddressTown(String town, CsvCell... sourceCells);
    void addAddressDistrict(String district, CsvCell... sourceCells);
    void addAddressPostcode(String postcode, CsvCell... sourceCells);
    void addAddressDisplayText(String displayText, CsvCell... sourceCells);
}
