package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Address;

import java.util.ArrayList;

public class AddressBuilder {

    private HasAddressI parentBuilder = null;

    public AddressBuilder(HasAddressI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    public void populateAddress(Address.AddressUse use, CsvCell houseNameFlatNumber, CsvCell numberAndStreet, CsvCell village, CsvCell town, CsvCell county, CsvCell postcode) {

        ArrayList<String> displayTextLines = new ArrayList<>();

        parentBuilder.addAddress(use);

        if (!houseNameFlatNumber.isEmpty()) {
            String str = houseNameFlatNumber.getString();
            parentBuilder.addAddressLine(str, houseNameFlatNumber);
            displayTextLines.add(str);
        }

        if (!numberAndStreet.isEmpty()) {
            String str = numberAndStreet.getString();
            parentBuilder.addAddressLine(str, numberAndStreet);
            displayTextLines.add(str);
        }

        if (!village.isEmpty()) {
            String str = village.getString();
            parentBuilder.addAddressLine(str, village);
            displayTextLines.add(str);
        }

        if (!town.isEmpty()) {
            String str = town.getString();
            parentBuilder.addAddressTown(str, town);
            displayTextLines.add(str);
        }

        if (!county.isEmpty()) {
            String str = county.getString();
            parentBuilder.addAddressDistrict(str, county);
            displayTextLines.add(str);
        }

        if (!postcode.isEmpty()) {
            String str = postcode.getString();
            parentBuilder.addAddressPostcode(str, postcode);
            displayTextLines.add(str);
        }

        String displayText = String.join(", ", displayTextLines);
        parentBuilder.addAddressDisplayText(displayText, houseNameFlatNumber, numberAndStreet, village, town, county, postcode);
    }
}
