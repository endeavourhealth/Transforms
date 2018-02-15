package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.StringType;

import java.util.ArrayList;

public class AddressBuilder {

    private HasAddressI parentBuilder = null;

    public AddressBuilder(HasAddressI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    public void beginAddress(Address.AddressUse use) {
        parentBuilder.addAddress(use);
    }

    public void addLine(String line, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(line)) {
            return;
        }
        parentBuilder.addAddressLine(line, sourceCells);

        updateAddressDisplay(sourceCells);
    }

    public void setTown(String town, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(town)) {
            return;
        }
        parentBuilder.addAddressTown(town, sourceCells);

        updateAddressDisplay(sourceCells);
    }

    public void setDistrict(String district, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(district)) {
            return;
        }
        parentBuilder.addAddressDistrict(district, sourceCells);

        updateAddressDisplay(sourceCells);
    }

    public void setPostcode(String postcode, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(postcode)) {
            return;
        }
        parentBuilder.addAddressPostcode(postcode, sourceCells);

        updateAddressDisplay(sourceCells);
    }

    private void updateAddressDisplay(CsvCell... sourceCells) {

        ArrayList<String> displayTextLines = new ArrayList<>();

        Address address = parentBuilder.getLastAddress();

        if (address.hasLine()) {
            for (StringType line: address.getLine()) {
                displayTextLines.add(line.getValue());
            }
        }

        if (address.hasCity()) {
            displayTextLines.add(address.getCity());
        }

        if (address.hasDistrict()) {
            displayTextLines.add(address.getDistrict());
        }

        if (address.hasPostalCode()) {
            displayTextLines.add(address.getPostalCode());
        }

        String displayText = String.join(", ", displayTextLines);
        parentBuilder.addAddressDisplayText(displayText, sourceCells);
    }


    /*public void populateAddress(Address.AddressUse use, CsvCell houseNameFlatNumber, CsvCell numberAndStreet, CsvCell village, CsvCell town, CsvCell county, CsvCell postcode) {

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
    }*/

    public void addAddressNoAudit(Address address) {

        Address.AddressUse use = address.getUse();
        parentBuilder.addAddress(use);

        if (address.hasLine()) {
            for (StringType line: address.getLine()) {
                parentBuilder.addAddressLine(line.getValue());
            }
        }

        if (address.hasCity()) {
            parentBuilder.addAddressTown(address.getCity());
        }

        if (address.hasDistrict()) {
            parentBuilder.addAddressDistrict(address.getDistrict());
        }

        if (address.hasPostalCode()) {
            parentBuilder.addAddressPostcode(address.getPostalCode());
        }

        updateAddressDisplay();
    }
}
