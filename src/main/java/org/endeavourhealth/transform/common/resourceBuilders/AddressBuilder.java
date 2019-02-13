package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Period;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AddressBuilder {

    private HasAddressI parentBuilder = null;
    private Address address = null;

    public AddressBuilder(HasAddressI parentBuilder) {
        this(parentBuilder, null);
    }

    public AddressBuilder(HasAddressI parentBuilder, Address address) {
        this.parentBuilder = parentBuilder;
        this.address = address;

        if (this.address == null) {
            this.address = parentBuilder.addAddress();
        }
    }

    public static boolean removeExistingAddressById(HasAddressI parentBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't remove address without ID");
        }

        List<Address> matches = new ArrayList<>();

        List<Address> addresses = parentBuilder.getAddresses();
        for (Address address: addresses) {
            //if we match on ID, then remove this address from the parent object
            if (address.hasId()
                    && address.getId().equals(idValue)) {

                matches.add(address);
            }
        }

        if (matches.isEmpty()) {
            return false;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " addresses for ID " + idValue);

        } else {
            Address address = matches.get(0);

            //remove any audits we've created for the CodeableConcept
            String identifierJsonPrefix = parentBuilder.getAddressJsonPrefix(address);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removeAddress(address);
            return true;
        }
    }

    public void setId(String id, CsvCell... sourceCells) {
        this.address.setId(id);

        auditNameValue("id", sourceCells);
    }

    public void setUse(Address.AddressUse use, CsvCell... sourceCells) {
        this.address.setUse(use);

        auditNameValue("use", sourceCells);
    }

    public void setType(Address.AddressType type, CsvCell... sourceCells) {
        this.address.setType(type);

        auditNameValue("type", sourceCells);
    }

    /**
     * helper function to add a line from separate house number and road cells
     */
    public void addLineFromHouseNumberAndRoad(CsvCell houseNumberCell, CsvCell roadCell) {
        List<String> toks = new ArrayList<>();
        if (!houseNumberCell.isEmpty()) {
            toks.add(houseNumberCell.getString());
        }
        if (!roadCell.isEmpty()) {
            toks.add(roadCell.getString());
        }
        if (toks.isEmpty()) {
            return;
        }
        String str = String.join(" ", toks);
        addLine(str, houseNumberCell, roadCell);
    }

    public void addLine(String line, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(line)) {
            return;
        }

        address.addLine(line);

        int index = address.getLine().size()-1;
        auditNameValue("line[" + index + "]", sourceCells);

        updateAddressDisplay(sourceCells);
    }

    public void setTown(String town, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(town)) {
            return;
        }

        address.setCity(town);

        auditNameValue("city", sourceCells);

        updateAddressDisplay(sourceCells);
    }

    public void setDistrict(String district, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(district)) {
            return;
        }
        address.setDistrict(district);

        auditNameValue("district", sourceCells);

        updateAddressDisplay(sourceCells);
    }

    public void setPostcode(String postcode, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(postcode)) {
            return;
        }

        //for consistency across the estate, and to ensure that SQL joins work, postcodes are always saved without spaces
        postcode = postcode.replaceAll(" ", "");

        address.setPostalCode(postcode);

        auditNameValue("postalCode", sourceCells);

        updateAddressDisplay(sourceCells);
    }


    private Period getOrCreateNamePeriod() {
        Period period = null;
        if (address.hasPeriod()) {
            period = address.getPeriod();
        } else {
            period = new Period();
            address.setPeriod(period);
        }
        return period;
    }

    public void setStartDate(Date date, CsvCell... sourceCells) {
        getOrCreateNamePeriod().setStart(date);

        auditNameValue("period.start", sourceCells);
    }

    public void setEndDate(Date date, CsvCell... sourceCells) {
        getOrCreateNamePeriod().setEnd(date);

        auditNameValue("period.end", sourceCells);
    }

    private void auditNameValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getAddressJsonPrefix(this.address) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            if (csvCell != null) {
                if (csvCell.getOldStyleAuditId() != null) {
                    audit.auditValueOldStyle(csvCell.getOldStyleAuditId(), csvCell.getColIndex(), jsonField);
                } else {
                    audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
                }
            }
        }
    }

    private void updateAddressDisplay(CsvCell... sourceCells) {

        String displayText = AddressHelper.generateDisplayText(address);
        address.setText(displayText);

        auditNameValue("text", sourceCells);
    }


    public Address getAddressCreated() {
        return this.address;
    }
}
