package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.StringType;

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

    public static AddressBuilder findOrCreateForId(HasAddressI parentBuilder, CsvCell idCell) {
        String idValue = idCell.getString();
        Address address = findForId(parentBuilder, idValue);
        if (address != null) {
            return new AddressBuilder(parentBuilder, address);

        } else {
            AddressBuilder ret = new AddressBuilder(parentBuilder, address);
            ret.setId(idValue, idCell);
            return ret;
        }

    }

    private static Address findForId(HasAddressI parentBuilder, String idValue) {
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
            return null;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " addresses for ID " + idValue);

        } else {
            return matches.get(0);
        }
    }

    public static boolean removeExistingAddressById(HasAddressI parentBuilder, String idValue) {
        Address address = findForId(parentBuilder, idValue);
        if (address != null) {

            //remove any audits we've created for the CodeableConcept
            String identifierJsonPrefix = parentBuilder.getAddressJsonPrefix(address);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removeAddress(address);
            return true;

        } else {
            return false;
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

    public void setCity(String town, CsvCell... sourceCells) {
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
        setText(displayText, sourceCells);
    }

    private void setText(String text, CsvCell... sourceCells) {
        address.setText(text);

        auditNameValue("text", sourceCells);
    }


    public Address getAddressCreated() {
        return this.address;
    }

    public void reset() {
        //this.address.setId(null); //do not remove any ID as that's used to match names up
        this.address.setPostalCode(null);
        this.address.setCity(null);
        this.address.setPeriod(null);
        this.address.setDistrict(null);
        this.address.setCountry(null);
        this.address.setState(null);
        this.address.setType(null);
        this.address.setUse(null);
        this.address.getLine().clear();
        this.address.setText(null);

    }

    /**
     * sets all the fields to the passed in address, but bypasses all auditing
     */
    public void addAddressNoAudit(Address otherAddress) {

        if (otherAddress.hasId()) {
            setId(otherAddress.getId());
        }

        if (otherAddress.hasPostalCode()) {
            setPostcode(otherAddress.getPostalCode());
        }

        if (otherAddress.hasCity()) {
            setCity(otherAddress.getCity());
        }

        if (otherAddress.hasPeriod()) {
            Period p = otherAddress.getPeriod();
            if (p.hasStart()) {
                setStartDate(p.getStart());
            }
            if (p.hasEnd()) {
                setEndDate(p.getEnd());
            }
        }

        if (otherAddress.hasDistrict()) {
            setDistrict(otherAddress.getDistrict());
        }

        if (otherAddress.hasCountry()) {
            //we don't use this, so don't expect to have it
            throw new RuntimeException("Address has unexpected country property");
        }

        if (otherAddress.hasState()) {
            //we don't use this, so don't expect to have it
            throw new RuntimeException("Address has unexpected state property");
        }

        if (otherAddress.hasType()) {
            setType(otherAddress.getType());
        }

        if (otherAddress.hasUse()) {
            setUse(otherAddress.getUse());
        }

        if (otherAddress.hasLine()) {
            for (StringType st: otherAddress.getLine()) {
                addLine(st.toString());
            }
        }

        //if we have a line, postcode etc., the text will have been dynamically generated, so only
        //carry over the text if none of them are present
        if (otherAddress.hasText()
            && !otherAddress.hasLine()
            && !otherAddress.hasCity()
            && !otherAddress.hasDistrict()
            && !otherAddress.hasPostalCode()) {

            setText(otherAddress.getText());
        }
    }

    public static void removeExistingAddresses(HasAddressI parentBuilder) {

        List<Address> addresses = new ArrayList<>(parentBuilder.getAddresses()); //need to copy the array so we can remove while iterating
        for (Address address: addresses) {

            //remove any audits we've created for the Address
            String jsonPrefix = parentBuilder.getAddressJsonPrefix(address);
            parentBuilder.getAuditWrapper().removeAudit(jsonPrefix);

            parentBuilder.removeAddress(address);
        }

    }
}
