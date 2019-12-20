package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
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

        if (otherAddress.hasExtension()) {
            throw new RuntimeException("Address has unexpected extension");
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


    /**
     * removes the last added contact point if it already exists in the resource, if not will end any
     * existing active ones. Anything added after the effective date will also be removed, to handle cases
     * where we're re-processing old data.
     */
    public static void deDuplicateLastAddress(HasAddressI resource, Date effectiveDate) throws Exception {

        List<Address> addresss = resource.getAddresses();
        if (addresss.isEmpty()) {
            return;
        }

        Address lastAddress = addresss.get(addresss.size()-1);
        Address.AddressUse lastUse = lastAddress.getUse();

        //for feeds that have discrete records in the source data (e.g. TPP, Cerner) with their own unique IDs,
        //then this function isn't suitable as it's expected that individual records will be maintained using the ID.
        //Same goes for feeds that externally set dates on entries.
        if (lastAddress.hasId()
                || lastAddress.hasPeriod()) {
            throw new Exception("De-duplication function only expected to be used when no unique IDs or explicit dates available");
        }

        //make sure to roll back if we're re-processing old data
        rollBackToDate(resource, effectiveDate, lastUse);

        List<Address> addresssToEnd = new ArrayList<>();
        boolean setStartDate = false;

        //note the start index is the one BEFORE the last one, above
        for (int i=addresss.size()-2; i>=0; i--) {
            Address address = addresss.get(i);

            //skip any that are of a different scope
            if (!sameUse(address, lastUse)) {
                continue;
            }

            //if we've got previous history of entries in the same scope, then this is a delta and we can set the start date
            setStartDate = true;

            //ended ones shouldn't count towards the duplicate check
            if (!PeriodHelper.isActive(address.getPeriod())) {
                continue;
            }

            //the shallow equals fn compares the value but not the period, which is what we want
            if (address.equalsShallow(lastAddress)) {
                //if the latest has same value as this existing active one, then it's a duplicate and should be removed
                addresss.remove(addresss.size() - 1);
                return;
            }

            //if we make it here, then this one should be ended
            addresssToEnd.add(address);
        }

        if (setStartDate) {
            AddressBuilder builder = new AddressBuilder(resource, lastAddress);
            builder.setStartDate(effectiveDate);
        }

        //end any active ones we've found
        if (!addresssToEnd.isEmpty()) {
            for (Address addressToEnd: addresssToEnd) {
                AddressBuilder builder = new AddressBuilder(resource, addressToEnd);
                builder.setEndDate(effectiveDate);
            }
        }
    }

    /**
     * if we know an address is no longer active, this function will find any active address (for the system and
     * use) and end it with the given date
     */
    public static void endAddresses(HasAddressI resource, Date effectiveDate, Address.AddressUse useToEnd) throws Exception {
        List<Address> addresss = resource.getAddresses();
        if (addresss.isEmpty()) {
            return;
        }

        //make sure to roll back if we're re-processing old data
        rollBackToDate(resource, effectiveDate, useToEnd);

        for (int i=addresss.size()-1; i>=0; i--) {
            Address address = addresss.get(i);
            if (sameUse(address, useToEnd)
                    && PeriodHelper.isActive(address.getPeriod())) {

                AddressBuilder builder = new AddressBuilder(resource, address);
                builder.setEndDate(effectiveDate);
            }
        }
    }

    private static boolean sameUse(Address address, Address.AddressUse use) {
        return address.hasUse()
                && address.getUse() == use;
    }

    /**
     * because we sometimes need to re-process past data, we need this function to essentially roll back the
     * list to what it would have been on a given date. Removes anything known to have been added on or after
     * the effective date, and un-ends anything ended on or after that date.
     */
    private static void rollBackToDate(HasAddressI resource, Date effectiveDate, Address.AddressUse use) throws Exception {
        if (use == null) {
            throw new Exception("De-duplication function only supports last entry having a use set");
        }

        List<Address> addresss = resource.getAddresses();
        for (int i=addresss.size()-1; i>=0; i--) {
            Address address = addresss.get(i);
            if (sameUse(address, use)
                    && address.hasPeriod()) {
                Period p = address.getPeriod();

                //if it was added on or after the effective date, remove it
                if (p.hasStart()
                        && !p.getStart().before(effectiveDate)) {
                    addresss.remove(i);
                    continue;
                }

                //if it was ended on or after the effective date, un-end it
                if (p.hasEnd()
                        && !p.getEnd().before(effectiveDate)) {
                    AddressBuilder builder = new AddressBuilder(resource, address);
                    builder.setEndDate(null);
                }
            }
        }
    }
}
