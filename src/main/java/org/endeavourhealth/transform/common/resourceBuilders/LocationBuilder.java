package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class LocationBuilder extends ResourceBuilderBase
                            implements HasAddressI {

    private Location location = null;

    public LocationBuilder() {
        this(null);
    }

    public LocationBuilder(Location location) {
        this.location = location;
        if (this.location == null) {
            this.location = new Location();
            this.location.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_LOCATION));
        }
    }


    @Override
    public DomainResource getResource() {
        return location;
    }

    public void addIdentifier(Identifier fhirIdentifier, CsvCell... sourceCells) {
        this.location.addIdentifier(fhirIdentifier);

        int index = this.location.getIdentifier().size()-1;
        auditValue("identifier[" + index + "].value", sourceCells);
    }

    public void addTelecom(ContactPoint fhirContact, CsvCell... sourceCells) {
        this.location.addTelecom(fhirContact);

        int index = this.location.getTelecom().size()-1;
        auditValue("telecom[" + index + "].value", sourceCells);
    }

    @Override
    public void addAddress(Address.AddressUse use) {
        Address address = new Address();
        address.setUse(use);
        this.location.setAddress(address);
    }

    @Override
    public Address getLastAddress() {
        return this.location.getAddress();
    }

    @Override
    public void addAddressLine(String line, CsvCell... sourceCells) {
        Address address = this.location.getAddress();
        address.addLine(line);

        int index = address.getLine().size()-1;
        auditValue("address.line[" + index + "]", sourceCells);
    }

    @Override
    public void addAddressTown(String town, CsvCell... sourceCells) {
        Address address = this.location.getAddress();
        address.setCity(town);

        auditValue("address.city", sourceCells);
    }

    @Override
    public void addAddressDistrict(String district, CsvCell... sourceCells) {
        Address address = this.location.getAddress();
        address.setDistrict(district);

        auditValue("address.district", sourceCells);
    }

    @Override
    public void addAddressPostcode(String postcode, CsvCell... sourceCells) {
        Address address = this.location.getAddress();
        address.setPostalCode(postcode);

        auditValue("address.postalCode", sourceCells);
    }

    @Override
    public void addAddressDisplayText(String displayText, CsvCell... sourceCells) {
        Address address = this.location.getAddress();
        address.setText(displayText);

        auditValue("address.text", sourceCells);
    }

    public void setDescription(String name, CsvCell... sourceCells) {
        this.location.setDescription(name);

        auditValue("description", sourceCells);
    }

    public void setName(String name, CsvCell... sourceCells) {
        this.location.setName(name);

        auditValue("name", sourceCells);
    }

    public void setMainContactName(String name, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.location, FhirExtensionUri.LOCATION_MAIN_CONTACT, name);

        auditStringExtension(extension, sourceCells);
    }

    public void setTypeFreeText(String type, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(type);
        this.location.setType(codeableConcept);

        auditValue("type.text", sourceCells);
    }

    public void setPartOf(Reference locationReference, CsvCell... sourceCells) {
        this.location.setPartOf(locationReference);

        auditValue("partOf.reference", sourceCells);
    }

    public void setManagingOrganisation(Reference organisationReference, CsvCell... sourceCells) {
        this.location.setManagingOrganization(organisationReference);

        auditValue("managingOrganization.reference", sourceCells);
    }

    public void setMode(Location.LocationMode mode, CsvCell... sourceCells) {
        this.location.setMode(mode);

        auditValue("mode", sourceCells);
    }

    public void setStatus(Location.LocationStatus status, CsvCell... sourceCells) {
        this.location.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setPhysicalType(CodeableConcept pt, CsvCell... sourceCells) {
        this.location.setPhysicalType(pt);

        auditValue("physicalType.value", sourceCells);
    }

    private Period findOrCreateOpenPeriod(Extension extension) {
        Period period = (Period)extension.getValue();
        if (period == null) {
            period = new Period();
            extension.setValue(period);
        }
        return period;
    }

    private void updateActiveStatus(Extension extension) {
        Period period = findOrCreateOpenPeriod(extension);
        boolean active = PeriodHelper.isActive(period);
        if (active) {
            this.location.setStatus(Location.LocationStatus.ACTIVE);
        } else {
            this.location.setStatus(Location.LocationStatus.INACTIVE);
        }
    }

    public void setOpenDate(Date date, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.location, FhirExtensionUri.ACTIVE_PERIOD);
        Period period = findOrCreateOpenPeriod(extension);
        period.setStart(date);

        updateActiveStatus(extension);

        auditPeriodStartExtension(extension, sourceCells);
    }

    public void setCloseDate(Date date, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.location, FhirExtensionUri.ACTIVE_PERIOD);
        Period period = findOrCreateOpenPeriod(extension);
        period.setStart(date);

        updateActiveStatus(extension);

        auditPeriodEndExtension(extension, sourceCells);
    }
}
