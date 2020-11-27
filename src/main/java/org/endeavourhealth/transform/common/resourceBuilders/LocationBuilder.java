package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.LocationPhysicalType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LocationBuilder extends ResourceBuilderBase
                            implements HasAddressI, HasIdentifierI, HasContactPointI, HasCodeableConceptI {

    private Location location = null;

    public LocationBuilder() {
        this(null);
    }

    public LocationBuilder(Location location) {
        this(location, null);
    }

    public LocationBuilder(Location location, ResourceFieldMappingAudit audit) {
        super(audit);

        this.location = location;
        if (this.location == null) {
            this.location = new Location();
            this.location.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));
        }
    }


    @Override
    public DomainResource getResource() {
        return location;
    }

    /**
     * removed - use ContactPointBuilder to add telephone numbers
     */
    /*public void addTelecom(ContactPoint fhirContact, CsvCell... sourceCells) {
        this.location.addTelecom(fhirContact);

        int index = this.location.getTelecom().size()-1;
        auditValue("telecom[" + index + "].value", sourceCells);
    }*/


    /*public void setDescription(String name, CsvCell... sourceCells) {
        this.location.setDescription(name);

        auditValue("description", sourceCells);
    }*/

    public void setName(String name, CsvCell... sourceCells) {
        this.location.setName(name);

        auditValue("name", sourceCells);
    }

    public void setMainContactName(String name, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.location, FhirExtensionUri.LOCATION_MAIN_CONTACT, name);

        auditStringExtension(extension, sourceCells);
    }

    /**
     * use CodeableConceptBuilder now to set the type
     */
    /*public void setTypeFreeText(String type, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(type);
        this.location.setType(codeableConcept);

        auditValue("type.text", sourceCells);
    }*/

    public void setPartOf(Reference locationReference, CsvCell... sourceCells) {
        this.location.setPartOf(locationReference);

        auditValue("partOf.reference", sourceCells);
    }


    public Reference getPartOf() {
        if (this.location.hasPartOf()) {
            return this.location.getPartOf();
        } else {
            return null;
        }
    }

    public Reference getManagingOrganisation() {
        if (this.location.hasManagingOrganization()) {
            return this.location.getManagingOrganization();
        } else {
            return null;
        }
    }

    public void setManagingOrganisation(Reference organisationReference, CsvCell... sourceCells) {
        this.location.setManagingOrganization(organisationReference);

        auditValue("managingOrganization.reference", sourceCells);
    }

    public Location.LocationMode getMode() {
        if (this.location.hasMode()) {
            return this.location.getMode();
        } else {
            return null;
        }
    }

    public void setMode(Location.LocationMode mode, CsvCell... sourceCells) {
        this.location.setMode(mode);

        auditValue("mode", sourceCells);
    }

    public Location.LocationStatus getStatus() {
        if (this.location.hasStatus()) {
            return this.location.getStatus();
        } else {
            return null;
        }
    }

    public void setStatus(Location.LocationStatus status, CsvCell... sourceCells) {
        this.location.setStatus(status);

        auditValue("status", sourceCells);
    }

    public LocationPhysicalType getPhysicalType() {
        if (this.location.hasPhysicalType()) {
            CodeableConcept cc = this.location.getPhysicalType();
            Coding coding = CodeableConceptHelper.findCoding(cc, LocationPhysicalType.AMBULATORY.getSystem()); //can use any of the enum to get the system
            if (coding != null) {
                return LocationPhysicalType.fromCode(coding.getCode());
            }
        }

        return null;
    }

    public void setPhysicalType(LocationPhysicalType locationPhysicalType, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(locationPhysicalType);
        this.location.setPhysicalType(codeableConcept);

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

    private void updateActiveStatus(Extension extension, CsvCell... sourceCells) {
        Period period = findOrCreateOpenPeriod(extension);
        boolean active = PeriodHelper.isActive(period);
        if (active) {
            setStatus(Location.LocationStatus.ACTIVE, sourceCells);
        } else {
            setStatus(Location.LocationStatus.INACTIVE, sourceCells);
        }
    }

    public void setOpenDate(Date date, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.location, FhirExtensionUri.ACTIVE_PERIOD);
        Period period = findOrCreateOpenPeriod(extension);
        period.setStart(date);

        //don't pass the source cells into this fn, as it's only the end date that's factored in to the status calculation
        updateActiveStatus(extension);

        auditPeriodStartExtension(extension, sourceCells);
    }

    public void setCloseDate(Date date, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.location, FhirExtensionUri.ACTIVE_PERIOD);
        Period period = findOrCreateOpenPeriod(extension);
        period.setEnd(date);

        updateActiveStatus(extension, sourceCells);

        auditPeriodEndExtension(extension, sourceCells);
    }

    @Override
    public Address addAddress() {
        if (this.location.hasAddress()) {
            throw new IllegalArgumentException("Trying to set address on location that already has one");
        }
        Address address = new Address();
        this.location.setAddress(address);
        return address;
    }

    @Override
    public String getAddressJsonPrefix(Address address) {
        return "address";
    }

    @Override
    public List<Address> getAddresses() {
        List<Address> ret = new ArrayList<>();
        if (this.location.hasAddress()) {
            ret.add(this.location.getAddress());
        }
        return ret;
    }

    @Override
    public void removeAddress(Address address) {
        this.location.setAddress(null);
    }

    @Override
    public Identifier addIdentifier() {
        return this.location.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.location.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.location.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.location.getIdentifier().remove(identifier);
    }

    @Override
    public ContactPoint addContactPoint() {
        return this.location.addTelecom();
    }

    @Override
    public String getContactPointJsonPrefix(ContactPoint contactPoint) {
        int index = location.getTelecom().indexOf(contactPoint);
        return "telecom[" + index + "]";
    }

    @Override
    public List<ContactPoint> getContactPoint() {
        return this.location.getTelecom();
    }

    @Override
    public void removeContactPoint(ContactPoint contactPoint) {
        this.location.getTelecom().remove(contactPoint);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {
        if (tag == CodeableConceptBuilder.Tag.Location_Type) {
            if (this.location.hasType()) {
                if (useExisting) {
                    return this.location.getType();
                } else {
                    throw new IllegalArgumentException("Trying to add new code to Condition that already has one");
                }
            }
            this.location.setType(new CodeableConcept());
            return this.location.getType();

        } else {
            throw new RuntimeException("Wrong tag for locationBuilder " + tag);
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Location_Type) {
            return "type";

        } else {
            throw new RuntimeException("Wrong tag for locationBuilder " + tag);
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Location_Type) {
            this.location.setType(null);

        } else {
            throw new RuntimeException("Wrong tag for locationBuilder " + tag);
        }
    }

}
