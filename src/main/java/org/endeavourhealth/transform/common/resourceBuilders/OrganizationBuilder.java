package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class OrganizationBuilder extends ResourceBuilderBase
                                implements HasAddressI {

    private Organization organization = null;

    public OrganizationBuilder() {
        this(null);
    }

    public OrganizationBuilder(Organization organization) {
        this.organization = organization;
        if (this.organization == null) {
            this.organization = new Organization();
            this.organization.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ORGANIZATION));
        }
    }

    @Override
    public DomainResource getResource() {
        return organization;
    }

    public void setMainLocation(Reference locationReference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.organization, FhirExtensionUri.ORGANISATION_MAIN_LOCATION, locationReference);

        auditReferenceExtension(extension, sourceCells);
    }

    public void addIdentifier(Identifier fhirIdentifier, CsvCell... sourceCells) {
        this.organization.addIdentifier(fhirIdentifier);

        int index = this.organization.getIdentifier().size()-1;
        auditValue("identifier[" + index + "].value", sourceCells);
    }

    public void setName(String name, CsvCell... sourceCells) {
        this.organization.setName(name);

        auditValue("name", sourceCells);
    }

    public void setParentOrganisation(Reference parentOrgReference, CsvCell... sourceCells) {
        this.organization.setPartOf(parentOrgReference);

        auditValue("partOf.reference", sourceCells);
    }

    public void setType(OrganisationType fhirOrgType, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirOrgType);
        this.organization.setType(codeableConcept);

        auditValue("type.coding[0]", sourceCells);
    }

    public void setTypeFreeText(String freeTextType, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(freeTextType);
        this.organization.setType(codeableConcept);

        auditValue("type.text", sourceCells);
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
        this.organization.setActive(active);
    }

    public void setOpenDate(Date date, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.organization, FhirExtensionUri.ACTIVE_PERIOD);
        Period period = findOrCreateOpenPeriod(extension);
        period.setStart(date);

        updateActiveStatus(extension);

        auditPeriodStartExtension(extension, sourceCells);
    }

    public void setCloseDate(Date date, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.organization, FhirExtensionUri.ACTIVE_PERIOD);
        Period period = findOrCreateOpenPeriod(extension);
        period.setStart(date);

        updateActiveStatus(extension);

        auditPeriodEndExtension(extension, sourceCells);
    }


    @Override
    public void addAddress(Address.AddressUse use) {
        Address address = new Address();
        address.setUse(use);
        this.organization.addAddress(address);
    }

    private int getLastAddressIndex() {
        if (!this.organization.hasAddress()) {
            throw new IllegalArgumentException("Need to call addAddress before setting address fields");
        }
        return this.organization.getAddress().size()-1;
    }

    @Override
    public Address getLastAddress() {
        int index = getLastAddressIndex();
        return this.organization.getAddress().get(index);
    }

    @Override
    public void addAddressLine(String line, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.addLine(line);

        int index = address.getLine().size()-1;
        auditValue("address[" + getLastAddressIndex() + "].line[" + index + "]", sourceCells);
    }

    @Override
    public void addAddressTown(String town, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.setCity(town);

        auditValue("address[" + getLastAddressIndex() + "].city", sourceCells);
    }

    @Override
    public void addAddressDistrict(String district, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.setDistrict(district);

        auditValue("address[" + getLastAddressIndex() + "].district", sourceCells);
    }

    @Override
    public void addAddressPostcode(String postcode, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.setPostalCode(postcode);

        auditValue("address[" + getLastAddressIndex() + "].postalCode", sourceCells);
    }

    @Override
    public void addAddressDisplayText(String displayText, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.setText(displayText);

        auditValue("address[" + getLastAddressIndex() + "].text", sourceCells);
    }
}
