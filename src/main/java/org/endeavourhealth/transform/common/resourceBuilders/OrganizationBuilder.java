package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class OrganizationBuilder extends ResourceBuilderBase {

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
}
