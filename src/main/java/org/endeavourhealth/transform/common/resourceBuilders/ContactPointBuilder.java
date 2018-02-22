package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Period;

import java.util.Date;

public class ContactPointBuilder {

    private HasContactPointI parentBuilder = null;
    private ContactPoint contactPoint = null;

    public ContactPointBuilder(HasContactPointI parentBuilder) {
        this(parentBuilder, null);
    }

    public ContactPointBuilder(HasContactPointI parentBuilder, ContactPoint contactPoint) {
        this.parentBuilder = parentBuilder;
        this.contactPoint = contactPoint;

        if (this.contactPoint == null) {
            this.contactPoint = parentBuilder.addContactPoint();
        }
    }

    public void setUse(ContactPoint.ContactPointUse use, CsvCell... sourceCells) {
        contactPoint.setUse(use);

        auditNameValue("use", sourceCells);
    }

    public void setSystem(ContactPoint.ContactPointSystem system, CsvCell... sourceCells) {
        contactPoint.setSystem(system);

        auditNameValue("system", sourceCells);
    }

    public void setValue(String value, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(value)) {
            return;
        }

        contactPoint.setValue(value);

        auditNameValue("value", sourceCells);
    }

    private void auditNameValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getContactPointJsonPrefix(this.contactPoint) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
        }
    }

    private Period getOrCreateNamePeriod() {
        Period period = null;
        if (contactPoint.hasPeriod()) {
            period = contactPoint.getPeriod();
        } else {
            period = new Period();
            contactPoint.setPeriod(period);
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
}
