package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Period;

import java.util.Date;

public class ContactPointBuilder {

    private HasContactPointI parentBuilder = null;

    public ContactPointBuilder(HasContactPointI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    /**
     * start the creation of a new ContactPoint on the parent object (patient etc.)
     */
    public void addContactPoint() {
        parentBuilder.addContactPoint();
    }

    public void setUse(ContactPoint.ContactPointUse use, CsvCell... sourceCells) {

        ContactPoint ContactPoint = parentBuilder.getLastContactPoint();
        ContactPoint.setUse(use);

        auditNameValue("use", sourceCells);
    }

    public void setSystem(ContactPoint.ContactPointSystem system, CsvCell... sourceCells) {

        ContactPoint ContactPoint = parentBuilder.getLastContactPoint();
        ContactPoint.setSystem(system);

        auditNameValue("system", sourceCells);
    }

    public void setValue(String value, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(value)) {
            return;
        }

        ContactPoint ContactPoint = parentBuilder.getLastContactPoint();
        ContactPoint.setValue(value);

        auditNameValue("value", sourceCells);
    }

    private void auditNameValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getLastContactPointJsonPrefix() + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
        }
    }

    private Period getOrCreateNamePeriod() {
        ContactPoint ContactPoint = parentBuilder.getLastContactPoint();
        Period period = null;
        if (ContactPoint.hasPeriod()) {
            period = ContactPoint.getPeriod();
        } else {
            period = new Period();
            ContactPoint.setPeriod(period);
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
