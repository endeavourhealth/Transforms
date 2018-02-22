package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Period;

import java.util.Date;

public class IdentifierBuilder {

    private HasIdentifierI parentBuilder = null;
    private Identifier identifier = null;

    public IdentifierBuilder(HasIdentifierI parentBuilder) {
        this(parentBuilder, null);
    }

    public IdentifierBuilder(HasIdentifierI parentBuilder, Identifier identifier) {
        this.parentBuilder = parentBuilder;
        this.identifier = identifier;
        if (this.identifier == null) {
            this.identifier = parentBuilder.addIdentifier();
        }
    }

    public void setUse(Identifier.IdentifierUse use, CsvCell... sourceCells) {
        identifier.setUse(use);

        auditNameValue("use", sourceCells);
    }

    public void setSystem(String system, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(system)) {
            return;
        }

        identifier.setSystem(system);

        auditNameValue("system", sourceCells);
    }

    public void setValue(String value, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(value)) {
            return;
        }

        identifier.setValue(value);

        auditNameValue("value", sourceCells);
    }

    private void auditNameValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getIdentifierJsonPrefix(identifier) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
        }
    }

    private Period getOrCreateNamePeriod() {
        Period period = null;
        if (identifier.hasPeriod()) {
            period = identifier.getPeriod();
        } else {
            period = new Period();
            identifier.setPeriod(period);
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
