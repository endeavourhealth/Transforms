package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Period;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

    public static boolean removeExistingIdentifier(HasIdentifierI parentBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't remove identifier without ID");
        }

        List<Identifier> matches = new ArrayList<>();

        List<Identifier> identifieres = parentBuilder.getIdentifiers();
        for (Identifier identifier: identifieres) {
            //if we match on ID, then remove this identifier from the parent object
            if (identifier.hasId()
                    && identifier.getId().equals(idValue)) {

                matches.add(identifier);
            }
        }

        if (matches.isEmpty()) {
            return false;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " identifiers for ID " + idValue);

        } else {
            Identifier identifier = matches.get(0);
            parentBuilder.removeIdentifier(identifier);
            return true;
        }
    }

    public void setUse(Identifier.IdentifierUse use, CsvCell... sourceCells) {
        identifier.setUse(use);

        auditIdentifierValue("use", sourceCells);
    }

    public void setId(String id, CsvCell... sourceCells) {
        this.identifier.setId(id);

        auditIdentifierValue("id", sourceCells);
    }


    public void setSystem(String system, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(system)) {
            return;
        }

        identifier.setSystem(system);

        auditIdentifierValue("system", sourceCells);
    }

    public void setValue(String value, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(value)) {
            return;
        }

        identifier.setValue(value);

        auditIdentifierValue("value", sourceCells);
    }

    private void auditIdentifierValue(String jsonSuffix, CsvCell... sourceCells) {

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

        auditIdentifierValue("period.start", sourceCells);
    }

    public void setEndDate(Date date, CsvCell... sourceCells) {
        getOrCreateNamePeriod().setEnd(date);

        auditIdentifierValue("period.end", sourceCells);
    }
}
