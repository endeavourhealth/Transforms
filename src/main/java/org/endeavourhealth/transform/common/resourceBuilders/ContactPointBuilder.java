package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Period;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

    public static boolean removeExistingAddress(HasContactPointI parentBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't remove patient contact without ID");
        }

        List<ContactPoint> matches = new ArrayList<>();

        List<ContactPoint> contactPoints = parentBuilder.getContactPoint();
        for (ContactPoint contactPoint: contactPoints) {
            //if we match on ID, then remove this contactPoint from the parent object
            if (contactPoint.hasId()
                    && contactPoint.getId().equals(idValue)) {

                matches.add(contactPoint);
            }
        }

        if (matches.isEmpty()) {
            return false;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " contactPoints for ID " + idValue);

        } else {
            ContactPoint contactPoint = matches.get(0);

            //remove any audits we've created for the CodeableConcept
            String identifierJsonPrefix = parentBuilder.getContactPointJsonPrefix(contactPoint);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removeContactPoint(contactPoint);
            return true;
        }
    }

    public void setUse(ContactPoint.ContactPointUse use, CsvCell... sourceCells) {
        contactPoint.setUse(use);

        auditContactPointValue("use", sourceCells);
    }

    public void setId(String id, CsvCell... sourceCells) {
        this.contactPoint.setId(id);

        auditContactPointValue("id", sourceCells);
    }

    public void setSystem(ContactPoint.ContactPointSystem system, CsvCell... sourceCells) {
        contactPoint.setSystem(system);

        auditContactPointValue("system", sourceCells);
    }

    public void setValue(String value, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(value)) {
            return;
        }

        contactPoint.setValue(value);

        auditContactPointValue("value", sourceCells);
    }

    private void auditContactPointValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getContactPointJsonPrefix(this.contactPoint) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            if (csvCell != null) {
                audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
            }
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

        auditContactPointValue("period.start", sourceCells);
    }

    public void setEndDate(Date date, CsvCell... sourceCells) {
        getOrCreateNamePeriod().setEnd(date);

        auditContactPointValue("period.end", sourceCells);
    }
}
