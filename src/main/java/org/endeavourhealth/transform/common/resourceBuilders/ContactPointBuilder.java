package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Period;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;

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

    public ContactPoint getContactPoint() {
        return contactPoint;
    }

    public static ContactPointBuilder findOrCreateForId(HasContactPointI parentBuilder, CsvCell idCell) {

        String idValue = idCell.getString();
        ContactPoint contactPoint = findForId(parentBuilder, idValue);
        if (contactPoint != null) {
            return new ContactPointBuilder(parentBuilder, contactPoint);

        } else {
            ContactPointBuilder ret = new ContactPointBuilder(parentBuilder, contactPoint);
            ret.setId(idValue, idCell);
            return ret;
        }

    }

    private static ContactPoint findForId(HasContactPointI parentBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't remove patient contact without ID");
        }

        List<ContactPoint> matches = new ArrayList<>();

        List<ContactPoint> contactPoints = parentBuilder.getContactPoint();
        for (ContactPoint contactPoint : contactPoints) {
            //if we match on ID, then remove this contactPoint from the parent object
            if (contactPoint.hasId()
                    && contactPoint.getId().equals(idValue)) {

                matches.add(contactPoint);
            }
        }

        if (matches.isEmpty()) {
            return null;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " contactPoints for ID " + idValue);

        } else {
            return matches.get(0);
        }
    }


    public static boolean removeExistingContactPointById(HasContactPointI parentBuilder, String idValue) {
        ContactPoint contactPoint = findForId(parentBuilder, idValue);
        if (contactPoint != null) {
            //remove any audits we've created for the ContactPoint
            String contactPointJsonPrefix = parentBuilder.getContactPointJsonPrefix(contactPoint);
            parentBuilder.getAuditWrapper().removeAudit(contactPointJsonPrefix);

            parentBuilder.removeContactPoint(contactPoint);
            return true;

        } else {
            return false;
        }
    }

    public static void removeExistingContactPointsBySystem(HasContactPointI parentBuilder, ContactPoint.ContactPointSystem system) {
        List<ContactPoint> matches = new ArrayList<>();
        List<ContactPoint> contactPoints = parentBuilder.getContactPoint();

        ListIterator<ContactPoint> iterator = contactPoints.listIterator();
        while (iterator.hasNext()) {
            ContactPoint contactPoint = iterator.next();
            if (contactPoint.hasSystem() && contactPoint.getSystem().equals(system)) {
                matches.add(contactPoint);
            }
        }
        if (!matches.isEmpty()) {
            String contactPointJsonPrefix;
            for (ContactPoint matchedContactPoint : matches) {
                //remove any audits we've created for the ContactPoint
                contactPointJsonPrefix = parentBuilder.getContactPointJsonPrefix(matchedContactPoint);
                parentBuilder.getAuditWrapper().removeAudit(contactPointJsonPrefix);
                parentBuilder.removeContactPoint(matchedContactPoint);
            }
        }

    }

    public static void removeExistingContactPoints(HasContactPointI parentBuilder) {

        List<ContactPoint> contactPoints = parentBuilder.getContactPoint();
        for (ContactPoint contactPoint : contactPoints) {

            //remove any audits we've created for the ContactPoint
            String contactPointJsonPrefix = parentBuilder.getContactPointJsonPrefix(contactPoint);
            parentBuilder.getAuditWrapper().removeAudit(contactPointJsonPrefix);
        }

        //clear all contact points
        parentBuilder.getContactPoint().clear();
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
        for (CsvCell csvCell : sourceCells) {
            if (csvCell != null) {
                if (csvCell.getOldStyleAuditId() != null) {
                    audit.auditValueOldStyle(csvCell.getOldStyleAuditId(), csvCell.getColIndex(), jsonField);
                } else {
                    audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
                }
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

    public void reset() {
        //this.contactPoint.setId(null); //do not remove any ID as that's used to match names up
        this.contactPoint.setValue(null);
        this.contactPoint.setUse(null);
        this.contactPoint.setSystem(null);
        this.contactPoint.setPeriod(null);
        this.contactPoint.setRankElement(null); //have to set the element to null in this weird case
    }

    public void addContactPointNoAudit(ContactPoint otherContactPoint) {
        if (otherContactPoint.hasId()) {
            setId(otherContactPoint.getId());
        }

        if (otherContactPoint.hasValue()) {
            setValue(otherContactPoint.getValue());
        }

        if (otherContactPoint.hasUse()) {
            setUse(otherContactPoint.getUse());
        }

        if (otherContactPoint.hasSystem()) {
            setSystem(otherContactPoint.getSystem());
        }

        if (otherContactPoint.hasPeriod()) {
            Period p = otherContactPoint.getPeriod();
            if (p.hasStart()) {
                setStartDate(p.getStart());
            }
            if (p.hasEnd()) {
                setEndDate(p.getEnd());
            }
        }

        if (otherContactPoint.hasRank()) {
            //we don't use rank, so throw an error if we have it set
            throw new RuntimeException("Not expecting rank element in ContactPoint as not used");
        }

        if (otherContactPoint.hasExtension()) {
            throw new RuntimeException("ContactPoint has unexpected extension");
        }
    }
}
