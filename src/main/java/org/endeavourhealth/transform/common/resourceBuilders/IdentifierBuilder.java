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

    public static IdentifierBuilder findOrCreateForId(HasIdentifierI parentBuilder, CsvCell idCell) {

        String idValue = idCell.getString();
        Identifier identifier = findForId(parentBuilder, idValue);
        if (identifier != null) {
            return new IdentifierBuilder(parentBuilder, identifier);

        } else {
            IdentifierBuilder ret = new IdentifierBuilder(parentBuilder, identifier);
            ret.setId(idValue, idCell);
            return ret;
        }
    }

    private static Identifier findForId(HasIdentifierI parentBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't remove identifier without ID");
        }

        List<Identifier> matches = new ArrayList<>();

        List<Identifier> identifiers = parentBuilder.getIdentifiers();
        for (Identifier identifier: identifiers) {
            //if we match on ID, then remove this identifier from the parent object
            if (identifier.hasId()
                    && identifier.getId().equals(idValue)) {

                matches.add(identifier);
            }
        }

        if (matches.isEmpty()) {
            return null;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " identifiers for ID " + idValue);

        } else {
            return matches.get(0);
        }
    }


    public static boolean removeExistingIdentifierById(HasIdentifierI parentBuilder, String idValue) {

        Identifier identifier = findForId(parentBuilder, idValue);
        if (identifier != null) {
            //remove any audits we've added for this Identifier
            String identifierJsonPrefix = parentBuilder.getIdentifierJsonPrefix(identifier);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            //and remove the Identifier itself
            parentBuilder.removeIdentifier(identifier);
            return true;

        } else {
            return false;
        }
    }

    public static void removeExistingIdentifiersForSystem(HasIdentifierI parentBuilder, String identifierSystem) {
        List<Identifier> matches = findExistingIdentifiersForSystem(parentBuilder, identifierSystem);
        for (Identifier match: matches) {

            //remove any audits we've added for this Identifier
            String identifierJsonPrefix = parentBuilder.getIdentifierJsonPrefix(match);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removeIdentifier(match);
        }
    }

    public static List<Identifier> findExistingIdentifiersForSystem(HasIdentifierI parentBuilder, String identifierSystem) {
        if (Strings.isNullOrEmpty(identifierSystem)) {
            throw new IllegalArgumentException("Can't match identifier without system");
        }

        List<Identifier> matches = new ArrayList<>();

        List<Identifier> identifiers = parentBuilder.getIdentifiers();
        for (Identifier identifier: identifiers) {

            if (!identifier.hasSystem()
                    || !identifier.getSystem().equalsIgnoreCase(identifierSystem)) {
                continue;
            }

            matches.add(identifier);
        }

        return matches;
    }

    public Identifier getIdentifier() {
        return identifier;
    }

    /*public static boolean removeExistingIdentifiersBySystem(HasIdentifierI parentBuilder, String identifierSystem) {
        if (Strings.isNullOrEmpty(identifierSystem)) {
            throw new IllegalArgumentException("Can't match identifier without system");
        }

        List<Identifier> matches = new ArrayList<>();

        List<Identifier> identifiers = parentBuilder.getIdentifiers();
        for (Identifier identifier: identifiers) {

            if (!identifier.hasSystem()
                    || !identifier.getSystem().equalsIgnoreCase(identifierSystem)) {
                continue;
            }

            matches.add(identifier);
        }

        if (matches.isEmpty()) {
            return false;

        } else {
            for (Identifier identifier: matches) {
                parentBuilder.removeIdentifier(identifier);
            }
            return true;
        }
        *//*if (matches.isEmpty()) {
            return false;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " identifiers for system " + identifierSystem);

        } else {
            Identifier identifier = matches.get(0);
            parentBuilder.removeIdentifier(identifier);
            return true;
        }*//*
    }*/

    /*public static boolean hasIdentifier(HasIdentifierI parentBuilder, String identifierSystem, String identifierValue) {
        if (Strings.isNullOrEmpty(identifierSystem)) {
            throw new IllegalArgumentException("Can't match identifier without system");
        }
        if (Strings.isNullOrEmpty(identifierValue)) {
            throw new IllegalArgumentException("Can't match identifier without value");
        }

        List<Identifier> identifiers = parentBuilder.getIdentifiers();
        for (Identifier identifier: identifiers) {

            if (!identifier.hasSystem()
                    || !identifier.getSystem().equalsIgnoreCase(identifierSystem)) {
                continue;
            }

            if (!identifier.hasValue()
                    || !identifier.getValue().equalsIgnoreCase(identifierValue)) {
                continue;
            }

            return true;
        }

        return false;
    }*/

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


    public void reset() {
        //this.identifier.setId(null); //never clear this as it's used for matching
        this.identifier.setValue(null);
        this.identifier.setPeriod(null);
        this.identifier.setSystem(null);
        this.identifier.setAssigner(null);
        this.identifier.setType(null);
        this.identifier.setUse(null);
    }

    public void addIdentifierNoAudit(Identifier otherIdentifier) {

        if (otherIdentifier.hasId()) {
            setId(otherIdentifier.getId());
        }

        if (otherIdentifier.hasValue()) {
            setValue(otherIdentifier.getValue());
        }

        if (otherIdentifier.hasPeriod()) {
            Period p = otherIdentifier.getPeriod();
            if (p.hasStart()) {
                setStartDate(p.getStart());
            }
            if (p.hasEnd()) {
                setEndDate(p.getEnd());
            }
        }

        if (otherIdentifier.hasSystem()) {
            setSystem(otherIdentifier.getSystem());
        }


        if (otherIdentifier.hasAssigner()) {
            //we don't use this, so don't expect to have it
            throw new RuntimeException("Identifier has unexpected assigner property");
        }

        if (otherIdentifier.hasType()) {
            //we don't use this, so don't expect to have it
            throw new RuntimeException("Identifier has unexpected type property");
        }

        if (otherIdentifier.hasExtension()) {
            throw new RuntimeException("Identifier has unexpected extension");
        }

        if (otherIdentifier.hasUse()) {
            setUse(otherIdentifier.getUse());
        }

    }
}
