package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.PeriodHelper;
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
                audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
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

    /**
     * removes the last added contact point if it already exists in the resource, if not will end any
     * existing active ones. Anything added after the effective date will also be removed, to handle cases
     * where we're re-processing old data.
     */
    public static void deDuplicateLastIdentifier(HasIdentifierI resource, Date effectiveDate) throws Exception {

        List<Identifier> identifiers = resource.getIdentifiers();
        if (identifiers.isEmpty()) {
            return;
        }

        Identifier lastIdentifier = identifiers.get(identifiers.size()-1);
        String lastSystem = lastIdentifier.getSystem();
        Identifier.IdentifierUse lastUse = lastIdentifier.getUse();

        //for feeds that have discrete records in the source data (e.g. TPP, Cerner) with their own unique IDs,
        //then this function isn't suitable as it's expected that individual records will be maintained using the ID.
        //Same goes for feeds that externally set dates on entries.
        if (lastIdentifier.hasId()
                || lastIdentifier.hasPeriod()) {
            throw new Exception("De-duplication function only expected to be used when no unique IDs or explicit dates available");
        }

        //make sure to roll back if we're re-processing old data
        rollBackToDate(resource, effectiveDate, lastSystem, lastUse);

        List<Identifier> identifiersToEnd = new ArrayList<>();
        boolean setStartDate = false;

        //note the start index is the one BEFORE the last one, above
        for (int i=identifiers.size()-2; i>=0; i--) {
            Identifier identifier = identifiers.get(i);

            //skip any that are of a different scope
            if (!sameSystemAndUse(identifier, lastSystem, lastUse)) {
                continue;
            }

            //if we've got previous history of entries in the same scope, then this is a delta and we can set the start date
            setStartDate = true;

            //ended ones shouldn't count towards the duplicate check
            if (!PeriodHelper.isActive(identifier.getPeriod())) {
                continue;
            }

            //the shallow equals fn compares the value but not the period, which is what we want
            if (identifier.equalsShallow(lastIdentifier)) {
                //if the latest has same value as this existing active one, then it's a duplicate and should be removed
                identifiers.remove(identifiers.size() - 1);
                return;
            }

            //if we make it here, then this one should be ended
            identifiersToEnd.add(identifier);
        }

        if (setStartDate) {
            IdentifierBuilder builder = new IdentifierBuilder(resource, lastIdentifier);
            builder.setStartDate(effectiveDate);
        }

        //end any active ones we've found
        if (!identifiersToEnd.isEmpty()) {
            for (Identifier identifierToEnd: identifiersToEnd) {
                IdentifierBuilder builder = new IdentifierBuilder(resource, identifierToEnd);
                builder.setEndDate(effectiveDate);
            }
        }
    }

    /**
     * if we know an identifier is no longer active, this function will find any active identifier (for the system and
     * use) and end it with the given date
     */
    public static void endIdentifiers(HasIdentifierI resource, Date effectiveDate, String systemToEnd, Identifier.IdentifierUse useToEnd) throws Exception {
        List<Identifier> identifiers = resource.getIdentifiers();
        if (identifiers.isEmpty()) {
            return;
        }

        //make sure to roll back if we're re-processing old data
        rollBackToDate(resource, effectiveDate, systemToEnd, useToEnd);

        for (int i=identifiers.size()-1; i>=0; i--) {
            Identifier identifier = identifiers.get(i);
            if (sameSystemAndUse(identifier, systemToEnd, useToEnd)
                    && PeriodHelper.isActive(identifier.getPeriod())) {

                IdentifierBuilder builder = new IdentifierBuilder(resource, identifier);
                builder.setEndDate(effectiveDate);
            }
        }
    }

    private static boolean sameSystemAndUse(Identifier identifier, String system, Identifier.IdentifierUse use) {
        return identifier.hasSystem()
                && identifier.hasUse()
                && identifier.getSystem().equals(system)
                && identifier.getUse() == use;
    }

    /**
     * because we sometimes need to re-process past data, we need this function to essentially roll back the
     * list to what it would have been on a given date. Removes anything known to have been added on or after
     * the effective date, and un-ends anything ended on or after that date.
     */
    private static void rollBackToDate(HasIdentifierI resource, Date effectiveDate, String system, Identifier.IdentifierUse use) throws Exception {
        if (Strings.isNullOrEmpty(system)) {
            throw new Exception("De-duplication function only supports last entry having a system set");
        }
        if (use == null) {
            throw new Exception("De-duplication function only supports last entry having a use set");
        }

        List<Identifier> identifiers = resource.getIdentifiers();
        for (int i=identifiers.size()-1; i>=0; i--) {
            Identifier identifier = identifiers.get(i);
            if (sameSystemAndUse(identifier, system, use)
                    && identifier.hasPeriod()) {
                Period p = identifier.getPeriod();

                //if it was added on or after the effective date, remove it
                if (p.hasStart()
                        && !p.getStart().before(effectiveDate)) {
                    identifiers.remove(i);
                    continue;
                }

                //if it was ended on or after the effective date, un-end it
                if (p.hasEnd()
                        && !p.getEnd().before(effectiveDate)) {
                    IdentifierBuilder builder = new IdentifierBuilder(resource, identifier);
                    builder.setEndDate(null);
                }
            }
        }
    }
}
