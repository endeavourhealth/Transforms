package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.NameHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.StringType;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NameBuilder {

    private HasNameI parentBuilder = null;
    private HumanName name = null;

    public NameBuilder(HasNameI parentBuilder) {
        this(parentBuilder, null);
    }

    public NameBuilder(HasNameI parentBuilder, HumanName humanName) {
        this.parentBuilder = parentBuilder;
        this.name = humanName;

        if (this.name == null) {
            this.name = parentBuilder.addName();
        }
    }

    public static NameBuilder findOrCreateForId(HasNameI parentBuilder, CsvCell idCell) {

        String idValue = idCell.getString();
        HumanName name = findForId(parentBuilder, idValue);
        if (name != null) {
            return new NameBuilder(parentBuilder, name);

        } else {
            NameBuilder ret = new NameBuilder(parentBuilder);
            ret.setId(idValue, idCell);
            return ret;
        }
    }

    private static HumanName findForId(HasNameI parentBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't remove name without ID");
        }

        List<HumanName> matches = new ArrayList<>();

        List<HumanName> names = parentBuilder.getNames();
        for (HumanName name: names) {
            //if we match on ID, then remove this name from the parent object
            if (name.hasId()
                    && name.getId().equals(idValue)) {

                matches.add(name);
            }
        }

        if (matches.isEmpty()) {
            return null;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " names for ID " + idValue);

        } else {
            return matches.get(0);
        }

    }

    public static boolean removeExistingNameById(HasNameI parentBuilder, String idValue) {
        HumanName name = findForId(parentBuilder, idValue);
        if (name != null) {

            //remove any audits we've created for the Name
            String identifierJsonPrefix = parentBuilder.getNameJsonPrefix(name);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removeName(name);
            return true;

        } else {
            return false;
        }

    }

    /*public static boolean removeExistingNameWithoutId(HasNameI parentBuilder) {

        List<HumanName> matches = new ArrayList<>();

        List<HumanName> names = parentBuilder.getNames();
        for (HumanName name: names) {
            if (!name.hasId()) {
                matches.add(name);
            }
        }

        if (matches.isEmpty()) {
            return false;
        }

        for (HumanName match: matches) {
            //remove any audits we've created for the Name
            String identifierJsonPrefix = parentBuilder.getNameJsonPrefix(match);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removeName(match);
        }
        return true;
    }*/

    public static void removeExistingNames(HasNameI parentBuilder) {
        List<HumanName> names = new ArrayList<>(parentBuilder.getNames()); //need to copy the array so we can remove while iterating
        for (HumanName name: names) {

            //remove any audits we've created for the Name
            String identifierJsonPrefix = parentBuilder.getNameJsonPrefix(name);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removeName(name);
        }
    }

    public void setUse(HumanName.NameUse use, CsvCell... sourceCells) {
        name.setUse(use);

        auditNameValue("use", sourceCells);
    }

    public void setId(String id, CsvCell... sourceCells) {
        this.name.setId(id);

        auditNameValue("id", sourceCells);
    }

    public void addPrefix(String prefix, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(prefix)) {
            return;
        }

        String[] toks = prefix.split(" ");
        for (String tok: toks) {
            //trim and handle empty tokens to get around double-spaces or weird spacing
            tok = tok.trim();
            if (!Strings.isNullOrEmpty(tok)) {

                name.addPrefix(tok);

                int index = name.getPrefix().size()-1;
                auditNameValue("prefix[" + index + "]", sourceCells);
            }
        }

        updateDisplayName(sourceCells);
    }

    public void addGiven(String given, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(given)) {
            return;
        }

        String[] toks = given.split(" ");
        for (String tok: toks) {
            //trim and handle empty tokens to get around double-spaces or weird spacing
            tok = tok.trim();
            if (!Strings.isNullOrEmpty(tok)) {

                name.addGiven(tok);

                int index = name.getGiven().size()-1;
                auditNameValue("given[" + index + "]", sourceCells);
            }
        }

        updateDisplayName(sourceCells);
    }

    public void addFamily(String family, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(family)) {
            return;
        }

        String[] toks = family.split(" ");
        for (String tok: toks) {
            //trim and handle empty tokens to get around double-spaces or weird spacing
            tok = tok.trim();
            if (!Strings.isNullOrEmpty(tok)) {

                name.addFamily(tok);

                int index = name.getFamily().size()-1;
                auditNameValue("family[" + index + "]", sourceCells);
            }
        }

        updateDisplayName(sourceCells);
    }

    public void addSuffix(String suffix, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(suffix)) {
            return;
        }

        String[] toks = suffix.split(" ");
        for (String tok: toks) {
            //trim and handle empty tokens to get around double-spaces or weird spacing
            tok = tok.trim();
            if (!Strings.isNullOrEmpty(tok)) {

                name.addSuffix(tok);

                int index = name.getSuffix().size()-1;
                auditNameValue("suffix[" + index + "]", sourceCells);
            }
        }

        updateDisplayName(sourceCells);
    }

    public void setText(String fullName, CsvCell... sourceCells) {
        name.setText(fullName);

        auditNameValue("text", sourceCells);
    }

    private void auditNameValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getNameJsonPrefix(this.name) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            if (csvCell != null) {
                audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
            }
        }
    }


    private void updateDisplayName(CsvCell... sourceCells) {

        String displayName = NameHelper.generateDisplayName(this.name);
        setText(displayName, sourceCells);
    }

    private Period getOrCreateNamePeriod() {
        Period period = null;
        if (name.hasPeriod()) {
            period = name.getPeriod();
        } else {
            period = new Period();
            name.setPeriod(period);
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

    public HumanName getNameCreated() {
        return this.name;
    }

    public void addNameNoAudit(HumanName otherName) {

        if (otherName.hasId()) {
            setId(otherName.getId());
        }

        if (otherName.hasUse()) {
            setUse(otherName.getUse());
        }

        if (otherName.hasPeriod()) {
            Period p = otherName.getPeriod();
            if (p.hasStart()) {
                setStartDate(p.getStart());
            }
            if (p.hasEnd()) {
                setEndDate(p.getEnd());
            }
        }

        if (otherName.hasPrefix()) {
            for (StringType st: otherName.getPrefix()) {
                addPrefix(st.toString());
            }
        }

        if (otherName.hasGiven()) {
            for (StringType st: otherName.getGiven()) {
                addGiven(st.toString());
            }
        }

        if (otherName.hasFamily()) {
            for (StringType st: otherName.getFamily()) {
                addFamily(st.toString());
            }
        }

        if (otherName.hasSuffix()) {
            for (StringType st: otherName.getSuffix()) {
                addSuffix(st.toString());
            }
        }

        //only carry over the text if there's nothing in the other elements because the text is automatically
        //generated when addGiven(..), addFamily(..) etc. are called
        if (otherName.hasText()
                && !otherName.hasPrefix()
                && !otherName.hasGiven()
                && !otherName.hasFamily()
                && !otherName.hasSuffix()) {

            setText(otherName.getText());
        }

        if (otherName.hasExtension()) {
            throw new RuntimeException("Name has unexpected extension");
        }

    }


    public void reset() {
        //this.name.setId(null); //do not remove any ID as that's used to match names up
        this.name.setUse(null);
        this.name.setPeriod(null);
        this.name.getPrefix().clear();
        this.name.getGiven().clear();
        this.name.getFamily().clear();
        this.name.getSuffix().clear();
        this.name.setText(null);
    }


    /**
     * removes the last added contact point if it already exists in the resource, if not will end any
     * existing active ones. Anything added after the effective date will also be removed, to handle cases
     * where we're re-processing old data.
     */
    public static void deDuplicateLastName(HasNameI resource, Date effectiveDate) throws Exception {

        List<HumanName> names = resource.getNames();
        if (names.isEmpty()) {
            return;
        }

        HumanName lastHumanName = names.get(names.size()-1);
        HumanName.NameUse lastUse = lastHumanName.getUse();

        //for feeds that have discrete records in the source data (e.g. TPP, Cerner) with their own unique IDs,
        //then this function isn't suitable as it's expected that individual records will be maintained using the ID.
        //Same goes for feeds that externally set dates on entries.
        if (lastHumanName.hasId()
                || lastHumanName.hasPeriod()) {
            throw new Exception("De-duplication function only expected to be used when no unique IDs or explicit dates available");
        }

        //make sure to roll back if we're re-processing old data
        rollBackToDate(resource, effectiveDate, lastUse);

        List<HumanName> namesToEnd = new ArrayList<>();
        boolean setStartDate = false;

        //note the start index is the one BEFORE the last one, above
        for (int i=names.size()-2; i>=0; i--) {
            HumanName name = names.get(i);

            //skip any that are of a different scope
            if (!sameUse(name, lastUse)) {
                continue;
            }

            //if we've got previous history of entries in the same scope, then this is a delta and we can set the start date
            setStartDate = true;

            //ended ones shouldn't count towards the duplicate check
            if (!PeriodHelper.isActive(name.getPeriod())) {
                continue;
            }

            //the shallow equals fn compares the value but not the period, which is what we want
            if (name.equalsShallow(lastHumanName)) {
                //if the latest has same value as this existing active one, then it's a duplicate and should be removed
                names.remove(names.size() - 1);
                return;
            }

            //if we make it here, then this one should be ended
            namesToEnd.add(name);
        }

        if (setStartDate) {
            NameBuilder builder = new NameBuilder(resource, lastHumanName);
            builder.setStartDate(effectiveDate);
        }

        //end any active ones we've found
        if (!namesToEnd.isEmpty()) {
            for (HumanName nameToEnd: namesToEnd) {
                NameBuilder builder = new NameBuilder(resource, nameToEnd);
                builder.setEndDate(effectiveDate);
            }
        }
    }

    /**
     * if we know an name is no longer active, this function will find any active name (for the system and
     * use) and end it with the given date
     */
    public static void endNames(HasNameI resource, Date effectiveDate, HumanName.NameUse useToEnd) throws Exception {
        List<HumanName> names = resource.getNames();
        if (names.isEmpty()) {
            return;
        }

        //make sure to roll back if we're re-processing old data
        rollBackToDate(resource, effectiveDate, useToEnd);

        for (int i=names.size()-1; i>=0; i--) {
            HumanName name = names.get(i);
            if (sameUse(name, useToEnd)
                    && PeriodHelper.isActive(name.getPeriod())) {

                NameBuilder builder = new NameBuilder(resource, name);
                builder.setEndDate(effectiveDate);
            }
        }
    }

    private static boolean sameUse(HumanName name, HumanName.NameUse use) {
        return name.hasUse()
                && name.getUse() == use;
    }

    /**
     * because we sometimes need to re-process past data, we need this function to essentially roll back the
     * list to what it would have been on a given date. Removes anything known to have been added on or after
     * the effective date, and un-ends anything ended on or after that date.
     */
    private static void rollBackToDate(HasNameI resource, Date effectiveDate, HumanName.NameUse use) throws Exception {
        if (use == null) {
            throw new Exception("De-duplication function only supports last entry having a use set");
        }

        List<HumanName> names = resource.getNames();
        for (int i=names.size()-1; i>=0; i--) {
            HumanName name = names.get(i);
            if (sameUse(name, use)
                    && name.hasPeriod()) {
                Period p = name.getPeriod();

                //if it was added on or after the effective date, remove it
                if (p.hasStart()
                        && !p.getStart().before(effectiveDate)) {
                    names.remove(i);
                    continue;
                }

                //if it was ended on or after the effective date, un-end it
                if (p.hasEnd()
                        && !p.getEnd().before(effectiveDate)) {
                    NameBuilder builder = new NameBuilder(resource, name);
                    builder.setEndDate(null);
                }
            }
        }
    }
}
