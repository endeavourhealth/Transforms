package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.NameHelper;
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
                if (csvCell.getOldStyleAuditId() != null) {
                    audit.auditValueOldStyle(csvCell.getOldStyleAuditId(), csvCell.getColIndex(), jsonField);
                } else {
                    audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
                }
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
}
