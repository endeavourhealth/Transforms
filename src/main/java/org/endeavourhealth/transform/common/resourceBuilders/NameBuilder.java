package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.NameConverter;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Period;

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

    public static boolean removeExistingNameById(HasNameI parentBuilder, String idValue) {
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
            return false;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " names for ID " + idValue);

        } else {
            HumanName name = matches.get(0);

            //remove any audits we've created for the Name
            String identifierJsonPrefix = parentBuilder.getNameJsonPrefix(name);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removeName(name);
            return true;
        }
    }

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

    public void addFullName(String fullName, CsvCell... sourceCells) {

        name.setText(fullName);

        auditNameValue("text", sourceCells);
    }

    private void auditNameValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getNameJsonPrefix(this.name) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            if (csvCell != null) {
                audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
            }
        }
    }


    private void updateDisplayName(CsvCell... sourceCells) {

        String displayName = NameConverter.generateDisplayName(this.name);
        name.setText(displayName);

        auditNameValue("text", sourceCells);
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
}
