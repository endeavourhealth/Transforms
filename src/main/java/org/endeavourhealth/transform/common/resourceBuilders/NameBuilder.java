package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.NameConverter;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Period;

import java.util.Date;

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

    public void setUse(HumanName.NameUse use, CsvCell... sourceCells) {
        name.setUse(use);

        auditNameValue("use", sourceCells);
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


    private void auditNameValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getNameJsonPrefix(this.name) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
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
