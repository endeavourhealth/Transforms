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

    public NameBuilder(HasNameI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    /**
     * start the creation of a new name on the parent object (patient, practitioner etc.)
     */
    public void beginName(HumanName.NameUse use, CsvCell... sourceCells) {
        parentBuilder.addName(use);

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

                HumanName name = parentBuilder.getLastName();
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

                HumanName name = parentBuilder.getLastName();
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

                HumanName name = parentBuilder.getLastName();
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

                HumanName name = parentBuilder.getLastName();
                name.addSuffix(tok);

                int index = name.getSuffix().size()-1;
                auditNameValue("suffix[" + index + "]", sourceCells);
            }
        }

        updateDisplayName(sourceCells);
    }


    private void auditNameValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = parentBuilder.getLastNameJsonPrefix() + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
        }
    }


    private void updateDisplayName(CsvCell... sourceCells) {
        HumanName humanName = parentBuilder.getLastName();
        String displayName = NameConverter.generateDisplayName(humanName);

        HumanName name = parentBuilder.getLastName();
        name.setText(displayName);

        auditNameValue("text", sourceCells);
    }

    private Period getOrCreateNamePeriod() {
        HumanName name = parentBuilder.getLastName();
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
