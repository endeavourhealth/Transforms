package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.NameConverter;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.HumanName;

import java.util.Date;

public class NameBuilder {

    private HasNameI parentBuilder = null;

    public NameBuilder(HasNameI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    /**
     * start the creation of a new name on the parent object (patient, practitioner etc.)
     */
    public void beginName(HumanName.NameUse use) {
        parentBuilder.addName(use);
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
                parentBuilder.addNamePrefix(tok, sourceCells);
            }
        }

        updateDisplayName();
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
                parentBuilder.addNameGiven(tok, sourceCells);
            }
        }

        updateDisplayName();
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
                parentBuilder.addNameFamily(tok, sourceCells);
            }
        }

        updateDisplayName();
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
                parentBuilder.addNameSuffix(tok, sourceCells);
            }
        }

        updateDisplayName();
    }

    private void updateDisplayName() {
        HumanName humanName = parentBuilder.getLastName();
        String displayName = NameConverter.generateDisplayName(humanName);
        parentBuilder.addNameDisplayName(displayName);
    }


    public void setStartDate(Date date, CsvCell... sourceCells) {
        //TODO - need to implement
    }

    public void setEndDate(Date date, CsvCell... sourceCells) {
        //TODO - need to implement
    }
}
