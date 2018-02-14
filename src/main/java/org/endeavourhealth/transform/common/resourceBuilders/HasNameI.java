package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.HumanName;

public interface HasNameI {

    void addName(HumanName.NameUse use);
    void addNamePrefix(String prefix, CsvCell... sourceCells);
    void addNameGiven(String given, CsvCell... sourceCells);
    void addNameFamily(String family, CsvCell... sourceCells);
    void addNameSuffix(String suffix, CsvCell... sourceCells);
    void addNameDisplayName(String displayName, CsvCell... sourceCells);
    HumanName getLastName();
}
