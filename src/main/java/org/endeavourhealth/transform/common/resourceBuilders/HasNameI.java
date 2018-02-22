package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.HumanName;

public interface HasNameI {

    void addName(HumanName.NameUse use);
    HumanName getLastName();
    String getLastNameJsonPrefix();
    public ResourceFieldMappingAudit getAuditWrapper();

    /*void addNamePrefix(String prefix, CsvCell... sourceCells);
    void addNameGiven(String given, CsvCell... sourceCells);
    void addNameFamily(String family, CsvCell... sourceCells);
    void addNameSuffix(String suffix, CsvCell... sourceCells);
    void addNameDisplayName(String displayName, CsvCell... sourceCells);*/

}
