package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.HumanName;

import java.util.List;

public interface HasNameI {

    HumanName addName();
    String getNameJsonPrefix(HumanName name);
    public ResourceFieldMappingAudit getAuditWrapper();
    List<HumanName> getNames();
    void removeName(HumanName name);

}
