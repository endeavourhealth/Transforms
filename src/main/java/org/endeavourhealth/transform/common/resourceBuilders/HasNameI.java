package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.HumanName;

public interface HasNameI {

    HumanName addName();
    String getNameJsonPrefix(HumanName name);
    public ResourceFieldMappingAudit getAuditWrapper();

}
