package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.ContactPoint;

public interface HasContactPointI {
    
    void addContactPoint();
    ContactPoint getLastContactPoint();
    String getLastContactPointJsonPrefix();
    ResourceFieldMappingAudit getAuditWrapper();
}
