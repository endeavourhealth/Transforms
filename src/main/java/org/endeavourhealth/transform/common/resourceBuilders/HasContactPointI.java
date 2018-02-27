package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.ContactPoint;

import java.util.List;

public interface HasContactPointI {

    ContactPoint addContactPoint();
    String getContactPointJsonPrefix(ContactPoint contactPoint);
    ResourceFieldMappingAudit getAuditWrapper();
    List<ContactPoint> getContactPoint();
    void removeContactPoint(ContactPoint contactPoint);
}
