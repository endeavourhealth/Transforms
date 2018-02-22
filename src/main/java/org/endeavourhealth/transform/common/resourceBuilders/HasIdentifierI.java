package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.Identifier;

public interface HasIdentifierI {

    void addIdentifier();
    Identifier getLastIdentifier();
    String getLastIdentifierJsonPrefix();
    ResourceFieldMappingAudit getAuditWrapper();
}
