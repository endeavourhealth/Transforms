package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.Identifier;

import java.util.List;

public interface HasIdentifierI {

    Identifier addIdentifier();
    String getIdentifierJsonPrefix(Identifier identifier);
    ResourceFieldMappingAudit getAuditWrapper();
    List<Identifier> getIdentifiers();
    void removeIdentifier(Identifier identifier);
}
