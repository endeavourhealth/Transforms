package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.DomainResource;
import org.hl7.fhir.instance.model.Resource;

public class GenericBuilder extends ResourceBuilderBase {

    private Resource resource;

    public GenericBuilder(Resource resource) {
        this.resource = resource;
    }

    public GenericBuilder(Resource resource, ResourceFieldMappingAudit auditWrapper) {
        super(auditWrapper);
        this.resource = resource;
    }

    @Override
    public DomainResource getResource() {
        return (DomainResource)resource;
    }
}
