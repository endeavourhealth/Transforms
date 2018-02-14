package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.CodeableConcept;

public interface HasCodeableConceptI {

    CodeableConcept getOrCreateCodeableConcept(String tag);
    String getCodeableConceptJsonPath(String tag);
    public ResourceFieldMappingAudit getAuditWrapper();

}
