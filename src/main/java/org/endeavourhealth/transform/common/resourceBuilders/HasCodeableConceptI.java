package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.CodeableConcept;

public interface HasCodeableConceptI {

    CodeableConcept createNewCodeableConcept(String tag);
    String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept);
    public ResourceFieldMappingAudit getAuditWrapper();

}
