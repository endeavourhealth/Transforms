package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.CodeableConcept;

public interface HasCodeableConceptI {

    CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting);
    String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept);
    public ResourceFieldMappingAudit getAuditWrapper();
    void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept);
}
