package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.DomainResource;

public interface HasContainedListI {

    DomainResource getResource();
    String getContainedListExtensionUrl();
    void auditValue(String jsonField, CsvCell... sourceCells);
}
