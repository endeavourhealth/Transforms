package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.hl7.fhir.instance.model.Address;

import java.util.List;

public interface HasAddressI {

    Address addAddress();
    String getAddressJsonPrefix(Address address);
    public ResourceFieldMappingAudit getAuditWrapper();
    List<Address> getAddresses();
    void removeAddress(Address address);
}
