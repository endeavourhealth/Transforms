package org.endeavourhealth.transform.pcr.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(LocationTransformer.class);

    public boolean shouldAlwaysTransform() {
        return false;
    }

    protected void transformResource(Long pcrId,
                                     Resource resource,
                                     AbstractPcrCsvWriter csvWriter,
                                     PcrTransformParams params) throws Exception {

        Location fhir = (Location) resource;

        long id;
        Long organisationId = params.getEnterpriseOrganisationId();
        String name = null;
        Long typeTermId = null;
        Long addressId = null;
        boolean isActive = true;
        Long parentLocationId = null;

        id = pcrId.longValue();


        if (fhir.hasName()) {
            name = fhir.getName();
        } else {
            //name can't be null in the DB, so just use an empty string
            name = "";
        }

        if (fhir.hasType()) {
            CodeableConcept cc = fhir.getType();
            if (cc.hasCoding()) {
                //we only ever use a single coding, so just get the first
                Coding coding = cc.getCoding().get(0);
                if (StringUtils.isNumeric(coding.getCode())) {
                    typeTermId = IMClient.getOrCreateConceptId("ServiceDeliveryLocationRoleType." + coding.getCode());
                }
            }
        }

        if (fhir.hasAddress()) {
            Address address = fhir.getAddress();
            if ((address.getId() != null) && (StringUtils.isNumeric(address.getId()))) {
                addressId = Long.parseLong(address.getId());
            }
        }

        if (fhir.hasStatus()) {
            isActive = (fhir.getStatus().equals(Location.LocationStatus.ACTIVE));
        }


        org.endeavourhealth.transform.pcr.outputModels.Location model = (org.endeavourhealth.transform.pcr.outputModels.Location) csvWriter;
        model.writeUpsert(id,
                organisationId,
                name,
                typeTermId,
                addressId,
                isActive,
                parentLocationId);
    }

}
