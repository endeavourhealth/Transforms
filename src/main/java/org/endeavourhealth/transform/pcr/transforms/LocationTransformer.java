package org.endeavourhealth.transform.pcr.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

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
        Long organisationId = params.getPcrOrganisationId();
        String name = null;
        Long typeTermId = null;
        Long addressId = null;
        boolean isActive = true;
        Long parentLocationId = null;
        Date startDate = null;
        Date endDate = null;

        id = pcrId.longValue();


        if (fhir.hasName()) {
            name = fhir.getName();
        } else {
            //name can't be null in the DB, so just use an empty string
            name = "";
        }
//TODO restore
//        if (fhir.hasType()) {
//            CodeableConcept cc = fhir.getType();
//            if (cc.hasCoding()) {
//                //we only ever use a single coding, so just get the first
//                Coding coding = cc.getCoding().get(0);
//                if (StringUtils.isNumeric(coding.getCode())) {
//                    typeTermId = getOrCreateConceptId("ServiceDeliveryLocationRoleType." + coding.getCode());
//                }
//            }
//        }

        if (fhir.hasAddress()) {
            Address address = fhir.getAddress();
            if ((address.getId() != null) && (StringUtils.isNumeric(address.getId()))) {
                addressId = Long.parseLong(address.getId());
                org.endeavourhealth.transform.pcr.outputModels.Address addressWriter = (org.endeavourhealth.transform.pcr.outputModels.Address) csvWriter;
                List<StringType> addressList = address.getLine();
                String al1 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 0);
                String al2 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 1);
                String al3 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 2);
                String al4 = org.endeavourhealth.transform.ui.helpers.AddressHelper.getLine(addressList, 3);
                String postcode = address.getPostalCode();
                //TODO get uprn (OS ref) and approximation. See TODO in Address outputModel

                Long propertyTypeId = IMClient.getOrCreateConceptId("Address.AddressUse." + address.getUse().toCode());
                addressWriter.writeUpsert(addressId, al1, al2, al3, al4, postcode,
                        null, null, propertyTypeId);

                if (address.hasPeriod()) {
                    Period period = address.getPeriod();
                    if (period.hasStart()) {
                        startDate = period.getStart();
                    }
                    if (period.hasEnd()) {
                        endDate = period.getEnd();
                    }
                }
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
                startDate,
                endDate,
                isActive,
                parentLocationId);
    }

}
