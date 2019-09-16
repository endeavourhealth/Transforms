package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(LocationTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Location;
    }

    public boolean shouldAlwaysTransform() {
        return false;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Location model = params.getOutputContainer().getLocations();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        Location fhir = (Location)resourceWrapper.getResource();

        String name = null;
        String typeCode = null;
        String typeDesc = null;
        String postcode = null;
        Long managingOrganisationId = null;


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
                typeCode = coding.getCode();
                typeDesc = coding.getDisplay();
            }

            if (Strings.isNullOrEmpty(typeDesc)) {
                typeDesc = cc.getText();
            }
        }

        if (fhir.hasAddress()) {
            Address address = fhir.getAddress();
            if (address.hasPostalCode()) {
                postcode = address.getPostalCode();
            }
        }

        if (fhir.hasManagingOrganization()) {
            Reference reference = fhir.getManagingOrganization();
            managingOrganisationId = transformOnDemandAndMapId(reference, SubscriberTableId.ORGANIZATION, params);
        }

        model.writeUpsert(subscriberId,
                name,
                typeCode,
                typeDesc,
                postcode,
                managingOrganisationId);

    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.LOCATION;
    }


}
