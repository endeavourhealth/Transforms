package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationEnterpriseTransformer extends AbstractEnterpriseTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(LocationEnterpriseTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Location;
    }

    public boolean shouldAlwaysTransform() {
        return false;
    }

    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {

        if (resourceWrapper.isDeleted()) {
            csvWriter.writeDelete(enterpriseId.longValue());
            return;
        }

        Location fhir = (Location)resourceWrapper.getResource();

        long id;
        String name = null;
        String typeCode = null;
        String typeDesc = null;
        String postcode = null;
        Long managingOrganisationId = null;

        id = enterpriseId.longValue();


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
            managingOrganisationId = transformOnDemandAndMapId(reference, params);
        }

        org.endeavourhealth.transform.enterprise.outputModels.Location model = (org.endeavourhealth.transform.enterprise.outputModels.Location)csvWriter;
        model.writeUpsert(id,
            name,
            typeCode,
            typeDesc,
            postcode,
            managingOrganisationId);
    }

}
