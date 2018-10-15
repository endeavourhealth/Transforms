package org.endeavourhealth.transform.pcr.transforms;

import com.google.common.base.Strings;
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

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Location fhir = (Location)resource;

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

        org.endeavourhealth.transform.pcr.outputModels.Location model = (org.endeavourhealth.transform.pcr.outputModels.Location)csvWriter;
        model.writeUpsert(id,
            name,
            typeCode,
            typeDesc,
            postcode,
            managingOrganisationId);
    }

}
