package org.endeavourhealth.transform.pcr.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.Organisation;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrganizationTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OrganizationTransformer.class);

    public boolean shouldAlwaysTransform() {
        return false;
    }

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Organization fhir = (Organization)resource;

        long id;
        String serviceId = params.getServiceId().toString();
        String systemId =  params.getSystemId().toString();
        String odsCode = null;
        String name = null;
        Long typeCode = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        Long mainLocationId = null;
        boolean isActive = true;
        Long parentOrganisationId = null;

        id = pcrId.longValue();

        //LOG.trace("Transforming Organisation " + fhir.getId() + " as enterprise ID " + id);

        if (fhir.hasIdentifier()) {
            odsCode = IdentifierHelper.findIdentifierValue(fhir.getIdentifier(), FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        }

        //if the organisation ODS code matches the one we're filing data for, replace the ID with the ID
        //we've pre-generated to use as our org ID
        /*if (odsCode != null
                && odsCode.equalsIgnoreCase(extractOrgOdsCode)) {
            EnterpriseIdHelper.saveEnterpriseOrganisationId(extractOrgOdsCode, enterpriseId);
        }*/

        //we have at least one Emis org without a name, which is against their spec, but we need to handle it
        if (fhir.hasName()) {
            name = fhir.getName();
        } else {
            name = "";
        }
        //name = fhir.getName();

        if (fhir.hasPartOf()) {
            Reference partOfReference = fhir.getPartOf();
            parentOrganisationId = transformOnDemandAndMapId(partOfReference, params);
        }

        if (fhir.hasType()) {
            CodeableConcept cc = fhir.getType();
            for (Coding coding: cc.getCoding()) {
                if (coding.getSystem()!=null && coding.getSystem().equals(FhirValueSetUri.VALUE_SET_ORGANISATION_TYPE)) {
                    typeCode =    IMClient.getConceptId("FhirValueSetUri.VALUE_SET_ORGANISATION_TYPE");
                    //TODO review this - Uri mappings not firm yet
                }
            }
        }

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.ORGANISATION_MAIN_LOCATION)) {

                    Reference locationReference = (Reference)extension.getValue();

                    Location location = (Location)findResource(locationReference, params);
                    if (location == null) {
                        //The Emis data contains organisations that refer to organisations that don't exist
                        LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " refers to " + locationReference.getReference() + " that doesn't exist");
                        continue;
                    }

                    if (location != null) {
                        if (StringUtils.isNumeric(location.getId())) {
                            mainLocationId = Long.parseLong((location.getId()));
                        }
                    }
                }
            }
        }

        Organisation model = (Organisation)csvWriter;
        model.writeUpsert(id,
            serviceId,
            systemId,
            odsCode,
            name,
            isActive,
            parentOrganisationId,
            typeCode,
            mainLocationId
            );
    }

}
