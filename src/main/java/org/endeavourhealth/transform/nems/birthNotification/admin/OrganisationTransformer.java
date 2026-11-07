package org.endeavourhealth.transform.nems.birthNotification.admin;

import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.nems.NemsXmlHelper;
import org.endeavourhealth.transform.nems.schema.v1.BundleType;
import org.endeavourhealth.transform.nems.schema.v1.EntryType;
import org.endeavourhealth.transform.nems.schema.v1.OrganizationType;
import org.endeavourhealth.transform.nems.schema.v1.PatientType;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.endeavourhealth.transform.nems.NemsXmlHelper.*;
import static org.endeavourhealth.common.ods.OdsWebService.lookupOrganisationViaRest;

import static org.endeavourhealth.transform.nems.DataCache.*;


public class OrganisationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OrganisationTransformer.class);

    public static void transform(BundleType bundle, FhirResourceFiler fhirResourceFiler, NemsXmlHelper xmlHelper) throws Exception {

        for (EntryType entry : bundle.getEntry()) {
            OrganizationType entryOrg = entry.getResource().getOrganization();
            if (null != entryOrg) {
                String odsCode = entryOrg.getIdentifier().getValue().getValue();
                String id = entryOrg.getId().getValue();
                OrganizationBuilder builder = new OrganizationBuilder();
                OdsOrganisation org = lookupOrganisationViaRest(odsCode);
                if (null != org) {

                    builder.setName(org.getOrganisationName());
                    builder.setOdsCode(org.getOdsCode());
                    builder.setId(xmlHelper.createUniqueId(odsCode, ResourceType.Organization));

                } else {
                    NemsXmlHelper.validateBatch(false);
                    TransformWarnings.log(LOG, xmlHelper, "Couldn't find Organization with ODS code: " + odsCode);
                    //throw new TransformException("Couldn't find Organization with ODS code: " + odsCode);
                }

                organisationMap.put(id, builder.getResourceId());
                fhirResourceFiler.saveAdminResource(null, builder);
            } else {
                PatientType patient = entry.getResource().getPatient();
                if(patient!= null) {
                    if (patient.getGeneralPractitioner() != null) {
                        String odsCode = patient.getGeneralPractitioner().getReference().getValue().substring(57);
                        OrganizationBuilder builder = new OrganizationBuilder();
                        OdsOrganisation org = lookupOrganisationViaRest(odsCode);
                        if (null != org) {

                            builder.setName(org.getOrganisationName());
                            builder.setOdsCode(org.getOdsCode());
                            builder.setId(xmlHelper.createUniqueId(odsCode, ResourceType.Organization));

                        } else {
                            NemsXmlHelper.validateBatch(false);
                            TransformWarnings.log(LOG, xmlHelper, "Couldn't find Organization with ODS code: " + odsCode);
                            //throw new TransformException("Couldn't find Organization with ODS code: " + odsCode);
                        }
                        fhirResourceFiler.saveAdminResource(null, builder);
                    }
                }
            }
        }
    }
}
