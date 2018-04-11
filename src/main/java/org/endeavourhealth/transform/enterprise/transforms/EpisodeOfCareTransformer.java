package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class EpisodeOfCareTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractEnterpriseCsvWriter csvWriter,
                          EnterpriseTransformParams params) throws Exception {

        EpisodeOfCare fhirEpisode = (EpisodeOfCare)resource;

        long id;
        long organisationId;
        long patientId;
        long personId;
        Integer registrationTypeId = null;
        Date dateRegistered = null;
        Date dateRegisteredEnd = null;
        Long usualGpPractitionerId = null;
        //Long managingOrganisationId = null;

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhirEpisode.hasCareManager()) {
            Reference practitionerReference = fhirEpisode.getCareManager();
            usualGpPractitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        //registration type has moved to the EpisodeOfCare resource, although there will be some old instances (for now)
        //where the extension is on the Patient resource
        Extension extension = ExtensionConverter.findExtension(fhirEpisode, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
        if (extension == null) {
            //if not on the episode, check the patientPatient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
            Patient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
            if (fhirPatient != null) { //if a patient has been subsequently deleted, this will be null)
                extension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
            }
        }

        if (extension != null) {
            Coding coding = (Coding)extension.getValue();
            RegistrationType fhirRegistrationType = RegistrationType.fromCode(coding.getCode());
            registrationTypeId = new Integer(fhirRegistrationType.ordinal());
        }

        /*if (fhirEpisode.hasManagingOrganization()) {
            Reference orgReference = fhirEpisode.getManagingOrganization();
            managingOrganisationId = findEnterpriseId(data.getOrganisations(), orgReference);
            if (managingOrganisationId == null) {
                managingOrganisationId = transformOnDemand(orgReference, data, otherResources, enterpriseOrganisationId, enterprisePatientId, enterprisePersonId, configName);
            }
        }*/

        Period period = fhirEpisode.getPeriod();
        if (period.hasStart()) {
            dateRegistered = period.getStart();
        }
        if (period.hasEnd()) {
            dateRegisteredEnd = period.getEnd();
        }

        org.endeavourhealth.transform.enterprise.outputModels.EpisodeOfCare model = (org.endeavourhealth.transform.enterprise.outputModels.EpisodeOfCare)csvWriter;
        model.writeUpsert(id,
            organisationId,
            patientId,
            personId,
            registrationTypeId,
            dateRegistered,
            dateRegisteredEnd,
            usualGpPractitionerId);
    }
}

