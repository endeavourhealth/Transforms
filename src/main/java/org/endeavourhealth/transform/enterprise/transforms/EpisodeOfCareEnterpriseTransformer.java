package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.subscriber.transforms.EpisodeOfCareTransformer;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EpisodeOfCareEnterpriseTransformer extends AbstractEnterpriseTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareEnterpriseTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.EpisodeOfCare;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {

        EpisodeOfCare fhir = (EpisodeOfCare)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {

            List<ResourceWrapper> fullHistory = EnterpriseTransformHelper.getFullHistory(resourceWrapper);
            deleteRegistrationStatusHistory(fullHistory, params);
            csvWriter.writeDelete(enterpriseId.longValue());
            return;
        }

        long id;
        long organisationId;
        long patientId;
        long personId;
        Integer registrationTypeId = null;
        Integer registrationStatusId = null;
        Date dateRegistered = null;
        Date dateRegisteredEnd = null;
        Long usualGpPractitionerId = null;
        //Long managingOrganisationId = null;

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();

        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasCareManager()) {
            Reference practitionerReference = fhir.getCareManager();
            usualGpPractitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        //registration type has moved to the EpisodeOfCare resource, although there will be some old instances (for now)
        //where the extension is on the Patient resource
        Extension regTypeExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
        if (regTypeExtension == null) {
            //if not on the episode, check the patientPatient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
            ResourceWrapper wrapper = params.findOrRetrieveResource(fhir.getPatient());
            if (wrapper != null) { //if a patient has been subsequently deleted, this will be null)
                Patient fhirPatient = (Patient) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
                regTypeExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
            }
        }

        if (regTypeExtension != null) {
            Coding coding = (Coding)regTypeExtension.getValue();
            RegistrationType fhirRegistrationType = RegistrationType.fromCode(coding.getCode());
            registrationTypeId = new Integer(fhirRegistrationType.ordinal());
        }

        //reg status is stored in a contained list with an extension giving the internal reference to it
        List<EpisodeOfCareTransformer.RegStatus> regStatuses = EpisodeOfCareTransformer.getRegStatusList(fhir);
        if (!regStatuses.isEmpty()) {
            EpisodeOfCareTransformer.RegStatus latest = regStatuses.get(regStatuses.size()-1);
            RegistrationStatus status = latest.getStatus();
            registrationStatusId = new Integer(status.ordinal());
        }

        Period period = fhir.getPeriod();
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
            registrationStatusId,
            dateRegistered,
            dateRegisteredEnd,
            usualGpPractitionerId);

        transformRegistrationStatusHistory(organisationId, patientId, personId, id, fhir, params);
    }

    private void deleteRegistrationStatusHistory(List<ResourceWrapper> fullHistory, EnterpriseTransformHelper params) throws Exception {

        org.endeavourhealth.transform.enterprise.outputModels.RegistrationStatusHistory writer = params.getOutputContainer().getRegistrationStatusHistory();
        Set<EpisodeOfCareTransformer.RegStatus> regStatuses = EpisodeOfCareTransformer.getAllRegStatuses(fullHistory);

        //generate IDs
        Map<EpisodeOfCareTransformer.RegStatus, SubscriberId> hmIds = EpisodeOfCareTransformer.findRegStatusIds(regStatuses, params.getEnterpriseConfigName(), false);

        //transform each one
        for (EpisodeOfCareTransformer.RegStatus regStatus: regStatuses) {

            //create a unique Id mapping reference for this episode of care registration status using
            //code and date. Sometimes duplicates are sent which we will simply overwrite/upsert
            SubscriberId subTableId = hmIds.get(regStatus);
            if (subTableId != null) { //will be null if never transformed before

                long registrationHistoryId = subTableId.getSubscriberId();
                writer.writeDelete(registrationHistoryId);
            }
        }
    }


    private static void transformRegistrationStatusHistory(long organisationId, long patientId, long personId,
                                                           long episodeOfCareId, EpisodeOfCare episodeOfCare,
                                                           EnterpriseTransformHelper params) throws  Exception {

        org.endeavourhealth.transform.enterprise.outputModels.RegistrationStatusHistory writer = params.getOutputContainer().getRegistrationStatusHistory();
        List<EpisodeOfCareTransformer.RegStatus> regStatuses = EpisodeOfCareTransformer.getRegStatusList(episodeOfCare);

        //generate IDs
        Map<EpisodeOfCareTransformer.RegStatus, SubscriberId> hmIds = EpisodeOfCareTransformer.findRegStatusIds(regStatuses, params.getEnterpriseConfigName(), true);

        //transform each one
        for (int i=0; i<regStatuses.size(); i++) {
            EpisodeOfCareTransformer.RegStatus regStatus = regStatuses.get(i);

            Date startDate = regStatus.getStart();

            RegistrationStatus status = regStatus.getStatus();
            Integer regStatusId = new Integer(status.ordinal());

            //get the subsequent status to derive the end date of this one.
            //if there is no subsequent status then this is the most recent status and has no end date
            Date endDate = null;
            if (i+1 < regStatuses.size()) {
                EpisodeOfCareTransformer.RegStatus nextRegStatus = regStatuses.get(i+1);
                endDate = nextRegStatus.getStart();
            }

            //create a unique Id mapping reference for this episode of care registration status using
            //code and date. Sometimes duplicates are sent which we will simply overwrite/upsert
            SubscriberId subTableId = hmIds.get(regStatus);
            long registrationHistoryId = subTableId.getSubscriberId();

            writer.writeUpsert(registrationHistoryId,
                    organisationId,
                    patientId,
                    personId,
                    episodeOfCareId,
                    regStatusId,
                    startDate,
                    endDate);
        }
    }


}

