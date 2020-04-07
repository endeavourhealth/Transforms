package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.RegistrationStatusHistory;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class EpisodeOfCareTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.EpisodeOfCare;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.EpisodeOfCare model = params.getOutputContainer().getEpisodesOfCare();

        EpisodeOfCare fhir = (EpisodeOfCare)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        long id;
        long organizationId;
        long patientId;
        long personId;
        Integer registrationTypeConceptId = null;
        Integer registrationStatusConceptId = null;
        Date dateRegistered = null;
        Date dateRegisteredEnd = null;
        Long usualGpPractitionerId = null;
        //Long managingOrganisationId = null;

        id = subscriberId.getSubscriberId();
        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasCareManager()) {
            Reference practitionerReference = fhir.getCareManager();
            usualGpPractitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        //registration type has moved to the EpisodeOfCare resource, although there will be some old instances (for now)
        //where the extension is on the Patient resource
        Extension regTypeExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
        if (regTypeExtension == null) {
            //if not on the episode, check the patientPatient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
            Patient fhirPatient = (Patient)params.findOrRetrieveResource(fhir.getPatient());
            if (fhirPatient != null) { //if a patient has been subsequently deleted, this will be null)
                regTypeExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
            }
        }

        if (regTypeExtension != null) {
            Coding coding = (Coding)regTypeExtension.getValue();
            RegistrationType fhirRegistrationType = RegistrationType.fromCode(coding.getCode());

            registrationTypeConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_REGISTRATION_TYPE, fhirRegistrationType.getCode(), coding.getDisplay());
        }

        //reg status is stored in a contained list with an extension giving the internal reference to it
        Extension regStatusExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_STATUS);
        if (regStatusExtension != null) {
            Reference idReference = (Reference)regStatusExtension.getValue();
            String idReferenceValue = idReference.getReference();
            idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char

            for (Resource containedResource: fhir.getContained()) {
                if (containedResource.getId().equals(idReferenceValue)) {
                    List_ list = (List_)containedResource;

                    //status is on the most recent entry
                    List<List_.ListEntryComponent> entries = list.getEntry();
                    List_.ListEntryComponent entry = entries.get(entries.size()-1);
                    if (entry.hasFlag()) {
                        CodeableConcept codeableConcept = entry.getFlag();
                        String code = CodeableConceptHelper.findCodingCode(codeableConcept, FhirValueSetUri.VALUE_SET_REGISTRATION_STATUS);
                        RegistrationStatus status = RegistrationStatus.fromCode(code);
                        registrationStatusConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_REGISTRATION_STATUS,
                                status.getCode(), status.getDescription());
                    }

                    break;
                }
            }
        }

        /*if (fhirEpisode.hasManagingOrganization()) {
            Reference orgReference = fhirEpisode.getManagingOrganization();
            managingOrganisationId = findEnterpriseId(data.getOrganisations(), orgReference);
            if (managingOrganisationId == null) {
                managingOrganisationId = transformOnDemand(orgReference, data, otherResources, enterpriseOrganisationId, enterprisePatientId, enterprisePersonId, configName);
            }
        }*/

        Period period = fhir.getPeriod();
        if (period.hasStart()) {
            dateRegistered = period.getStart();
        }
        if (period.hasEnd()) {
            dateRegisteredEnd = period.getEnd();
        }

        model.writeUpsert(subscriberId,
                organizationId,
                patientId,
                personId,
                registrationTypeConceptId,
                registrationStatusConceptId,
                dateRegistered,
                dateRegisteredEnd,
                usualGpPractitionerId);

        List<ResourceWrapper> fullHistory = getFullHistory(resourceWrapper);
        transformRegistrationStatusHistory(organizationId, patientId, personId, id, fullHistory, params);
    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.EPISODE_OF_CARE;
    }

    private static List<ResourceWrapper> getFullHistory(ResourceWrapper resourceWrapper) throws Exception {
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        UUID serviceId = resourceWrapper.getServiceId();
        String resourceType = resourceWrapper.getResourceType();
        UUID resourceId = resourceWrapper.getResourceId();
        return resourceDal.getResourceHistory(serviceId, resourceType, resourceId);
    }

    private static void transformRegistrationStatusHistory(Long organisationId, Long patientId, Long personId,
                                                           Long episodeOfCareId, List<ResourceWrapper> fullHistory,
                                                           SubscriberTransformHelper params) throws Exception {

        Extension regStatusExtension = null;
        SubscriberId id = null;
        Period period = null;
        Date start = null;
        Date end = null;
        Integer registrationStatusConceptId = null;

        for (ResourceWrapper wrapper : fullHistory) {
            EpisodeOfCare episodeOfCare = (EpisodeOfCare) wrapper.getResource();
            id = AbstractSubscriberTransformer.findOrCreateSubscriberId(params, SubscriberTableId.REGISTRATION_STATUS_HISTORY, episodeOfCare.getId());

            regStatusExtension = ExtensionConverter.findExtension(episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_STATUS);
            if (regStatusExtension != null) {
                Reference idReference = (Reference)regStatusExtension.getValue();
                String idReferenceValue = idReference.getReference();
                idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char

                for (Resource containedResource: episodeOfCare.getContained()) {
                    if (containedResource.getId().equals(idReferenceValue)) {
                        List_ list = (List_)containedResource;

                        //status is on the most recent entry
                        List<List_.ListEntryComponent> entries = list.getEntry();
                        List_.ListEntryComponent entry = entries.get(entries.size()-1);
                        if (entry.hasFlag()) {
                            CodeableConcept codeableConcept = entry.getFlag();
                            String code = CodeableConceptHelper.findCodingCode(codeableConcept, FhirValueSetUri.VALUE_SET_REGISTRATION_STATUS);
                            RegistrationStatus status = RegistrationStatus.fromCode(code);
                            registrationStatusConceptId = IMHelper.getIMConcept(params, episodeOfCare, IMConstant.FHIR_REGISTRATION_STATUS,
                                    status.getCode(), status.getDescription());
                        }

                        break;
                    }
                }
            }

            period = episodeOfCare.getPeriod();
            if (period.hasStart()) {
                start = period.getStart();
            }
            if (period.hasEnd()) {
                end = period.getEnd();
            }

            /*
            RegistrationStatusHistory history = params.getOutputContainer().getRegistrationStatusHistory();
            history.writeUpsert(id,
                    organisationId,
                    patientId,
                    personId,
                    episodeOfCareId,
                    registrationStatusConceptId,
                    start,
                    end);
             */

        }
    }
}

