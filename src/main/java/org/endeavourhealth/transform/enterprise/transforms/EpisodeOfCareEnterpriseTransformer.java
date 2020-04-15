package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
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
                        registrationStatusId = new Integer(status.ordinal());
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

        //TODO: remove this check for go live to introduce Compass v1 upgrade tables population
        if (!TransformConfig.instance().isLive()) {

            transformRegistrationStatusHistory(organisationId, patientId, personId, id, resourceWrapper, params);
        }

    }

    private static List<ResourceWrapper> getFullHistory(ResourceWrapper resourceWrapper) throws Exception {
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        UUID serviceId = resourceWrapper.getServiceId();
        String resourceType = resourceWrapper.getResourceType();
        UUID resourceId = resourceWrapper.getResourceId();
        return resourceDal.getResourceHistory(serviceId, resourceType, resourceId);
    }

    private static void transformRegistrationStatusHistory(Long organisationId, Long patientId, Long personId,
                                                           Long episodeOfCareId, ResourceWrapper resourceWrapper,
                                                           EnterpriseTransformHelper params) throws  Exception {

        Extension regStatusExtension = null;

        EpisodeOfCare episodeOfCare = (EpisodeOfCare)resourceWrapper.getResource();
        //all the registration status Id values are in this extension
        regStatusExtension
                = ExtensionConverter.findExtension(episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_STATUS);
        if (regStatusExtension != null) {
            Reference idReference = (Reference)regStatusExtension.getValue();
            String idReferenceValue = idReference.getReference();
            idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char

            for (Resource containedResource: episodeOfCare.getContained()) {
                if (containedResource != null && containedResource.getId().equals(idReferenceValue)) {
                    List_ list = (List_)containedResource;

                    Map<Integer, Date> hmRegStatusStartDate = new HashMap<>();
                    Map<Integer, Integer> hmRegStatusNext = new HashMap<>();
                    Integer prevStatusId = null;
                    Integer registrationStatusId = null;

                    //get all the status entries in the contained list to build up reference maps
                    List<List_.ListEntryComponent> entries = list.getEntry();
                    for (List_.ListEntryComponent entry : entries) {

                        //each status entry will be dated with a flag with coding
                        if (entry.hasFlag() && entry.hasDate()) {

                            CodeableConcept codeableConcept = entry.getFlag();
                            String code
                                    = CodeableConceptHelper.findCodingCode(codeableConcept, FhirValueSetUri.VALUE_SET_REGISTRATION_STATUS);
                            RegistrationStatus status = RegistrationStatus.fromCode(code);
                            registrationStatusId = new Integer(status.ordinal());

                            //setup the start date link for each status
                            Date startDate = entry.getDate();
                            hmRegStatusStartDate.put(registrationStatusId, startDate);

                            //set the subsequent status link for each status
                            if (prevStatusId != null) {
                                hmRegStatusNext.put(prevStatusId, registrationStatusId);
                            }
                            prevStatusId = registrationStatusId;
                        }
                    }

                    //loop through registration status hash map to derive start and end date and file them
                    for (Integer regStatusId: hmRegStatusStartDate.keySet()) {

                        Date startDate = hmRegStatusStartDate.get(regStatusId);

                        //get the subsequent status to derive the end date of this one.
                        //if there is no subsequent status then this is the most recent status and has no end date
                        Date endDate = null;
                        Integer regStatusIdNext = hmRegStatusNext.get(regStatusId);
                        if (regStatusIdNext != null) {
                            endDate = hmRegStatusStartDate.get(regStatusIdNext);
                        }

                        //set end date to null for last status entry in list.
                        //this will be the last registrationStatusId set in previous iteration through the status history
                        if (regStatusId == registrationStatusId) {
                            endDate = null;
                        }

                        //create a unique Id mapping reference for this episode of care registration status using
                        //code and date. Sometimes duplicates are sent which we will simply overwrite/upsert
                        String sourceId
                                = ReferenceHelper.createReferenceExternal(episodeOfCare).getReference()+":"+regStatusId+":"+startDate;
                        SubscriberId subTableId
                                = findOrCreateSubscriberId(params, SubscriberTableId.REGISTRATION_STATUS_HISTORY, sourceId);
                        params.setSubscriberIdTransformed(resourceWrapper, subTableId);
                        Long registrationHistoryId = subTableId.getSubscriberId();

                        org.endeavourhealth.transform.enterprise.outputModels.RegistrationStatusHistory history =
                                params.getOutputContainer().getRegistrationStatusHistory();
                        history.writeUpsert(registrationHistoryId,
                                organisationId,
                                patientId,
                                personId,
                                episodeOfCareId,
                                regStatusId,
                                startDate,
                                endDate);
                    }
                    //break out here as registration status containedResource entries are done
                    break;
                }
            }
        }
    }
}

