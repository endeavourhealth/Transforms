package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class EpisodeOfCareTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.EpisodeOfCare model = params.getOutputContainer().getEpisodesOfCare();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);

            return;
        }

        EpisodeOfCare fhirEpisode = (EpisodeOfCare) FhirResourceHelper.deserialiseResouce(resourceWrapper);

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
        organizationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhirEpisode.hasCareManager()) {
            Reference practitionerReference = fhirEpisode.getCareManager();
            usualGpPractitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        //registration type has moved to the EpisodeOfCare resource, although there will be some old instances (for now)
        //where the extension is on the Patient resource
        Extension regTypeExtension = ExtensionConverter.findExtension(fhirEpisode, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
        if (regTypeExtension == null) {
            //if not on the episode, check the patientPatient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
            Patient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
            if (fhirPatient != null) { //if a patient has been subsequently deleted, this will be null)
                regTypeExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
            }
        }

        if (regTypeExtension != null) {
            Coding coding = (Coding)regTypeExtension.getValue();
            RegistrationType fhirRegistrationType = RegistrationType.fromCode(coding.getCode());

            registrationTypeConceptId = IMHelper.getIMConcept(params, fhirEpisode, IMConstant.FHIR_REGISTRATION_TYPE, fhirRegistrationType.getCode());
        }

        //reg status is stored in a contained list with an extension giving the internal reference to it
        Extension regStatusExtension = ExtensionConverter.findExtension(fhirEpisode, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_STATUS);
        if (regStatusExtension != null) {
            Reference idReference = (Reference)regStatusExtension.getValue();
            String idReferenceValue = idReference.getReference();
            idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char

            for (Resource containedResource: fhirEpisode.getContained()) {
                if (containedResource.getId().equals(idReferenceValue)) {
                    List_ list = (List_)containedResource;

                    //status is on the most recent entry
                    List<List_.ListEntryComponent> entries = list.getEntry();
                    List_.ListEntryComponent entry = entries.get(entries.size()-1);
                    if (entry.hasFlag()) {
                        CodeableConcept codeableConcept = entry.getFlag();
                        String code = CodeableConceptHelper.findCodingCode(codeableConcept, FhirValueSetUri.VALUE_SET_REGISTRATION_STATUS);
                        RegistrationStatus status = RegistrationStatus.fromCode(code);
                        registrationStatusConceptId = IMHelper.getIMConcept(params, fhirEpisode, IMConstant.FHIR_REGISTRATION_STATUS, status.getCode());

                        // Using test data, registrationStatusConceptId is null in 23/989 rows, logging to see why
                        if (registrationStatusConceptId == null) {
                            LOG.info("registrationStatusConceptId is null where status.getcode() value is: " + status.getCode());
                        }

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

        Period period = fhirEpisode.getPeriod();
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


    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.EPISODE_OF_CARE;
    }


}

