package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class EpisodeOfCareTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.EpisodeOfCare;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractEnterpriseCsvWriter csvWriter,
                          EnterpriseTransformHelper params) throws Exception {

        EpisodeOfCare fhir = (EpisodeOfCare)resource;

        if (isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            super.transformResourceDelete(enterpriseId, csvWriter, params);
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
        //TODO wwe had problems with organisationId and Adastra. Investigate later. See also PatientTransformer
//        if (fhirEpisode.getManagingOrganization().isEmpty()) {
//            organisationId = params.getEnterpriseOrganisationId().longValue();
//        } else {
//            organisationId = transformOnDemandAndMapId(fhirEpisode.getManagingOrganization(), params);
//        }
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
            ResourceWrapper wrapper = findResource(fhir.getPatient(), params);
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
    }
}

