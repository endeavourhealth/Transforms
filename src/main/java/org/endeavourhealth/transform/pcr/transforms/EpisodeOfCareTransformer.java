package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EpisodeOfCareTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        EpisodeOfCare fhirEpisode = (EpisodeOfCare)resource;

//        long id;
//        long organisationId;
//        long patientId;
//        long personId;
//        Integer registrationTypeId = null;
//        Integer registrationStatusId = null;
//        Date dateRegistered = null;
//        Date dateRegisteredEnd = null;
//        Long usualGpPractitionerId = null;
//        //Long managingOrganisationId = null;
//
//        id = enterpriseId.longValue();
//        organisationId = params.getEnterpriseOrganisationId().longValue();
//        patientId = params.getEnterprisePatientId().longValue();
//        personId = params.getEnterprisePersonId().longValue();
//
//        if (fhirEpisode.hasCareManager()) {
//            Reference practitionerReference = fhirEpisode.getCareManager();
//            usualGpPractitionerId = transformOnDemandAndMapId(practitionerReference, params);
//        }
//
//        //registration type has moved to the EpisodeOfCare resource, although there will be some old instances (for now)
//        //where the extension is on the Patient resource
//        Extension regTypeExtension = ExtensionConverter.findExtension(fhirEpisode, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
//        if (regTypeExtension == null) {
//            //if not on the episode, check the patientPatient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
//            Patient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
//            if (fhirPatient != null) { //if a patient has been subsequently deleted, this will be null)
//                regTypeExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
//            }
//        }
//
//        if (regTypeExtension != null) {
//            Coding coding = (Coding)regTypeExtension.getValue();
//            RegistrationType fhirRegistrationType = RegistrationType.fromCode(coding.getCode());
//            registrationTypeId = new Integer(fhirRegistrationType.ordinal());
//        }
//
//        //reg status is stored in a contained list with an extension giving the internal reference to it
//        Extension regStatusExtension = ExtensionConverter.findExtension(fhirEpisode, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_STATUS);
//        if (regStatusExtension != null) {
//            Reference idReference = (Reference)regStatusExtension.getValue();
//            String idReferenceValue = idReference.getReference();
//            idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char
//
//            for (Resource containedResource: fhirEpisode.getContained()) {
//                if (containedResource.getId().equals(idReferenceValue)) {
//                    List_ list = (List_)containedResource;
//
//                    //status is on the most recent entry
//                    List<List_.ListEntryComponent> entries = list.getEntry();
//                    List_.ListEntryComponent entry = entries.get(entries.size()-1);
//                    if (entry.hasFlag()) {
//                        CodeableConcept codeableConcept = entry.getFlag();
//                        String code = CodeableConceptHelper.findCodingCode(codeableConcept, FhirValueSetUri.VALUE_SET_REGISTRATION_STATUS);
//                        RegistrationStatus status = RegistrationStatus.fromCode(code);
//                        registrationStatusId = new Integer(status.ordinal());
//                    }
//
//                    break;
//                }
//            }
//        }
//
//        /*if (fhirEpisode.hasManagingOrganization()) {
//            Reference orgReference = fhirEpisode.getManagingOrganization();
//            managingOrganisationId = findEnterpriseId(data.getOrganisations(), orgReference);
//            if (managingOrganisationId == null) {
//                managingOrganisationId = transformOnDemand(orgReference, data, otherResources, enterpriseOrganisationId, enterprisePatientId, enterprisePersonId, configName);
//            }
//        }*/
//
//        Period period = fhirEpisode.getPeriod();
//        if (period.hasStart()) {
//            dateRegistered = period.getStart();
//        }
//        if (period.hasEnd()) {
//            dateRegisteredEnd = period.getEnd();
//        }
//
//        org.endeavourhealth.transform.pcr.outputModels.EpisodeOfCare model = (org.endeavourhealth.transform.pcr.outputModels.EpisodeOfCare)csvWriter;
//        model.writeUpsert(id,
//            organisationId,
//            patientId,
//            personId,
//            registrationTypeId,
//            registrationStatusId,
//            dateRegistered,
//            dateRegisteredEnd,
//            usualGpPractitionerId);
    }
}

