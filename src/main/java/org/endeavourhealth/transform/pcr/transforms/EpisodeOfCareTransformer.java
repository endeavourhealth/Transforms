package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class EpisodeOfCareTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                                     Resource resource,
                                     AbstractPcrCsvWriter csvWriter,
                                     PcrTransformParams params) throws Exception {

        EpisodeOfCare fhirEpisode = (EpisodeOfCare) resource;

        long id;
        long organisationId;
        long patientId;
        Integer registrationTypeId = null;
        Integer registrationStatusId = 0;
        Integer gpRegistrationStatusSubConceptId=null;
        Integer effectiveDatePrecision = null;
        Long effectivePractitionerId = null;
        Long enteredByPractitionerId = null;
        Date dateRegistered = null;
        Date dateRegisteredEnd = null;
        Long specialityConceptId = null;
        Long adminConceptId = null;
        Long reasonConceptId = null;
        String encounterLinkId = null; //Should this be a Long?
        Long locationId=null;
        Long referralRequestId=null;
        Boolean isConsent=false;
        Boolean isCurrent=false;
        Long latestCareEpisodeStatusId=null;

        id = pcrId.longValue();
        organisationId = params.getPcrOrganisationId().longValue();
        patientId = params.getPcrPatientId().longValue();

        Extension regTypeExtension = ExtensionConverter.findExtension(fhirEpisode, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
        if (regTypeExtension == null) {
            //if not on the episode, check the patientPatient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
            Patient fhirPatient = (Patient) findResource(fhirEpisode.getPatient(), params);
            if (fhirPatient != null) { //if a patient has been subsequently deleted, this will be null)
                regTypeExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
            }
        }

        if (regTypeExtension != null) {
            Coding coding = (Coding) regTypeExtension.getValue();
            RegistrationType fhirRegistrationType = RegistrationType.fromCode(coding.getCode());
            registrationTypeId = new Integer(fhirRegistrationType.ordinal());
        }

        //TODO More detail? We only care about active for phase 1
        if (fhirEpisode.getStatus() != null) {
            if (fhirEpisode.getStatus().equals(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE)) {
                registrationStatusId = 2;
                isCurrent=true;
            }
        }

        Period period = fhirEpisode.getPeriod();
        if (period.hasStart()) {
            dateRegistered = period.getStart();
        }
        if (period.hasEnd()) {
            dateRegisteredEnd = period.getEnd();
        } else {
            isCurrent=true;
        }


        OutputContainer data = params.getOutputContainer();
        org.endeavourhealth.transform.pcr.outputModels.GpRegistrationStatus model = data.getGpRegistration();
        model.writeUpsert(id,
                patientId,
                organisationId,
                dateRegistered,
                effectiveDatePrecision,
                effectivePractitionerId,
                enteredByPractitionerId,
                dateRegisteredEnd,
                registrationTypeId,
                registrationStatusId,
                gpRegistrationStatusSubConceptId,
                isCurrent
        );

//        org.endeavourhealth.transform.pcr.outputModels.CareEpisode model = (org.endeavourhealth.transform.pcr.outputModels.CareEpisode) csvWriter;
//        model.writeUpsert(id,
//                patientId,
//                organisationId,
//                dateRegistered,
//                effectiveDatePrecision,
//                effectivePractitionerId,
//                enteredByPractitionerId,
//                dateRegisteredEnd,
//                encounterLinkId,
//                registrationStatusId,
//                specialityConceptId,
//                adminConceptId,
//                reasonConceptId,
//                registrationTypeId,
//                locationId,
//                referralRequestId,
//                isConsent,
//                latestCareEpisodeStatusId
//        );
    }
}

