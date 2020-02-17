package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.ehr.models.Encounter;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingEmergencyCdsTarget;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CompositionBuilder;
import org.hl7.fhir.instance.model.Composition;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class EmergencyCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EmergencyCdsTargetTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createEmergencyCdsEncounters(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createEmergencyCdsEncounters(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target emergency cds records for the current exchangeId
        List<StagingEmergencyCdsTarget> targetEmergencyCdsRecords = csvHelper.retrieveTargetEmergencyCds();
        if (targetEmergencyCdsRecords == null) {
            return;
        }

        //TransformWarnings.log(LOG, csvHelper, "Target EmergencyCds records to transform to FHIR: {} for exchangeId: {}", targetEmergencyCds.size(), csvHelper.getExchangeId());

        for (StagingEmergencyCdsTarget targetEmergencyCds : targetEmergencyCdsRecords) {

            String uniqueId = targetEmergencyCds.getUniqueId();
            boolean isDeleted = targetEmergencyCds.isDeleted();

            if (isDeleted) {

                // retrieve the existing Composition resource to perform the deletion on
                Composition existingComposition
                        = (Composition) csvHelper.retrieveResourceForLocalId(ResourceType.Composition, uniqueId);

                if (existingComposition != null) {
                    CompositionBuilder compositionBuilder = new CompositionBuilder(existingComposition, targetEmergencyCds.getAudit());

                    //remember to pass in false since this existing composition is already ID mapped
                    fhirResourceFiler.deletePatientResource(null, false, compositionBuilder);
                } else {
                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Composition: {} for deletion", uniqueId);
                }

                continue;
            }

            // create the FHIR Composition resource
            CompositionBuilder compositionBuilder
                    = new CompositionBuilder(null, targetEmergencyCds.getAudit());
            compositionBuilder.setId(uniqueId);

            Integer personId = targetEmergencyCds.getPersonId();
            String patientIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Patient, Integer.toString(personId));
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            compositionBuilder.setPatientSubject(patientReference);
            compositionBuilder.setTitle("Encounter Composition");
            compositionBuilder.setStatus(Composition.CompositionStatus.FINAL);
            Identifier identifier = new Identifier();
            identifier.setValue(uniqueId);
            compositionBuilder.setIdentifier(identifier);

            // set top level encounter which encapsulates the other sub-encounters (up to 5 in composition sections)
            Encounter encounterEmergencyParent = new Encounter();
            encounterEmergencyParent.setEncounterType("emergency");

            //the attendanceId associated with the emergency encounter(s), used for top level parent encounter
            String attendanceId = targetEmergencyCds.getAttendanceId();
            String topLevelEncounterId = attendanceId + ":00:ED";
            String topLevelEncounterIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, topLevelEncounterId);
            encounterEmergencyParent.setEncounterId(topLevelEncounterIdStr);
            encounterEmergencyParent.setPatientId(patientIdStr);
            encounterEmergencyParent.setEffectiveDate(targetEmergencyCds.getDtArrival());
            encounterEmergencyParent.setEffectiveEndDate(targetEmergencyCds.getDtDeparture());

            String episodeIdStr = null;
            if (targetEmergencyCds.getEpisodeId() != null) {
                episodeIdStr =
                        IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, targetEmergencyCds.getEpisodeId().toString());
            }
            encounterEmergencyParent.setEpisodeOfCareId(episodeIdStr);

            //the top level encounterId which links to ENCTR and other associated records
            Integer parentEncounterId = targetEmergencyCds.getEncounterId();
            String parentEncounterIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, Integer.toString(parentEncounterId));
            encounterEmergencyParent.setParentEncounterId(parentEncounterIdStr);

            String performerIdStr = null;
            if (targetEmergencyCds.getPerformerPersonnelId() != null) {
                performerIdStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Practitioner, targetEmergencyCds.getPerformerPersonnelId().toString());
            }
            encounterEmergencyParent.setPractitionerId(performerIdStr);

            String serviceProviderOrgStr = null;
            if (!Strings.isNullOrEmpty(targetEmergencyCds.getOrganisationCode())) {
                serviceProviderOrgStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Organization, targetEmergencyCds.getOrganisationCode());
            }
            encounterEmergencyParent.setServiceProviderOrganisationId(serviceProviderOrgStr);
            encounterEmergencyParent.setAdditionalFieldsJson(null);
            String encounterInstanceAsJson = null;
            encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterEmergencyParent);
            compositionBuilder.addSection("encounter-1", encounterEmergencyParent.getEncounterId(), encounterInstanceAsJson);


            // sub encounter: the A&E attendance  (sequence #1)
            Encounter encounterArrival = new Encounter();
            encounterArrival.setEncounterType("emergency arrival");
            String attendanceIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, attendanceId+":01:ED");
            encounterArrival.setEncounterId(attendanceIdStr);
            encounterArrival.setPatientId(patientIdStr);
            encounterArrival.setEffectiveDate(targetEmergencyCds.getDtArrival());
            encounterArrival.setEffectiveEndDate(null);
            encounterArrival.setEpisodeOfCareId(episodeIdStr);
            encounterArrival.setParentEncounterId(topLevelEncounterIdStr);
            encounterArrival.setPractitionerId(performerIdStr);
            encounterArrival.setServiceProviderOrganisationId(serviceProviderOrgStr);

            // create a list of additional data to store as Json for this encounterArrival instance
            JsonObject additionalArrivalObjs = new JsonObject();
            additionalArrivalObjs.addProperty("department_type", targetEmergencyCds.getDepartmentType());
            additionalArrivalObjs.addProperty("ambulance_no", targetEmergencyCds.getAmbulanceNo());
            additionalArrivalObjs.addProperty("arrival_mode", targetEmergencyCds.getArrivalMode());
            additionalArrivalObjs.addProperty("chief_complaint", targetEmergencyCds.getChiefComplaint());
            encounterArrival.setAdditionalFieldsJson(additionalArrivalObjs.toString());

            encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterArrival);
            compositionBuilder.addSection("encounter-1-01", encounterArrival.getEncounterId(), encounterInstanceAsJson);

            // sub encounter: the initial assessment  (sequence #2)
            Date initialAssessmentDate = targetEmergencyCds.getDtInitialAssessment();
            if (initialAssessmentDate != null) {

                Encounter encounterAssessment = new Encounter();
                encounterAssessment.setEncounterType("emergency initial assessment");
                attendanceIdStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, attendanceId+":02:ED");
                encounterAssessment.setEncounterId(attendanceIdStr);
                encounterAssessment.setPatientId(patientIdStr);
                encounterAssessment.setEffectiveDate(initialAssessmentDate);
                encounterAssessment.setEffectiveEndDate(null);
                encounterAssessment.setEpisodeOfCareId(episodeIdStr);
                encounterAssessment.setParentEncounterId(topLevelEncounterIdStr);
                encounterAssessment.setPractitionerId(performerIdStr);
                encounterAssessment.setServiceProviderOrganisationId(serviceProviderOrgStr);

                //create a list of additional data to store as Json for this encounterAssessment instance
                JsonObject additionalAssessmentObjs = new JsonObject();
                if (!Strings.isNullOrEmpty(targetEmergencyCds.getSafeguardingConcerns())){
                    additionalAssessmentObjs.addProperty("safeguarding_concerns", targetEmergencyCds.getSafeguardingConcerns());
                }
                encounterAssessment.setAdditionalFieldsJson(additionalAssessmentObjs.toString());

                encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterAssessment);
                compositionBuilder.addSection("encounter-1-02", encounterAssessment.getEncounterId(), encounterInstanceAsJson);
            }

            // sub encounter: the diagnosis, investigation and treatments  (sequence #3)
            Date invTreatDiagDate = targetEmergencyCds.getDtSeenForTreatment();
            if (invTreatDiagDate !=null) {

                Encounter encounterInvTreat = new Encounter();
                encounterInvTreat.setEncounterType("emergency investigations and treatments");
                attendanceIdStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, attendanceId + ":03:ED");
                encounterInvTreat.setEncounterId(attendanceIdStr);
                encounterInvTreat.setPatientId(patientIdStr);
                encounterInvTreat.setEffectiveDate(targetEmergencyCds.getDtSeenForTreatment());
                encounterInvTreat.setEffectiveEndDate(null);
                encounterInvTreat.setEpisodeOfCareId(episodeIdStr);
                encounterInvTreat.setParentEncounterId(topLevelEncounterIdStr);
                encounterInvTreat.setPractitionerId(performerIdStr);
                encounterInvTreat.setServiceProviderOrganisationId(serviceProviderOrgStr);

                // create a list of additional data to store as Json for this encounterInvTreat instance
                JsonObject additionalInvTreatObjs = new JsonObject();
                additionalInvTreatObjs.addProperty("diagnosis", targetEmergencyCds.getDiagnosis());
                additionalInvTreatObjs.addProperty("investigations", targetEmergencyCds.getInvestigations());
                additionalInvTreatObjs.addProperty("treatments", targetEmergencyCds.getTreatments());
                additionalInvTreatObjs.addProperty("referred_to_services", targetEmergencyCds.getReferredToServices());
                encounterInvTreat.setAdditionalFieldsJson(additionalInvTreatObjs.toString());

                encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterInvTreat);
                compositionBuilder.addSection("encounter-1-03", encounterInvTreat.getEncounterId(), encounterInstanceAsJson);
            }

            // sub encounter: the inpatient admission  (sequence #4) - this ultimately links up with the
            // inpatient_cds record with the same encounter_id
            Date admissionDate = targetEmergencyCds.getDtDecidedToAdmit();
            if (admissionDate != null) {

                Encounter encounterAdmission = new Encounter();
                encounterAdmission.setEncounterType("inpatient admission");
                attendanceIdStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, attendanceId+":04:ED");
                encounterAdmission.setEncounterId(attendanceIdStr);
                encounterAdmission.setPatientId(patientIdStr);
                encounterAdmission.setEffectiveDate(admissionDate);
                encounterAdmission.setEffectiveEndDate(null);
                encounterAdmission.setEpisodeOfCareId(episodeIdStr);
                encounterAdmission.setParentEncounterId(topLevelEncounterIdStr);
                encounterAdmission.setPractitionerId(performerIdStr);
                encounterAdmission.setServiceProviderOrganisationId(serviceProviderOrgStr);

                // no additional data to store as Json for this encounterAdmission instance
                encounterAdmission.setAdditionalFieldsJson(null);

                encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterAdmission);
                compositionBuilder.addSection("encounter-1-04", encounterAdmission.getEncounterId(), encounterInstanceAsJson);
            }

            // sub encounter: the discharge/departure from emergency  (sequence #5)
            Date departureDate = targetEmergencyCds.getDtDeparture();
            if (departureDate != null) {

                Encounter encounterDischarge = new Encounter();
                encounterDischarge.setEncounterType("emergency discharge");
                attendanceIdStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, attendanceId+":05:ED");
                encounterDischarge.setEncounterId(attendanceIdStr);
                encounterDischarge.setPatientId(patientIdStr);
                encounterDischarge.setEffectiveDate(departureDate);
                encounterDischarge.setEffectiveEndDate(null);
                encounterDischarge.setEpisodeOfCareId(episodeIdStr);
                encounterDischarge.setParentEncounterId(topLevelEncounterIdStr);
                encounterDischarge.setPractitionerId(performerIdStr);
                encounterDischarge.setServiceProviderOrganisationId(serviceProviderOrgStr);

                // additional data to store as Json for this encounterAdmission instance
                JsonObject additionalDischargeObjs = new JsonObject();
                additionalDischargeObjs.addProperty("discharge_status", targetEmergencyCds.getDischargeStatus());
                additionalDischargeObjs.addProperty("discharge_destination", targetEmergencyCds.getDischargeDestination());
                encounterDischarge.setAdditionalFieldsJson(additionalDischargeObjs.toString());

                encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterDischarge);
                compositionBuilder.addSection("encounter-1-05", encounterDischarge.getEncounterId(), encounterInstanceAsJson);
            }

            //LOG.debug("Saving CompositionId: "+uniqueId+", with resourceData: "+ FhirSerializationHelper.serializeResource(compositionBuilder.getResource()));

            //save composition record
            fhirResourceFiler.savePatientResource(null, compositionBuilder);

            //LOG.debug("Transforming compositionId: "+uniqueId+"  Filed");
        }
    }
}