package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingEmergencyCdsTarget;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.models.Encounter;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CompositionBuilder;
import org.hl7.fhir.instance.model.Composition;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            compositionBuilder.setPatientSubject(patientReference);
            compositionBuilder.setTitle("Encounter Composition");
            compositionBuilder.setStatus(Composition.CompositionStatus.FINAL);
            Identifier identifier = new Identifier();
            identifier.setValue(uniqueId);
            compositionBuilder.setIdentifier(identifier);

            Integer parentEncounterId = targetEmergencyCds.getEncounterId();  //the top level encounterId
            String attendanceId = targetEmergencyCds.getAttendanceId();  //the attendanceId associated with each sub-encounter

            // set top level encounter which encapsulates the other sub-encounters (up to 5 in composition sections)
            Encounter encounterEmergencyParent = new Encounter();
            encounterEmergencyParent.setEncounterType("emergency");
            encounterEmergencyParent.setEncounterId(Integer.toString(parentEncounterId));
            encounterEmergencyParent.setEffectiveDate(targetEmergencyCds.getDtArrival());
            encounterEmergencyParent.setEffectiveEndDate(targetEmergencyCds.getDtDeparture());
            encounterEmergencyParent.setEpisodeOfCareId(targetEmergencyCds.getEpisodeId());
            encounterEmergencyParent.setParentEncounterId(null);
            encounterEmergencyParent.setPractitionerId(targetEmergencyCds.getPerformerPersonnelId());
            encounterEmergencyParent.setServiceProviderOrganisationId(targetEmergencyCds.getOrganisationCode());
            encounterEmergencyParent.setAdditionalFieldsJson(null);
            String encounterInstanceAsJson = null;
            encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterEmergencyParent);
            compositionBuilder.addSection("encounter-1", encounterInstanceAsJson);

            // sub encounter: the A&E attendance  (sequence #1)
            Encounter encounterArrival = new Encounter();
            encounterArrival.setEncounterType("a&e attendance");
            encounterArrival.setEncounterId(attendanceId+":1");
            encounterArrival.setEffectiveDate(targetEmergencyCds.getDtArrival());
            encounterArrival.setEffectiveEndDate(null);
            encounterArrival.setEpisodeOfCareId(targetEmergencyCds.getEpisodeId());
            encounterArrival.setParentEncounterId(Integer.toString(parentEncounterId));
            encounterArrival.setPractitionerId(targetEmergencyCds.getPerformerPersonnelId());
            encounterArrival.setServiceProviderOrganisationId(targetEmergencyCds.getOrganisationCode());

            // create a list of additional data to store as Json for this encounterArrival instance
            List <Object> additionalArrivalObjs = new ArrayList<>();
            additionalArrivalObjs.add(targetEmergencyCds.getDepartmentType());
            additionalArrivalObjs.add(targetEmergencyCds.getAmbulanceNo());
            additionalArrivalObjs.add(targetEmergencyCds.getArrivalMode());
            String additionalArrivalObjsAsJson = ObjectMapperPool.getInstance().writeValueAsString(additionalArrivalObjs);
            encounterArrival.setAdditionalFieldsJson(additionalArrivalObjsAsJson);

            encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterArrival);
            compositionBuilder.addSection("encounter-1-1", encounterInstanceAsJson);

            // sub encounter: the initial assessment  (sequence #2)
            // get initial assessment date. if null and there is a chief complaint value, use arrival date which is always present
            Date initialAssessmentDate = targetEmergencyCds.getDtInitialAssessment();
            String chiefComplaint = targetEmergencyCds.getChiefComplaint();
            if (initialAssessmentDate == null && !Strings.isNullOrEmpty(chiefComplaint)) {
                initialAssessmentDate = targetEmergencyCds.getDtArrival();
            }

            if (initialAssessmentDate != null) {

                Encounter encounterAssessment = new Encounter();
                encounterAssessment.setEncounterType("a&e assessment");
                encounterAssessment.setEncounterId(attendanceId+":2");

                encounterAssessment.setEffectiveDate(initialAssessmentDate);
                encounterAssessment.setEffectiveEndDate(null);
                encounterAssessment.setEpisodeOfCareId(targetEmergencyCds.getEpisodeId());
                encounterAssessment.setParentEncounterId(Integer.toString(parentEncounterId));
                encounterAssessment.setPractitionerId(targetEmergencyCds.getPerformerPersonnelId());
                encounterAssessment.setServiceProviderOrganisationId(targetEmergencyCds.getOrganisationCode());

                //create a list of additional data to store as Json for this encounterAssessment instance
                List<Object> additionalAssessmentObjs = new ArrayList<>();
                additionalAssessmentObjs.add(chiefComplaint);
                String additionalAssessmentObjsAsJson = ObjectMapperPool.getInstance().writeValueAsString(additionalAssessmentObjs);
                encounterAssessment.setAdditionalFieldsJson(additionalAssessmentObjsAsJson);

                encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterAssessment);
                compositionBuilder.addSection("encounter-1-2", encounterInstanceAsJson);
            }

            // sub encounter: the investigation and treatment  (sequence #3)
            Encounter encounterInvTreat = new Encounter();
            encounterInvTreat.setEncounterType("a&e investigations and treatments");
            encounterInvTreat.setEncounterId(attendanceId+":3");
            encounterInvTreat.setEffectiveDate(targetEmergencyCds.getDtSeenForTreatment());
            encounterInvTreat.setEffectiveEndDate(null);
            encounterInvTreat.setEpisodeOfCareId(targetEmergencyCds.getEpisodeId());
            encounterInvTreat.setParentEncounterId(Integer.toString(parentEncounterId));
            encounterInvTreat.setPractitionerId(targetEmergencyCds.getPerformerPersonnelId());
            encounterInvTreat.setServiceProviderOrganisationId(targetEmergencyCds.getOrganisationCode());

            // create a list of additional data to store as Json for this encounterInvTreat instance
            List <Object> additionalInvTreatObjs = new ArrayList<>();
            additionalInvTreatObjs.add(targetEmergencyCds.getDiagnosis());
            additionalInvTreatObjs.add(targetEmergencyCds.getInvestigations());
            additionalInvTreatObjs.add(targetEmergencyCds.getTreatments());
            additionalInvTreatObjs.add(targetEmergencyCds.getSafeguardingConcerns());

            String additionalInvTreatObjsAsJson = ObjectMapperPool.getInstance().writeValueAsString(additionalInvTreatObjs);
            encounterInvTreat.setAdditionalFieldsJson(additionalInvTreatObjsAsJson);

            encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterInvTreat);
            compositionBuilder.addSection("encounter-1-3", encounterInstanceAsJson);

            // sub encounter: the inpatient admission  (sequence #4) - this ultimately links up with the
            // inpatient_cds record with the same encounter_id
            Date admissionDate = targetEmergencyCds.getDtDecidedToAdmit();
            if (admissionDate != null) {
                Encounter encounterAdmission = new Encounter();
                encounterAdmission.setEncounterType("admission");
                encounterAdmission.setEncounterId(attendanceId + ":4");
                encounterAdmission.setEffectiveDate(admissionDate);
                encounterAdmission.setEffectiveEndDate(null);
                encounterAdmission.setEpisodeOfCareId(targetEmergencyCds.getEpisodeId());
                encounterAdmission.setParentEncounterId(Integer.toString(parentEncounterId));
                encounterAdmission.setPractitionerId(targetEmergencyCds.getPerformerPersonnelId());
                encounterAdmission.setServiceProviderOrganisationId(targetEmergencyCds.getOrganisationCode());

                // no additional data to store as Json for this encounterAdmission instance
                encounterAdmission.setAdditionalFieldsJson(null);

                encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterAdmission);
                compositionBuilder.addSection("encounter-1-4", encounterInstanceAsJson);
            }

            // sub encounter: the discharge/departure from emergency  (sequence #5)
            Date departureDate = targetEmergencyCds.getDtDeparture();
            if (departureDate != null) {

                Encounter encounterDischarge = new Encounter();
                encounterDischarge.setEncounterType("discharge");
                encounterDischarge.setEncounterId(attendanceId + ":5");
                encounterDischarge.setEffectiveDate(departureDate);
                encounterDischarge.setEffectiveEndDate(null);
                encounterDischarge.setEpisodeOfCareId(targetEmergencyCds.getEpisodeId());
                encounterDischarge.setParentEncounterId(Integer.toString(parentEncounterId));
                encounterDischarge.setPractitionerId(targetEmergencyCds.getPerformerPersonnelId());
                encounterDischarge.setServiceProviderOrganisationId(targetEmergencyCds.getOrganisationCode());

                // additional data to store as Json for this encounterAdmission instance
                List <Object> additionalDischargeObjs = new ArrayList<>();
                additionalDischargeObjs.add(targetEmergencyCds.getDischargeStatus());
                additionalDischargeObjs.add(targetEmergencyCds.getDischargeDestination());
                additionalDischargeObjs.add(targetEmergencyCds.getReferredToServices());
                String additionalDischargeObjsAsJson = ObjectMapperPool.getInstance().writeValueAsString(additionalDischargeObjs);
                encounterDischarge.setAdditionalFieldsJson(additionalDischargeObjsAsJson);

                encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterDischarge);
                compositionBuilder.addSection("encounter-1-5", encounterInstanceAsJson);
            }


            //save composition record
            fhirResourceFiler.savePatientResource(null, compositionBuilder);

            //LOG.debug("Transforming compositionId: "+uniqueId+"  Filed");
        }
    }
}