package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCriticalCareCdsTarget;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CriticalCareCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(CriticalCareCdsTargetTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createCriticalCareCdsEncounters(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createCriticalCareCdsEncounters(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target critical care cds records for the current exchangeId
        List<StagingCriticalCareCdsTarget> targetCriticalCareCdsRecords = csvHelper.retrieveTargetCriticalCareCds();
        if (targetCriticalCareCdsRecords == null) {
            return;
        }

        for (StagingCriticalCareCdsTarget targetCriticalCareCds : targetCriticalCareCdsRecords) {

            String uniqueId = targetCriticalCareCds.getUniqueId();
            boolean isDeleted = targetCriticalCareCds.isDeleted();

            //TODO: file into v2 Core publisher

//            if (isDeleted) {
//
//                // retrieve the existing Composition resource to perform the deletion on
//                Composition existingComposition
//                        = (Composition) csvHelper.retrieveResourceForLocalId(ResourceType.Composition, uniqueId);
//
//                if (existingComposition != null) {
//                    CompositionBuilder compositionBuilder = new CompositionBuilder(existingComposition, targetCriticalCareCds.getAudit());
//
//                    //remember to pass in false since this existing composition is already ID mapped
//                    fhirResourceFiler.deletePatientResource(null, false, compositionBuilder);
//                } else {
//                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Composition: {} for deletion", uniqueId);
//                }
//
//                continue;
//            }
//
//            // create the FHIR Composition resource
//            CompositionBuilder compositionBuilder
//                    = new CompositionBuilder(null, targetCriticalCareCds.getAudit());
//            compositionBuilder.setId(uniqueId);
//
//            Integer personId = targetCriticalCareCds.getPersonId();
//            String patientIdStr
//                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Patient, Integer.toString(personId));
//            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
//            compositionBuilder.setPatientSubject(patientReference);
//            compositionBuilder.setTitle("Encounter Composition");
//            compositionBuilder.setStatus(Composition.CompositionStatus.FINAL);
//            Identifier identifier = new Identifier();
//            identifier.setValue(uniqueId);
//            compositionBuilder.setIdentifier(identifier);
//
//            // set single critical care encounter, i.e. no sub encounters.
//            // These encounters link to inpatient records using the spellNumber and episodeNumber
//            Encounter encounterCriticalCare = new Encounter();
//            encounterCriticalCare.setEncounterType("critical care");
//            String criticalCareId = targetCriticalCareCds.getCriticalCareIdentifier()+":CC";
//            String criticalCareEncounterIdStr
//                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, criticalCareId);
//            encounterCriticalCare.setEncounterId(criticalCareEncounterIdStr);
//            encounterCriticalCare.setPatientId(patientIdStr);
//            encounterCriticalCare.setEffectiveDate(targetCriticalCareCds.getCareStartDate());
//            encounterCriticalCare.setEffectiveEndDate(targetCriticalCareCds.getDischargeDate());
//            encounterCriticalCare.setEpisodeOfCareId(null);
//
//            //the parent encounter Id pointing at the inpatient record linked by spellNumber and EpisodeNumber
//            String spellId = targetCriticalCareCds.getSpellNumber();
//            String episodeNumber = targetCriticalCareCds.getEpisodeNumber();
//            String parentEncounterId = spellId +":"+episodeNumber+":IP";
//            String parentEncounterIdStr
//                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, parentEncounterId);
//            encounterCriticalCare.setParentEncounterId(parentEncounterIdStr);
//
//            String performerIdStr = null;
//            if (targetCriticalCareCds.getPerformerPersonnelId() != null) {
//                performerIdStr
//                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Practitioner, targetCriticalCareCds.getPerformerPersonnelId().toString());
//            }
//            encounterCriticalCare.setPractitionerId(performerIdStr);
//
//            String serviceProviderOrgStr = null;
//            if (!Strings.isNullOrEmpty(targetCriticalCareCds.getOrganisationCode())) {
//                serviceProviderOrgStr
//                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Organization, targetCriticalCareCds.getOrganisationCode());
//            }
//            encounterCriticalCare.setServiceProviderOrganisationId(serviceProviderOrgStr);   //originally derived from the linked inpatient record
//
//            //add in additional fields data
//            JsonObject additionalObjs = new JsonObject();
//            additionalObjs.addProperty("critical_care_type_id", targetCriticalCareCds.getCriticalCareTypeId());
//            additionalObjs.addProperty("care_unit_function", targetCriticalCareCds.getCareUnitFunction());
//            additionalObjs.addProperty("admission_source_code", targetCriticalCareCds.getAdmissionSourceCode());
//            additionalObjs.addProperty("admission_type_code", targetCriticalCareCds.getAdmissionTypeCode());
//            additionalObjs.addProperty("admission_location", targetCriticalCareCds.getAdmissionTypeCode());
//            if (!Strings.isNullOrEmpty(targetCriticalCareCds.getGestationLengthAtDelivery())) {
//                additionalObjs.addProperty("gestation_length_at_delivery", targetCriticalCareCds.getGestationLengthAtDelivery());
//            }
//            if (targetCriticalCareCds.getAdvancedRespiratorySupportDays() != null) {
//                additionalObjs.addProperty("advanced_respiratory_support_days", targetCriticalCareCds.getAdvancedRespiratorySupportDays());
//            }
//            if (targetCriticalCareCds.getBasicRespiratorySupportsDays() != null) {
//                additionalObjs.addProperty("basic_respiratory_support_days", targetCriticalCareCds.getBasicRespiratorySupportsDays());
//            }
//            if (targetCriticalCareCds.getAdvancedCardiovascularSupportDays() != null) {
//                additionalObjs.addProperty("advanced_cardiovascular_support_days", targetCriticalCareCds.getAdvancedCardiovascularSupportDays());
//            }
//            if (targetCriticalCareCds.getRenalSupportDays() != null) {
//                additionalObjs.addProperty("renal_support_days", targetCriticalCareCds.getRenalSupportDays());
//            }
//            if (targetCriticalCareCds.getNeurologicalSupportDays() != null) {
//                additionalObjs.addProperty("neurological_support_days", targetCriticalCareCds.getNeurologicalSupportDays());
//            }
//            if (targetCriticalCareCds.getGastroIntestinalSupportDays() != null) {
//                additionalObjs.addProperty("gastro_intestinal_support_days", targetCriticalCareCds.getGastroIntestinalSupportDays());
//            }
//            if (targetCriticalCareCds.getDermatologicalSupportDays() != null) {
//                additionalObjs.addProperty("dermatological_support_days", targetCriticalCareCds.getDermatologicalSupportDays());
//            }
//            if (targetCriticalCareCds.getLiverSupportDays() != null) {
//                additionalObjs.addProperty("liver_support_days", targetCriticalCareCds.getLiverSupportDays());
//            }
//            if (targetCriticalCareCds.getOrganSupportMaximum() != null) {
//                additionalObjs.addProperty("organ_support_maximum", targetCriticalCareCds.getOrganSupportMaximum());
//            }
//            if (targetCriticalCareCds.getCriticalCareLevel2Days() != null) {
//                additionalObjs.addProperty("critical_care_level2_days", targetCriticalCareCds.getCriticalCareLevel2Days());
//            }
//            if (targetCriticalCareCds.getCriticalCareLevel3Days() != null) {
//                additionalObjs.addProperty("critical_care_level3_days", targetCriticalCareCds.getCriticalCareLevel3Days());
//            }
//            if (!Strings.isNullOrEmpty(targetCriticalCareCds.getDischargeStatusCode())) {
//                additionalObjs.addProperty("discharge_status_code", targetCriticalCareCds.getDischargeStatusCode());
//            }
//            if (!Strings.isNullOrEmpty(targetCriticalCareCds.getDischargeDestination())) {
//                additionalObjs.addProperty("discharge_destination", targetCriticalCareCds.getDischargeDestination());
//            }
//            if (!Strings.isNullOrEmpty(targetCriticalCareCds.getDischargeLocation())) {
//                additionalObjs.addProperty("discharge_location", targetCriticalCareCds.getDischargeLocation());
//            }
//            if (!Strings.isNullOrEmpty(targetCriticalCareCds.getCareActivity1())) {
//                additionalObjs.addProperty("care_activity_1", targetCriticalCareCds.getCareActivity1());
//            }
//            if (!Strings.isNullOrEmpty(targetCriticalCareCds.getCareActivity2100())) {
//                additionalObjs.addProperty("care_activity_2100", targetCriticalCareCds.getCareActivity2100());
//            }
//
//            encounterCriticalCare.setAdditionalFieldsJson(additionalObjs.toString());
//            String encounterInstanceAsJson = null;
//            encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterCriticalCare);
//            compositionBuilder.addSection("encounter-1", encounterCriticalCare.getEncounterId(), encounterInstanceAsJson);
//
//
//            //LOG.debug("Saving CompositionId: "+uniqueId+", with resourceData: "+ FhirSerializationHelper.serializeResource(compositionBuilder.getResource()));
//
//            //save composition record
//            fhirResourceFiler.savePatientResource(null, compositionBuilder);
//
//            //LOG.debug("Transforming compositionId: "+uniqueId+"  Filed");
        }
    }
}