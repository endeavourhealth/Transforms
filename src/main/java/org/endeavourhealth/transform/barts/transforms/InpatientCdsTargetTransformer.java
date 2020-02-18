package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.ehr.models.Encounter;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingInpatientCdsTarget;
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

import java.util.List;

public class InpatientCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(InpatientCdsTargetTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createInpatientCdsEncounters(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createInpatientCdsEncounters(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target inpatient cds records for the current exchangeId
        List<StagingInpatientCdsTarget> targetInpatientCdsRecords = csvHelper.retrieveTargetInpatientCds();
        if (targetInpatientCdsRecords == null) {
            return;
        }

        for (StagingInpatientCdsTarget targetInpatientCds : targetInpatientCdsRecords) {

            String uniqueId = targetInpatientCds.getUniqueId();
            boolean isDeleted = targetInpatientCds.isDeleted();

            if (isDeleted) {

                // retrieve the existing Composition resource to perform the deletion on
                Composition existingComposition
                        = (Composition) csvHelper.retrieveResourceForLocalId(ResourceType.Composition, uniqueId);

                if (existingComposition != null) {
                    CompositionBuilder compositionBuilder = new CompositionBuilder(existingComposition, targetInpatientCds.getAudit());

                    //remember to pass in false since this existing composition is already ID mapped
                    fhirResourceFiler.deletePatientResource(null, false, compositionBuilder);
                } else {
                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Composition: {} for deletion", uniqueId);
                }

                continue;
            }

            // create the FHIR Composition resource
            CompositionBuilder compositionBuilder
                    = new CompositionBuilder(null, targetInpatientCds.getAudit());
            compositionBuilder.setId(uniqueId);

            Integer personId = targetInpatientCds.getPersonId();
            String patientIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Patient, Integer.toString(personId));
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            compositionBuilder.setPatientSubject(patientReference);
            compositionBuilder.setTitle("Encounter Composition");
            compositionBuilder.setStatus(Composition.CompositionStatus.FINAL);
            Identifier identifier = new Identifier();
            identifier.setValue(uniqueId);
            compositionBuilder.setIdentifier(identifier);

            // Set top level encounter which encapsulates the other sub-encounters (up to X episode encounters in composition sections)
            String spellId = targetInpatientCds.getSpellNumber();
            String topLevelEncounterId = spellId + ":00:IP";
            String topLevelEncounterIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, topLevelEncounterId);
            String episodeIdStr = null;
            if (targetInpatientCds.getEpisodeId() != null) {
                episodeIdStr =
                        IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, targetInpatientCds.getEpisodeId().toString());
            }
            String serviceProviderOrgStr = null;
            if (!Strings.isNullOrEmpty(targetInpatientCds.getEpisodeStartSiteCode())) {
                serviceProviderOrgStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Organization, targetInpatientCds.getEpisodeStartSiteCode());
            }
            String episodeNumber = targetInpatientCds.getEpisodeNumber();

            // NOTE: top level inpatient encounter is only created / updated when episodeNumber = 01 to prevent duplicates
            if (episodeNumber.equalsIgnoreCase("01")) {
                Encounter encounterInpatient = new Encounter();
                encounterInpatient.setEncounterType("inpatient");
                encounterInpatient.setEncounterId(topLevelEncounterIdStr);
                encounterInpatient.setPatientId(patientIdStr);
                encounterInpatient.setEffectiveDate(targetInpatientCds.getDtSpellStart());
                encounterInpatient.setEffectiveEndDate(targetInpatientCds.getDtDischarge());  //the discharge date if present
                encounterInpatient.setEpisodeOfCareId(episodeIdStr);

                //This is the top level encounterId which links to ENCNTR and other associated records
                Integer parentEncounterId = targetInpatientCds.getEncounterId();
                String parentEncounterIdStr = null;
                if (parentEncounterId != null) {
                    parentEncounterIdStr
                            = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, Integer.toString(parentEncounterId));
                }
                encounterInpatient.setParentEncounterId(parentEncounterIdStr);
                encounterInpatient.setPractitionerId(null);   //top level inpatient encounter has no practitioner
                encounterInpatient.setServiceProviderOrganisationId(serviceProviderOrgStr);

                //add in additional fields data
                JsonObject additionalInpatientObjs = new JsonObject();
                additionalInpatientObjs.addProperty("admission_method_code", targetInpatientCds.getAdmissionMethodCode());
                additionalInpatientObjs.addProperty("admission_source_code", targetInpatientCds.getAdmissionSourceCode());
                additionalInpatientObjs.addProperty("patient_classification", targetInpatientCds.getPatientClassification());

                encounterInpatient.setAdditionalFieldsJson(additionalInpatientObjs.toString());
                String encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterInpatient);
                compositionBuilder.addSection("encounter-1", encounterInpatient.getEncounterId(), encounterInstanceAsJson);
            }

            //an inpatient sub encounter has an episode number which is used with the top level spellNumber to create the encounterId reference
            Encounter encounterInpatientEpisode = new Encounter();
            encounterInpatientEpisode.setEncounterType("inpatient episode");

            String encounterId = spellId +":"+episodeNumber+":IP";
            String encounterIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, encounterId);

            // each inpatient episode has a specific practitioner
            String performerIdStr = null;
            if (targetInpatientCds.getPerformerPersonnelId() != null) {
                performerIdStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Practitioner, targetInpatientCds.getPerformerPersonnelId().toString());
            }

            encounterInpatientEpisode.setEncounterId(encounterIdStr);
            encounterInpatientEpisode.setPatientId(patientIdStr);
            encounterInpatientEpisode.setEffectiveDate(targetInpatientCds.getDtEpisodeStart());
            encounterInpatientEpisode.setEffectiveEndDate(targetInpatientCds.getDtEpisodeEnd());
            encounterInpatientEpisode.setEpisodeOfCareId(episodeIdStr);
            encounterInpatientEpisode.setParentEncounterId(topLevelEncounterIdStr);
            encounterInpatientEpisode.setPractitionerId(performerIdStr);
            encounterInpatientEpisode.setServiceProviderOrganisationId(serviceProviderOrgStr);

            // create a list of additional data to store as Json for this encounterInpatientEpisode instance
            JsonObject additionalObjs = new JsonObject();
            additionalObjs.addProperty("episode_start_ward_code", targetInpatientCds.getEpisodeStartWardCode());
            additionalObjs.addProperty("episode_end_ward_code", targetInpatientCds.getEpisodeEndWardCode());
            if (!Strings.isNullOrEmpty(targetInpatientCds.getPrimaryDiagnosisICD())) {
                additionalObjs.addProperty("primary_diagnosis", targetInpatientCds.getPrimaryDiagnosisICD());
            }
            if (!Strings.isNullOrEmpty(targetInpatientCds.getSecondaryDiagnosisICD())) {
                additionalObjs.addProperty("secondary_diagnosis", targetInpatientCds.getSecondaryDiagnosisICD());
            }
            if (!Strings.isNullOrEmpty(targetInpatientCds.getOtherDiagnosisICD())) {
                additionalObjs.addProperty("other_diagnosis", targetInpatientCds.getOtherDiagnosisICD());
            }
            if (!Strings.isNullOrEmpty(targetInpatientCds.getPrimaryProcedureOPCS())) {
                additionalObjs.addProperty("primary_procedure", targetInpatientCds.getPrimaryProcedureOPCS());
            }
            if (!Strings.isNullOrEmpty(targetInpatientCds.getSecondaryProcedureOPCS())) {
                additionalObjs.addProperty("secondary_procedure", targetInpatientCds.getSecondaryProcedureOPCS());
            }
            if (!Strings.isNullOrEmpty(targetInpatientCds.getOtherProceduresOPCS())) {
                additionalObjs.addProperty("other_procedures", targetInpatientCds.getOtherProceduresOPCS());
            }

            encounterInpatientEpisode.setAdditionalFieldsJson(additionalObjs.toString());

            String encounterEpisodeInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterInpatientEpisode);
            String encounterEpisodeDesc = "encounter-1-"+episodeNumber;
            compositionBuilder.addSection(encounterEpisodeDesc, encounterInpatientEpisode.getEncounterId(), encounterEpisodeInstanceAsJson);

            // if the episodeNumber record is 01 and the DischargeDate is present then create the discharge encounter
            if (episodeNumber.equalsIgnoreCase("01") && targetInpatientCds.getDtDischarge() != null) {

                Encounter encounterInpatientDischarge = new Encounter();
                encounterInpatientDischarge.setEncounterType("inpatient discharge");

                encounterId = spellId +":"+episodeNumber+":IP:D";
                encounterInpatientDischarge.setEncounterId(encounterId);
                encounterInpatientDischarge.setPatientId(patientIdStr);
                encounterInpatientDischarge.setEffectiveDate(targetInpatientCds.getDtDischarge());
                encounterInpatientDischarge.setEffectiveEndDate(null);
                encounterInpatientDischarge.setEpisodeOfCareId(episodeIdStr);
                encounterInpatientDischarge.setParentEncounterId(topLevelEncounterIdStr);
                encounterInpatientDischarge.setPractitionerId(performerIdStr);
                encounterInpatientDischarge.setServiceProviderOrganisationId(serviceProviderOrgStr);

                //add in additional fields data
                JsonObject additionalDischargeObjs = new JsonObject();
                additionalDischargeObjs.addProperty("discharge_method", targetInpatientCds.getDischargeMethod());
                additionalDischargeObjs.addProperty("discharge_destination", targetInpatientCds.getDischargeDestinationCode());

                encounterInpatientDischarge.setAdditionalFieldsJson(additionalDischargeObjs.toString());
                String encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterInpatientDischarge);
                String encounterDischargeDesc = "encounter-1-"+episodeNumber+"D";
                compositionBuilder.addSection(encounterDischargeDesc, encounterInpatientDischarge.getEncounterId(), encounterInstanceAsJson);
            }


            //LOG.debug("Saving CompositionId: "+uniqueId+", with resourceData: "+ FhirSerializationHelper.serializeResource(compositionBuilder.getResource()));

            //save composition record
            fhirResourceFiler.savePatientResource(null, compositionBuilder);

            //LOG.debug("Transforming compositionId: "+uniqueId+"  Filed");
        }
    }
}