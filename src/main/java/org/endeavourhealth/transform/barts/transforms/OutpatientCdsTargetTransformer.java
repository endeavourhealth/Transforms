package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.ehr.models.Encounter;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingOutpatientCdsTarget;
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

public class OutpatientCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OutpatientCdsTargetTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createOutpatientCdsEncounters(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createOutpatientCdsEncounters(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target outpatient cds records for the current exchangeId
        List<StagingOutpatientCdsTarget> targetOutpatientCdsRecords = csvHelper.retrieveTargetOutpatientCds();
        if (targetOutpatientCdsRecords == null) {
            return;
        }

        for (StagingOutpatientCdsTarget targetOutpatientCds : targetOutpatientCdsRecords) {

            String uniqueId = targetOutpatientCds.getUniqueId();
            boolean isDeleted = targetOutpatientCds.isDeleted();

            if (isDeleted) {

                // retrieve the existing Composition resource to perform the deletion on
                Composition existingComposition
                        = (Composition) csvHelper.retrieveResourceForLocalId(ResourceType.Composition, uniqueId);

                if (existingComposition != null) {
                    CompositionBuilder compositionBuilder = new CompositionBuilder(existingComposition, targetOutpatientCds.getAudit());

                    //remember to pass in false since this existing composition is already ID mapped
                    fhirResourceFiler.deletePatientResource(null, false, compositionBuilder);
                } else {
                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Composition: {} for deletion", uniqueId);
                }

                continue;
            }

            // create the FHIR Composition resource
            CompositionBuilder compositionBuilder
                    = new CompositionBuilder(null, targetOutpatientCds.getAudit());
            compositionBuilder.setId(uniqueId);

            Integer personId = targetOutpatientCds.getPersonId();
            String patientIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Patient, Integer.toString(personId));
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            compositionBuilder.setPatientSubject(patientReference);
            compositionBuilder.setTitle("Encounter Composition");
            compositionBuilder.setStatus(Composition.CompositionStatus.FINAL);
            Identifier identifier = new Identifier();
            identifier.setValue(uniqueId);
            compositionBuilder.setIdentifier(identifier);

            // set single outpatient encounter, i.e. no sub encounters
            Encounter encounterOutpatient = new Encounter();
            encounterOutpatient.setEncounterType("outpatient");
            String attendanceId = targetOutpatientCds.getApptAttendanceIdentifier()+":OP";
            String attendanceEncounterIdStr
                    = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, attendanceId);
            encounterOutpatient.setEncounterId(attendanceEncounterIdStr);
            encounterOutpatient.setPatientId(patientIdStr);
            encounterOutpatient.setEffectiveDate(targetOutpatientCds.getApptDate());
            encounterOutpatient.setEffectiveEndDate(null);   //no end date

            String episodeIdStr = null;
            if (targetOutpatientCds.getEpisodeId() != null) {
                episodeIdStr =
                        IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, targetOutpatientCds.getEpisodeId().toString());
            }
            encounterOutpatient.setEpisodeOfCareId(episodeIdStr);

            //the top level encounterId which links to ENCNTR and other associated records
            String parentEncounterIdStr = null;
            Integer parentEncounterId = targetOutpatientCds.getEncounterId();
            if (parentEncounterId != null) {
                parentEncounterIdStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Encounter, Integer.toString(parentEncounterId));
            }
            encounterOutpatient.setParentEncounterId(parentEncounterIdStr);

            String performerIdStr = null;
            if (targetOutpatientCds.getPerformerPersonnelId() != null) {
                performerIdStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Practitioner, targetOutpatientCds.getPerformerPersonnelId().toString());
            }
            encounterOutpatient.setPractitionerId(performerIdStr);

            String serviceProviderOrgStr = null;
            if (!Strings.isNullOrEmpty(targetOutpatientCds.getApptSiteCode())) {
                serviceProviderOrgStr
                        = IdHelper.getOrCreateEdsResourceIdString(csvHelper.getServiceId(), ResourceType.Organization, targetOutpatientCds.getApptSiteCode());
            }
            encounterOutpatient.setServiceProviderOrganisationId(serviceProviderOrgStr);

            //add in additional fields data - note, all dates for either diagnosis or procedure data match the encounter date
            JsonObject additionalArrivalObjs = new JsonObject();
            additionalArrivalObjs.addProperty("administrative_category_code", targetOutpatientCds.getAdministrativeCategoryCode());
            additionalArrivalObjs.addProperty("appt_attended_code", targetOutpatientCds.getApptAttendedCode());
            additionalArrivalObjs.addProperty("appt_outcome_code", targetOutpatientCds.getApptOutcomeCode());
            if (!Strings.isNullOrEmpty(targetOutpatientCds.getPrimaryDiagnosisICD())) {
                additionalArrivalObjs.addProperty("primary_diagnosis", targetOutpatientCds.getPrimaryDiagnosisICD());
            }
            if (!Strings.isNullOrEmpty(targetOutpatientCds.getSecondaryDiagnosisICD())) {
                additionalArrivalObjs.addProperty("secondary_diagnosis", targetOutpatientCds.getSecondaryDiagnosisICD());
            }
            if (!Strings.isNullOrEmpty(targetOutpatientCds.getOtherDiagnosisICD())) {
                additionalArrivalObjs.addProperty("other_diagnosis", targetOutpatientCds.getOtherDiagnosisICD());
            }
            if (!Strings.isNullOrEmpty(targetOutpatientCds.getPrimaryProcedureOPCS())) {
                additionalArrivalObjs.addProperty("primary_procedure", targetOutpatientCds.getPrimaryProcedureOPCS());
            }
            if (!Strings.isNullOrEmpty(targetOutpatientCds.getSecondaryProcedureOPCS())) {
                additionalArrivalObjs.addProperty("secondary_procedure", targetOutpatientCds.getSecondaryProcedureOPCS());
            }
            if (!Strings.isNullOrEmpty(targetOutpatientCds.getOtherProceduresOPCS())) {
                additionalArrivalObjs.addProperty("other_procedures", targetOutpatientCds.getOtherProceduresOPCS());
            }

            encounterOutpatient.setAdditionalFieldsJson(additionalArrivalObjs.toString());
            String encounterInstanceAsJson = null;
            encounterInstanceAsJson = ObjectMapperPool.getInstance().writeValueAsString(encounterOutpatient);
            compositionBuilder.addSection("encounter-1", encounterOutpatient.getEncounterId(), encounterInstanceAsJson);

            //LOG.debug("Saving CompositionId: "+uniqueId+", with resourceData: "+ FhirSerializationHelper.serializeResource(compositionBuilder.getResource()));

            //save composition record
            fhirResourceFiler.savePatientResource(null, compositionBuilder);

            //LOG.debug("Transforming compositionId: "+uniqueId+"  Filed");
        }
    }
}