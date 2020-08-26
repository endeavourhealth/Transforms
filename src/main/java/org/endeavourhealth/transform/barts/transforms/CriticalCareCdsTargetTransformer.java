package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCriticalCareCdsTarget;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.*;
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

            //check if any patient filtering applied
            if (!csvHelper.processRecordFilteringOnPatientId(Integer.toString(targetCriticalCareCds.getPersonId()))) {
                continue;
            }

            String uniqueId = targetCriticalCareCds.getUniqueId();
            boolean isDeleted = targetCriticalCareCds.isDeleted();

            //the unique Id for the critical care encounter based on critical care identifier
            String criticalCareId = targetCriticalCareCds.getCriticalCareIdentifier()+":CC";

            if (isDeleted) {

                //this is an existing single critical care encounter linked to an episode parent, so only delete this one
                Encounter existingEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, criticalCareId);

                if (existingEncounter != null) {

                    EncounterBuilder encounterBuilder
                            = new EncounterBuilder(existingEncounter, targetCriticalCareCds.getAudit());

                    //delete the encounter
                    fhirResourceFiler.deletePatientResource(null, false, encounterBuilder);

                } else {
                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", criticalCareId);
                }

                continue;
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(null, targetCriticalCareCds.getAudit());
            encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
            encounterBuilder.setId(criticalCareId);

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Inpatient Critical Care");

            Integer personId = targetCriticalCareCds.getPersonId();
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            encounterBuilder.setPatient(patientReference);

            Integer performerPersonnelId = targetCriticalCareCds.getPerformerPersonnelId();
            if (performerPersonnelId != null && performerPersonnelId != 0) {

                Reference practitionerReference
                        = ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId));
                encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
            }
            String serviceProviderOrgId = targetCriticalCareCds.getOrganisationCode();
            if (!Strings.isNullOrEmpty(serviceProviderOrgId)) {

                encounterBuilder.setServiceProvider(ReferenceHelper.createReference(ResourceType.Organization, serviceProviderOrgId));
            }

            encounterBuilder.setPeriodStart(targetCriticalCareCds.getCareStartDate());

            if (targetCriticalCareCds.getDischargeDate() != null) {

                encounterBuilder.setPeriodEnd(targetCriticalCareCds.getDischargeDate());
                encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            } else {
                encounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
            }

            //these encounter events are children of the spell episode encounter records already created
            String spellId = targetCriticalCareCds.getSpellNumber();
            String episodeNumber = targetCriticalCareCds.getEpisodeNumber();
            String parentEncounterId = spellId +":"+episodeNumber+":IP:Episode";

            //retrieve and update the parent to point to this new child encounter
            EncounterBuilder existingParentEncounterBuilder = null;
            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, parentEncounterId);
            if (existingParentEncounter != null) {

                existingParentEncounterBuilder = new EncounterBuilder(existingParentEncounter, targetCriticalCareCds.getAudit());

                //link the parent to the child
                Reference childCriticalRef = ReferenceHelper.createReference(ResourceType.Encounter, criticalCareId);
                ContainedListBuilder listBuilder = new ContainedListBuilder(existingParentEncounterBuilder);
                if (existingParentEncounterBuilder.isIdMapped()) {

                    childCriticalRef
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(childCriticalRef, csvHelper);
                }
                listBuilder.addReference(childCriticalRef);
            } else {

                //if this happens, then we cannot locate the parent Inpatient Episode which must have been deleted
                TransformWarnings.log(LOG, csvHelper, "Cannot find existing parent EncounterId: {} for Critical Care CDS record: "+uniqueId, parentEncounterId);
                continue;
            }

            //set the new encounter as a child of it's parent
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, parentEncounterId);
            parentEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            encounterBuilder.setPartOf(parentEncounter);

            //add in additional extended data as Parameters resource with additional extension
            setCriticalContainedParameters(encounterBuilder, targetCriticalCareCds);

            String cdsUniqueId = targetCriticalCareCds.getUniqueId();
            if (!cdsUniqueId.isEmpty()) {

                cdsUniqueId = cdsUniqueId.replaceFirst("CCCDS-","");
                IdentifierBuilder.removeExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
                identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
                identifierBuilder.setValue(cdsUniqueId);
            }

            //save critical care encounter record and the parent (with updated references). Parent is filed first.
            fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);

            //LOG.debug("Saving child critical encounter: "+ FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, encounterBuilder);
        }
    }

    private static void setCriticalContainedParameters(EncounterBuilder encounterBuilder,
                                                        StagingCriticalCareCdsTarget targetCriticalCareCds) throws Exception {

        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();

        String careUnitFunction = targetCriticalCareCds.getCareUnitFunction();
        if (!Strings.isNullOrEmpty(careUnitFunction)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "care_unit_function"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "care_unit_function", careUnitFunction, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String admissionSourceCode = targetCriticalCareCds.getAdmissionSourceCode();
        if (!Strings.isNullOrEmpty(admissionSourceCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "admission_source_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "admission_source_code", admissionSourceCode,IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }
        String criticalCareTypeId = targetCriticalCareCds.getCriticalCareTypeId();
        if (!Strings.isNullOrEmpty(criticalCareTypeId)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "critical_care_type_id"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            //CriticalCareTypeId is a CDS local code
            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "critical_care_type_id", criticalCareTypeId, IMConstant.CDS_LOCAL
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String admissionTypeCode = targetCriticalCareCds.getAdmissionTypeCode();
        if (!Strings.isNullOrEmpty(admissionTypeCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "admission_type_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "admission_type_code", admissionTypeCode,IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String admissionLocationCode = targetCriticalCareCds.getAdmissionLocation();
        if (!Strings.isNullOrEmpty(admissionLocationCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "admission_location"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "admission_location", admissionLocationCode,IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String dischargeStatusCode = targetCriticalCareCds.getDischargeStatusCode();
        if (!Strings.isNullOrEmpty(dischargeStatusCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "discharge_status_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "discharge_status_code", dischargeStatusCode,IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String dischargeDestinationCode = targetCriticalCareCds.getDischargeDestination();
        if (!Strings.isNullOrEmpty(dischargeDestinationCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "discharge_destination"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "discharge_destination", dischargeDestinationCode,IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String dischargeLocationCode = targetCriticalCareCds.getDischargeLocation();
        if (!Strings.isNullOrEmpty(dischargeLocationCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "discharge_location"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "discharge_location", dischargeLocationCode,IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        //Very specific critical care type info here added as Additional JSON where present
        JsonObject criticalCareObjs = new JsonObject();

        String gestationLengthAtDelivery = targetCriticalCareCds.getGestationLengthAtDelivery();
        if (!Strings.isNullOrEmpty(gestationLengthAtDelivery)) {
            criticalCareObjs.addProperty("gestation_length_at_delivery", gestationLengthAtDelivery);
        }
        Integer advancedRespiratorySupportDays = targetCriticalCareCds.getAdvancedRespiratorySupportDays();
        if (advancedRespiratorySupportDays != null) {
            criticalCareObjs.addProperty("advanced_respiratory_support_days", advancedRespiratorySupportDays);
        }
        Integer basicRespiratorySupportsDays = targetCriticalCareCds.getBasicRespiratorySupportsDays();
        if (basicRespiratorySupportsDays != null) {
            criticalCareObjs.addProperty("basic_respiratory_support_days", basicRespiratorySupportsDays);
        }
        Integer advancedCardiovascularSupportDays = targetCriticalCareCds.getAdvancedCardiovascularSupportDays();
        if (advancedCardiovascularSupportDays != null) {
            criticalCareObjs.addProperty("advanced_cardiovascular_support_days", advancedCardiovascularSupportDays);
        }
        Integer basicCardiovascularSupportDays = targetCriticalCareCds.getBasicCardiovascularSupportDays();
        if (basicCardiovascularSupportDays != null) {
            criticalCareObjs.addProperty("basic_cardiovascular_support_days", basicCardiovascularSupportDays);
        }
        Integer renalSupportDays = targetCriticalCareCds.getRenalSupportDays();
        if (renalSupportDays != null) {
            criticalCareObjs.addProperty("renal_support_days", renalSupportDays);
        }
        Integer neurologicalSupportDays = targetCriticalCareCds.getNeurologicalSupportDays();
        if (neurologicalSupportDays != null) {
            criticalCareObjs.addProperty("neurological_support_days", neurologicalSupportDays);
        }
        Integer gastroIntestinalSupportDays = targetCriticalCareCds.getGastroIntestinalSupportDays();
        if (gastroIntestinalSupportDays != null) {
            criticalCareObjs.addProperty("gastrointestinal_support_days", gastroIntestinalSupportDays);
        }
        Integer dermatologicalSupportDays = targetCriticalCareCds.getDermatologicalSupportDays();
        if (dermatologicalSupportDays != null) {
            criticalCareObjs.addProperty("dermatological_support_days", dermatologicalSupportDays);
        }
        Integer liverSupportDays = targetCriticalCareCds.getLiverSupportDays();
        if (liverSupportDays != null) {
            criticalCareObjs.addProperty("liver_support_days", liverSupportDays);
        }
        Integer organSupportMaximum = targetCriticalCareCds.getOrganSupportMaximum();
        if (organSupportMaximum != null) {
            criticalCareObjs.addProperty("organ_support_maximum", organSupportMaximum);
        }
        Integer criticalCareLevel2Days = targetCriticalCareCds.getCriticalCareLevel2Days();
        if (criticalCareLevel2Days != null) {
            criticalCareObjs.addProperty("critical_care_level_2_days", criticalCareLevel2Days);
        }
        Integer criticalCareLevel3Days =  targetCriticalCareCds.getCriticalCareLevel3Days();
        if (criticalCareLevel3Days != null) {
            criticalCareObjs.addProperty("critical_care_level_3_days", criticalCareLevel3Days);
        }
        String careActivity1 = targetCriticalCareCds.getCareActivity1();
        if (!Strings.isNullOrEmpty(careActivity1)) {
            criticalCareObjs.addProperty("care_activity_1", careActivity1);
        }
        String careActivity2100 =  targetCriticalCareCds.getCareActivity2100();
        if (!Strings.isNullOrEmpty(careActivity2100)) {
            criticalCareObjs.addProperty("care_activity_2100", careActivity2100);
        }

        //add if any elements in the set
        if (!criticalCareObjs.entrySet().isEmpty()) {
            parametersBuilder.addParameter("JSON_critical_care", criticalCareObjs.toString());
        }
    }
}