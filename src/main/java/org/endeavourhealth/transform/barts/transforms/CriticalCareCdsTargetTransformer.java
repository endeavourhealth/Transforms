package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
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

            EncounterBuilder encounterBuilder = new EncounterBuilder();
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

                existingParentEncounterBuilder = new EncounterBuilder(existingParentEncounter);

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

            //TODO: loads of very specific critical care type info here

            // targetCriticalCareCds.getGestationLengthAtDelivery());
            // targetCriticalCareCds.getAdvancedRespiratorySupportDays());
            // targetCriticalCareCds.getBasicRespiratorySupportsDays());
            // targetCriticalCareCds.getAdvancedCardiovascularSupportDays());
            // targetCriticalCareCds.getRenalSupportDays());
            // targetCriticalCareCds.getNeurologicalSupportDays());
            // targetCriticalCareCds.getGastroIntestinalSupportDays());
            // targetCriticalCareCds.getDermatologicalSupportDays());
            // targetCriticalCareCds.getLiverSupportDays());
            // targetCriticalCareCds.getOrganSupportMaximum());
            // targetCriticalCareCds.getCriticalCareLevel2Days());
            // targetCriticalCareCds.getCriticalCareLevel3Days());
            // targetCriticalCareCds.getCareActivity1());
            // targetCriticalCareCds.getCareActivity2100());

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
                    "critical_care_type"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","critical",
                    "critical_care_type", criticalCareTypeId,IMConstant.BARTS_CERNER
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
    }
}