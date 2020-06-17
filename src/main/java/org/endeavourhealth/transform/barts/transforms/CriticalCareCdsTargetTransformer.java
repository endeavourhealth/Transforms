package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCriticalCareCdsTarget;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
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
            if (performerPersonnelId != null) {

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
            //TODO: set name and values using IM map once done, i.e. replace critical_care_type etc.
            ContainedParametersBuilder containedParametersBuilder
                    = new ContainedParametersBuilder(encounterBuilder);
            containedParametersBuilder.removeContainedParameters();

            String criticalCareTypeId = targetCriticalCareCds.getCriticalCareTypeId();
            if (!Strings.isNullOrEmpty(criticalCareTypeId)) {
                containedParametersBuilder.addParameter("critical_care_type", "" + criticalCareTypeId);
            }
            String careUnitFunction = targetCriticalCareCds.getCareUnitFunction();
            if (!Strings.isNullOrEmpty(careUnitFunction)) {
                containedParametersBuilder.addParameter("cc_unit_function", "" + careUnitFunction);
            }
            String admissionSourceCode = targetCriticalCareCds.getAdmissionSourceCode();
            if (!Strings.isNullOrEmpty(admissionSourceCode)) {
                containedParametersBuilder.addParameter("cc_admission_source", "" + admissionSourceCode);
            }
            String admissionTypeCode = targetCriticalCareCds.getAdmissionTypeCode();
            if (!Strings.isNullOrEmpty(admissionTypeCode)) {
                containedParametersBuilder.addParameter("cc_admission_type", "" + admissionTypeCode);
            }
            String admissionLocationCode = targetCriticalCareCds.getAdmissionLocation();
            if (!Strings.isNullOrEmpty(admissionLocationCode)) {
                containedParametersBuilder.addParameter("cc_admission_location", "" + admissionLocationCode);
            }
            String dischargeStatusCode = targetCriticalCareCds.getDischargeStatusCode();
            if (!Strings.isNullOrEmpty(dischargeStatusCode)) {
                containedParametersBuilder.addParameter("cc_discharge_status", "" + dischargeStatusCode);
            }
            String dischargeDestinationCode = targetCriticalCareCds.getDischargeDestination();
            if (!Strings.isNullOrEmpty(dischargeDestinationCode)) {
                containedParametersBuilder.addParameter("cc_discharge_destination", "" + dischargeDestinationCode);
            }
            String dischargeLocationCode = targetCriticalCareCds.getDischargeLocation();
            if (!Strings.isNullOrEmpty(dischargeLocationCode)) {
                containedParametersBuilder.addParameter("cc_discharge_location", "" + dischargeLocationCode);
            }
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

            LOG.debug("Saving child critical encounter: "+ FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, encounterBuilder);
        }
    }
}