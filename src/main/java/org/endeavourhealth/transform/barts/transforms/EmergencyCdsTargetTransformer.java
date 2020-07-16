package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.apache.commons.lang3.ObjectUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingEmergencyCdsTarget;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

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

        for (StagingEmergencyCdsTarget targetEmergencyCds : targetEmergencyCdsRecords) {

            //check if any patient filtering applied
            String personId = Integer.toString(targetEmergencyCds.getPersonId());
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                continue;
            }

            boolean isDeleted = targetEmergencyCds.isDeleted();
            if (isDeleted) {

                deleteEmergencyCdsEncounterAndChildren(targetEmergencyCds, fhirResourceFiler, csvHelper);
                continue;
            }

            //process top level encounter - the existing parent encounter
            Integer encounterId = targetEmergencyCds.getEncounterId();  //this is used to identify the top level parent encounter

            if (encounterId != null) {

                EncounterBuilder parentEncounterBuilder = null;

                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //update the existing top level encounter
                    updateExistingParentEncounter(existingParentEncounter, targetEmergencyCds, fhirResourceFiler, csvHelper);

                    //create the linked child encounters
                    parentEncounterBuilder = new EncounterBuilder(existingParentEncounter, targetEmergencyCds.getAudit());
                    createEmergencyCdsEncounters(targetEmergencyCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

                } else {

                    //create top level parent with minimum data and the sub encounters
                    parentEncounterBuilder
                            = createEmergencyCdsEncounterParentAndSubs(targetEmergencyCds, fhirResourceFiler, csvHelper);
                }

                //now delete any older HL7 Encounters for patients we've updated
                //but waiting until everything has been saved to the DB first
                fhirResourceFiler.waitUntilEverythingIsSaved();

                //find the patient UUID for the encounters we have just filed, so we can tidy up the
                //HL7 encounters after doing all the saving of the DW encounters
                Reference patientReference = parentEncounterBuilder.getPatient();
                if (!parentEncounterBuilder.isIdMapped()) {
                    patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                }
                String patientUuid = ReferenceHelper.getReferenceId(patientReference);
                deleteHL7ReceiverPatientEmergencyEncounters(patientUuid, fhirResourceFiler, csvHelper);

            } else {

                String uniqueId = targetEmergencyCds.getUniqueId();
                //throw new Exception("encounter_id missing for Inpatient CDS record: " + uniqueId);
                LOG.warn("encounter_id missing for Emergency CDS record: " + uniqueId);
            }
        }
    }

    private static void createEmergencyCdsEncounters(StagingEmergencyCdsTarget targetEmergencyCds,
                                                     FhirResourceFiler fhirResourceFiler,
                                                     BartsCsvHelper csvHelper,
                                                     EncounterBuilder existingParentEncounterBuilder) throws Exception {


        ///retrieve the parent encounter (if not passed in) to point to any new child encounters created during this method
        if (existingParentEncounterBuilder == null) {
            Integer parentEncounterId = targetEmergencyCds.getEncounterId();
            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(parentEncounterId));
            existingParentEncounterBuilder = new EncounterBuilder(existingParentEncounter);
        }
        ContainedListBuilder existingEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

        //unique to the emergency dept. attendance
        String attendanceId = targetEmergencyCds.getAttendanceId();

        ////start with the A&E arrival encounter///////////////////////////////////////////////////////////////////////
        EncounterBuilder arrivalEncounterBuilder = new EncounterBuilder();
        arrivalEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        String arrivalEncounterId = attendanceId + ":01:EM";
        arrivalEncounterBuilder.setId(arrivalEncounterId);
        Date arrivalDate = targetEmergencyCds.getDtArrival();
        arrivalEncounterBuilder.setPeriodStart(arrivalDate);
        arrivalEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

        CodeableConceptBuilder codeableConceptBuilderAdmission
                = new CodeableConceptBuilder(arrivalEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilderAdmission.setText("Emergency Arrival");

        setCommonEncounterAttributes(arrivalEncounterBuilder, targetEmergencyCds, csvHelper, true, fhirResourceFiler);

        //and link the parent to this new child encounter
        Reference childArrivalRef = ReferenceHelper.createReference(ResourceType.Encounter, arrivalEncounterId);
        if (existingParentEncounterBuilder.isIdMapped()) {

            childArrivalRef
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(childArrivalRef, csvHelper);
        }
        existingEncounterList.addReference(childArrivalRef);

        //set the additional extended data as Parameters resource with additional extension
        setArrivalContainedParameters(arrivalEncounterBuilder, targetEmergencyCds);

        //check for other dates to determine if the arrival has ended
        Date assessmentDate = targetEmergencyCds.getDtInitialAssessment();
        Date invAndTreatmentsDate = targetEmergencyCds.getDtSeenForTreatment();
        Date admitDate = targetEmergencyCds.getDtDecidedToAdmit();
        Date conclusionDate = targetEmergencyCds.getDtConclusion();
        Date departureDate = targetEmergencyCds.getDtDeparture();
        Date aeEndDate
                = ObjectUtils.firstNonNull(assessmentDate, invAndTreatmentsDate, admitDate, conclusionDate, departureDate);
        if (aeEndDate != null) {

            arrivalEncounterBuilder.setPeriodEnd(aeEndDate);
            arrivalEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        }

        ////Is there an initial assessment encounter?///////////////////////////////////////////////////////////////////
        EncounterBuilder assessmentEncounterBuilder = null;
        if (assessmentDate != null) {

            assessmentEncounterBuilder = new EncounterBuilder();
            assessmentEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String assessmentEncounterId = attendanceId + ":02:EM";
            assessmentEncounterBuilder.setId(assessmentEncounterId);
            assessmentEncounterBuilder.setPeriodStart(assessmentDate);
            assessmentEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderAssessment
                    = new CodeableConceptBuilder(assessmentEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAssessment.setText("Emergency Assessment");

            setCommonEncounterAttributes(assessmentEncounterBuilder, targetEmergencyCds, csvHelper, true, fhirResourceFiler);

            //and link the parent to this new child encounter
            Reference childAssessmentRef = ReferenceHelper.createReference(ResourceType.Encounter, assessmentEncounterId);
            if (existingParentEncounterBuilder.isIdMapped()) {

                childAssessmentRef
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAssessmentRef, csvHelper);
            }
            existingEncounterList.addReference(childAssessmentRef);

            //add in additional extended data as Parameters resource with additional extension
            //TODO:  Check on live that safeguearding concerns Snomed examples are filed verses additional
//            ContainedParametersBuilder containedParametersBuilderAss
//                    = new ContainedParametersBuilder(assessmentEncounterBuilder);
//            containedParametersBuilderAss.removeContainedParameters();
//
//            String safeGuardingConcerns = targetEmergencyCds.getSafeguardingConcerns();
//            if (!Strings.isNullOrEmpty(safeGuardingConcerns)) {
//
//                //containedParametersBuilderAss.addParameter("safe_guarding_concerns", "" + safeGuardingConcerns);
//            }

            Date aeAssessmentEndDate
                    = ObjectUtils.firstNonNull(invAndTreatmentsDate, admitDate, conclusionDate, departureDate);
            if (aeAssessmentEndDate != null) {

                assessmentEncounterBuilder.setPeriodEnd(aeAssessmentEndDate);
                assessmentEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
        }

        ////Is there a treatments encounter?////////////////////////////////////////////////////////////////////////////
        EncounterBuilder treatmentsEncounterBuilder = null;
        if (invAndTreatmentsDate != null) {

            treatmentsEncounterBuilder = new EncounterBuilder();
            treatmentsEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String treatmentsEncounterId = attendanceId + ":03:EM";
            treatmentsEncounterBuilder.setId(treatmentsEncounterId);
            treatmentsEncounterBuilder.setPeriodStart(invAndTreatmentsDate);
            treatmentsEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderTreatments
                    = new CodeableConceptBuilder(treatmentsEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderTreatments.setText("Emergency Treatment");

            setCommonEncounterAttributes(treatmentsEncounterBuilder, targetEmergencyCds, csvHelper, true, fhirResourceFiler);

            //and link the parent to this new child encounter
            Reference childTreatmentsRef = ReferenceHelper.createReference(ResourceType.Encounter, treatmentsEncounterId);
            if (existingParentEncounterBuilder.isIdMapped()) {

                childTreatmentsRef
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(childTreatmentsRef, csvHelper);
            }
            existingEncounterList.addReference(childTreatmentsRef);

            //TODO - do we save the linked clinical data here - check live examples?
            //targetEmergencyCds.getDiagnosis();
            //targetEmergencyCds.getInvestigations();
            //targetEmergencyCds.getTreatments();

            String referredToServices = targetEmergencyCds.getReferredToServices();
            if (!Strings.isNullOrEmpty(referredToServices)) {

                //TODO:  create referrals(s) linked to main encounter_id or ParametersList?
            }

            Date aeTreatmentsEndDate
                    = ObjectUtils.firstNonNull(admitDate, conclusionDate, departureDate);
            if (aeTreatmentsEndDate != null) {

                treatmentsEncounterBuilder.setPeriodEnd(aeTreatmentsEndDate);
                treatmentsEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
        }


        ////Is there a conclusion/discharge encounter?/////////////////////////////////////////////////////////////////////////////
        EncounterBuilder conclusionEncounterBuilder = null;
        if (conclusionDate != null) {

            conclusionEncounterBuilder = new EncounterBuilder();
            conclusionEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String conclusionEncounterId = attendanceId + ":04:EM";
            conclusionEncounterBuilder.setId(conclusionEncounterId);
            conclusionEncounterBuilder.setPeriodStart(conclusionDate);
            conclusionEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderDischarge
                    = new CodeableConceptBuilder(conclusionEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderDischarge.setText("Emergency Conclusion");

            setCommonEncounterAttributes(conclusionEncounterBuilder, targetEmergencyCds, csvHelper, true, fhirResourceFiler);

            //and link the parent to this new child encounter
            Reference childConclusionRef = ReferenceHelper.createReference(ResourceType.Encounter, conclusionEncounterId);
            if (existingParentEncounterBuilder.isIdMapped()) {

                childConclusionRef
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(childConclusionRef, csvHelper);
            }
            existingEncounterList.addReference(childConclusionRef);

            //add in additional extended data as Parameters resource with additional extension
            setConclusionContainedParameters(conclusionEncounterBuilder, targetEmergencyCds);

            //note ordering of date here, i.e. if not concluded, then departed is the end date
            Date aeConclusionEndDate
                    = ObjectUtils.firstNonNull(conclusionDate, departureDate);
            if (aeConclusionEndDate != null) {

                conclusionEncounterBuilder.setPeriodEnd(aeConclusionEndDate);
                conclusionEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
        }

        //save the existing parent encounter here with the updated child refs added during this method,
        //then the child sub encounter afterwards
        //LOG.debug("Saving parent EM encounter: "+ FhirSerializationHelper.serializeResource(existingParentEncounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);

        //save the A&E arrival encounter - always created
        //LOG.debug("Saving child arrival EM encounter: "+ FhirSerializationHelper.serializeResource(arrivalEncounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, arrivalEncounterBuilder);

        //save the A&E assessment encounter
        if (assessmentEncounterBuilder != null) {
            //LOG.debug("Saving child assessment EM encounter: "+ FhirSerializationHelper.serializeResource(assessmentEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, assessmentEncounterBuilder);
        }
        //save the A&E treatments encounter
        if (treatmentsEncounterBuilder != null) {
            //LOG.debug("Saving child treatments EM encounter: "+ FhirSerializationHelper.serializeResource(treatmentsEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, treatmentsEncounterBuilder);
        }
        //save the A&E discharge encounter
        if (conclusionEncounterBuilder != null) {
            //LOG.debug("Saving child discharge EM encounter: "+ FhirSerializationHelper.serializeResource(dischargeEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, conclusionEncounterBuilder);
        }
    }

    private static EncounterBuilder createEmergencyCdsEncounterParentAndSubs(StagingEmergencyCdsTarget targetEmergencyCds,
                                                                 FhirResourceFiler fhirResourceFiler,
                                                                 BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentEncounterBuilder = new EncounterBuilder();
        parentEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        Integer encounterId = targetEmergencyCds.getEncounterId();
        parentEncounterBuilder.setId(Integer.toString(encounterId));

        Date arrivalDate = targetEmergencyCds.getDtArrival();
        parentEncounterBuilder.setPeriodStart(arrivalDate);

        Date departureDate = targetEmergencyCds.getDtDeparture();
        if (departureDate != null) {

            parentEncounterBuilder.setPeriodEnd(departureDate);
            parentEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            parentEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Emergency");

        setCommonEncounterAttributes(parentEncounterBuilder, targetEmergencyCds, csvHelper, false, fhirResourceFiler);

        //then create child level encounters linked to this new parent
        createEmergencyCdsEncounters(targetEmergencyCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

        return parentEncounterBuilder;
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     StagingEmergencyCdsTarget targetEmergencyCds,
                                                     BartsCsvHelper csvHelper,
                                                     boolean isChildEncounter,
                                                     FhirResourceFiler fhirResourceFiler) throws Exception  {

        //every encounter has the following common attributes
        Integer personId = targetEmergencyCds.getPersonId();
        if (personId !=null) {
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            if (builder.isIdMapped()) {

                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }
            builder.setPatient(patientReference);
        }
        // Retrieve or create EpisodeOfCare for top level parent encounter only
        if (!isChildEncounter) {

            EpisodeOfCareBuilder episodeOfCareBuilder
                    = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(targetEmergencyCds);
            if (episodeOfCareBuilder != null) {

                csvHelper.setEpisodeReferenceOnEncounter(episodeOfCareBuilder, builder, fhirResourceFiler);

                // Using parent encounter start and end times
                Date arrivalDate = targetEmergencyCds.getDtArrival();
                Date departureDate = targetEmergencyCds.getDtDeparture();

                if (arrivalDate != null) {

                    if (episodeOfCareBuilder.getRegistrationStartDate() == null || arrivalDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {
                        episodeOfCareBuilder.setRegistrationStartDate(arrivalDate);
                        episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
                    }

                    // End date
                    if (departureDate != null) {

                        if (episodeOfCareBuilder.getRegistrationEndDate() == null || departureDate.after(episodeOfCareBuilder.getRegistrationEndDate())) {
                            episodeOfCareBuilder.setRegistrationEndDate(departureDate);
                        }
                    }
                } else {
                    if (episodeOfCareBuilder.getRegistrationEndDate() == null) {
                        episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.PLANNED);
                    }
                }

                // Check whether to Finish EpisodeOfCare
                // If the patient has left AE (checkout-time/enddatetime) and not been admitted (decisionToAdmitDateTime empty) complete EpisodeOfCare
                Date decidedToAdmitDate = targetEmergencyCds.getDtDecidedToAdmit();
                if (departureDate != null
                        && decidedToAdmitDate != null) {

                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
                }
            }
        }
        Integer performerPersonnelId = targetEmergencyCds.getPerformerPersonnelId();
        if (performerPersonnelId != null) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId));
            if (builder.isIdMapped()) {

                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }
        String serviceProviderOrgId = targetEmergencyCds.getOrganisationCode();
        if (!Strings.isNullOrEmpty(serviceProviderOrgId)) {

            Reference organizationReference
                    = ReferenceHelper.createReference(ResourceType.Organization, serviceProviderOrgId);
            if (builder.isIdMapped()) {

                organizationReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, csvHelper);
            }
            builder.setServiceProvider(organizationReference);
        }
        //get the existing parent encounter to link to this top level encounter if this is a child
        if (isChildEncounter) {

            Integer encounterId = targetEmergencyCds.getEncounterId();
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(encounterId));
            parentEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            builder.setPartOf(parentEncounter);
        }
        //set the CDS identifier against the Encounter
        String cdsUniqueId = targetEmergencyCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("ECDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(builder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(builder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }
    }

    private static void deleteEmergencyCdsEncounterAndChildren(StagingEmergencyCdsTarget targetEmergencyCds,
                                                               FhirResourceFiler fhirResourceFiler,
                                                               BartsCsvHelper csvHelper) throws Exception {

        Integer encounterId = targetEmergencyCds.getEncounterId();  //this is used to identify the top level parent episode

        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        if (encounterId != null) {

            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));

            if (existingParentEncounter != null) {

                EncounterBuilder parentEncounterBuilder
                        = new EncounterBuilder(existingParentEncounter, targetEmergencyCds.getAudit());

                //has this encounter got child encounters?
                if (existingParentEncounter.hasContained()) {

                    ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
                    ResourceDalI resourceDal = DalProvider.factoryResourceDal();

                    for (List_.ListEntryComponent item : listBuilder.getContainedListItems()) {
                        Reference ref = item.getItem();
                        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                        if (comps.getResourceType() != ResourceType.Encounter) {
                            continue;
                        }
                        Encounter childEncounter
                                = (Encounter) resourceDal.getCurrentVersionAsResource(csvHelper.getServiceId(), ResourceType.Encounter, comps.getId());
                        if (childEncounter != null) {
                            LOG.debug("Deleting child encounter " + childEncounter.getId());

                            fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                        } else {

                            TransformWarnings.log(LOG, csvHelper, "Cannot find existing child Encounter ref: {} for deletion", comps.getId());
                        }
                    }
                }

                //finally, delete the top level parent
                fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

            } else {
                TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", encounterId);
            }
        }
    }

    private static void updateExistingParentEncounter(Encounter existingEncounter,
                                                      StagingEmergencyCdsTarget targetEmergencyCds,
                                                      FhirResourceFiler fhirResourceFiler,
                                                      BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder existingEncounterBuilder
                = new EncounterBuilder(existingEncounter, targetEmergencyCds.getAudit());

        //todo - decide on how much to update the top level with
        Date departureDate = targetEmergencyCds.getDtDeparture();
        if (departureDate != null) {

            existingEncounterBuilder.setPeriodEnd(departureDate);
            existingEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            //may not have been discharged, i.e. passed away
            Date conclusionDate = targetEmergencyCds.getDtConclusion();
            if (conclusionDate != null) {

                existingEncounterBuilder.setPeriodEnd(conclusionDate);
                existingEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

            } else {

                existingEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
            }
        }

        String cdsUniqueId = targetEmergencyCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("ECDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(existingEncounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(existingEncounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }

        fhirResourceFiler.savePatientResource(null, !existingEncounterBuilder.isIdMapped(), existingEncounterBuilder);
    }

    /**
     * we match to some HL7 Receiver Encounters, basically taking them over
     * so we call this to tidy up (delete) any Episodes left not taken over, as the HL7 Receiver creates too many
     * episodes because it doesn't have the data to avoid doing so
     */
    private static void deleteHL7ReceiverPatientEmergencyEncounters(String patientUuid,
                                                           FhirResourceFiler fhirResourceFiler,
                                                           BartsCsvHelper csvHelper) throws Exception {

        UUID serviceUuid = fhirResourceFiler.getServiceId();
        UUID systemUuid = fhirResourceFiler.getSystemId();

        //we want to delete any HL7 Encounter more than 24 hours older than the DW file extract date
        Date extractDateTime = csvHelper.getExtractDateTime();
        Date cutoff = new Date(extractDateTime.getTime() - (24 * 60 * 60 * 1000));

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> resourceWrappers
                = resourceDal.getResourcesByPatient(serviceUuid, UUID.fromString(patientUuid), ResourceType.Encounter.toString());
        for (ResourceWrapper wrapper: resourceWrappers) {

            //if this episode is for our own service + system ID (i.e. DW feed), then leave it
            UUID wrapperSystemId = wrapper.getSystemId();
            if (wrapperSystemId.equals(systemUuid)) {
                continue;
            }

            String json = wrapper.getResourceData();
            Encounter existingEncounter = (Encounter) FhirSerializationHelper.deserializeResource(json);

            //if the HL7 Encounter has no date info at all or is before our cutoff, delete it
            if (!existingEncounter.hasPeriod()
                    || !existingEncounter.getPeriod().hasStart()
                    || existingEncounter.getPeriod().getStart().before(cutoff)) {

                //finally, check it is an Emergency encounter class before deleting
                if (existingEncounter.getClass_().equals(Encounter.EncounterClass.EMERGENCY)) {
                    GenericBuilder builder = new GenericBuilder(existingEncounter);
                    //we have no audit for deleting these encounters, since it's not triggered by a specific piece of data
                    //builder.setDeletedAudit(...);
                    fhirResourceFiler.deletePatientResource(null, false, builder);
                }
            }
        }
    }

    private static void setArrivalContainedParameters(EncounterBuilder encounterBuilder,
                                                      StagingEmergencyCdsTarget targetEmergencyCds) throws Exception {

        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();

        String aeAttendanceCategoryCode = targetEmergencyCds.getAttendanceCategory();
        if (!Strings.isNullOrEmpty(aeAttendanceCategoryCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "attendance_category"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "attendance_category", aeAttendanceCategoryCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();   //DM_aeAttendanceCategory
            String valueConceptIri = valueResponse.getConcept().getIri();         //CM_AEAttCat3
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        String aeAttendanceSource = targetEmergencyCds.getAttendanceSource();
        if (!Strings.isNullOrEmpty(aeAttendanceSource)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "attendance_source"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "attendance_source", aeAttendanceSource,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = valueResponse.getConcept().getIri();
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        String aeDepartmentType = targetEmergencyCds.getDepartmentType();
        if (!Strings.isNullOrEmpty(aeDepartmentType)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "department_type"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "department_type", aeDepartmentType,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = valueResponse.getConcept().getIri();
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        String aeArrivalMode = targetEmergencyCds.getArrivalMode();
        if (!Strings.isNullOrEmpty(aeArrivalMode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "arrival_mode"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = "SM_".concat(aeArrivalMode);  //NOTE: a Snomed code so no IM lookup
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        //TODO: check filed as observation?
//        String chiefComplaint = targetEmergencyCds.getChiefComplaint();
//        if (!Strings.isNullOrEmpty(chiefComplaint)) {
//
//            //value SN_{code}
//            parametersBuilder.addParameter("ae_chief_complaint", "" + chiefComplaint);
//        }


        String treatmentFunctionCode = targetEmergencyCds.getTreatmentFunctionCode();
        if (!Strings.isNullOrEmpty(treatmentFunctionCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "treatment_function_code"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "treatment_function_code", treatmentFunctionCode,"CM_BartCernerCode"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = valueResponse.getConcept().getIri();
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }
    }

    private static void setConclusionContainedParameters(EncounterBuilder encounterBuilder,
                                                        StagingEmergencyCdsTarget targetEmergencyCds) throws Exception {

        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();

        String dischargeDestinationCode = targetEmergencyCds.getDischargeDestination();
        if (!Strings.isNullOrEmpty(dischargeDestinationCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","emergency",
                    "discharge_destination"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = "SM_".concat(dischargeDestinationCode);  //NOTE: a Snomed code so no IM value lookup
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        //TODO - are these being mapped as additional?
//        String dischargeStatusCode = targetEmergencyCds.getDischargeStatus();
//        if (!Strings.isNullOrEmpty(dischargeStatusCode)) {
//            parametersBuilder.addParameter("ae_discharge_status", "" + dischargeStatusCode);
//        }
//        String dischargeFollowUp = targetEmergencyCds.getDischargeFollowUp();
//        if (!Strings.isNullOrEmpty(dischargeFollowUp)) {
//            parametersBuilder.addParameter("ae_discharge_follow_up", "" + dischargeFollowUp);
//        }
    }
}