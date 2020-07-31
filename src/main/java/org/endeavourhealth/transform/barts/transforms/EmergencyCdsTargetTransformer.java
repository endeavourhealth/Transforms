package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.apache.commons.lang3.ObjectUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientSearchDalI;
import org.endeavourhealth.core.database.dal.eds.models.PatientSearch;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingEmergencyCdsTarget;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
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

import java.util.*;

public class EmergencyCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EmergencyCdsTargetTransformer.class);
    private static Set<Long> patientEmergencyEncounterDates = new HashSet<>();
    private static Set<String> patientEmergencyEpisodesDeleted = new HashSet<>();

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

            patientEmergencyEncounterDates.clear();
            patientEmergencyEpisodesDeleted.clear();

            //process top level encounter - the existing parent encounter
            Integer encounterId = targetEmergencyCds.getEncounterId();  //this is used to identify the top level parent encounter

            if (encounterId != null) {

                EncounterBuilder parentEncounterBuilder = null;

                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //update the existing top level encounter
                    parentEncounterBuilder
                            = updateExistingParentEncounter(existingParentEncounter, targetEmergencyCds, fhirResourceFiler, csvHelper);

                    //create the linked child encounters
                    createEmergencyCdsEncounters(targetEmergencyCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

                } else {

                    //create top level parent with minimum data and the sub encounters
                    parentEncounterBuilder
                            = createEmergencyCdsEncounterParentAndSubs(targetEmergencyCds, fhirResourceFiler, csvHelper);
                }
                //now process and linked clinical event data
                fhirResourceFiler.waitUntilEverythingIsSaved();
                createEmergencyCdsEncounterClinicalEvents(targetEmergencyCds, fhirResourceFiler);

                //now delete any older HL7 Encounters for patients we've updated
                //but waiting until everything has been saved to the DB first
                fhirResourceFiler.waitUntilEverythingIsSaved();

                //find the patient UUID for the encounters we have just filed, so we can tidy up the
                //HL7 encounters after doing all the saving of the DW encounters
                deleteHL7ReceiverPatientEmergencyEncounters(targetEmergencyCds, fhirResourceFiler, csvHelper);
                patientEmergencyEncounterDates.clear();
                patientEmergencyEpisodesDeleted.clear();

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
        patientEmergencyEncounterDates.add(existingParentEncounterBuilder.getPeriod().getStart().getTime());
        //save the A&E arrival encounter - always created
        //LOG.debug("Saving child arrival EM encounter: "+ FhirSerializationHelper.serializeResource(arrivalEncounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, arrivalEncounterBuilder);
        patientEmergencyEncounterDates.add(arrivalEncounterBuilder.getPeriod().getStart().getTime());

        //save the A&E assessment encounter
        if (assessmentEncounterBuilder != null) {
            //LOG.debug("Saving child assessment EM encounter: "+ FhirSerializationHelper.serializeResource(assessmentEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, assessmentEncounterBuilder);
            patientEmergencyEncounterDates.add(assessmentEncounterBuilder.getPeriod().getStart().getTime());
        }
        //save the A&E treatments encounter
        if (treatmentsEncounterBuilder != null) {
            //LOG.debug("Saving child treatments EM encounter: "+ FhirSerializationHelper.serializeResource(treatmentsEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, treatmentsEncounterBuilder);
            patientEmergencyEncounterDates.add(treatmentsEncounterBuilder.getPeriod().getStart().getTime());
        }
        //save the A&E discharge encounter
        if (conclusionEncounterBuilder != null) {
            //LOG.debug("Saving child discharge EM encounter: "+ FhirSerializationHelper.serializeResource(dischargeEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, conclusionEncounterBuilder);
            patientEmergencyEncounterDates.add(conclusionEncounterBuilder.getPeriod().getStart().getTime());
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
                                                     FhirResourceFiler fhirResourceFiler) throws Exception {

        //every encounter has the following common attributes
        Integer personId = targetEmergencyCds.getPersonId();
        if (personId != null) {
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            if (builder.isIdMapped()) {

                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }
            builder.setPatient(patientReference);
        }
        // Retrieve or create EpisodeOfCare link for encounter including sub-encounters
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
                    && decidedToAdmitDate == null) {

                episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
            }
        }

        Integer performerPersonnelId = targetEmergencyCds.getPerformerPersonnelId();
        if (performerPersonnelId != null && performerPersonnelId != 0) {

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

            cdsUniqueId = cdsUniqueId.replaceFirst("ECDS-", "");
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

    private static EncounterBuilder updateExistingParentEncounter(Encounter existingEncounter,
                                                                  StagingEmergencyCdsTarget targetEmergencyCds,
                                                                  FhirResourceFiler fhirResourceFiler,
                                                                  BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder existingEncounterBuilder
                = new EncounterBuilder(existingEncounter, targetEmergencyCds.getAudit());

        //set the overall encounter status depending on sub encounter completion
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

            cdsUniqueId = cdsUniqueId.replaceFirst("ECDS-", "");
            IdentifierBuilder.removeExistingIdentifiersForSystem(existingEncounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(existingEncounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }

        return existingEncounterBuilder;
    }

    /*
     * we match to some HL7 Receiver Encounters, basically taking them over
     * so we call this to tidy up (delete) any matching Encounters that have been taken over
     */
    private static void deleteHL7ReceiverPatientEmergencyEncounters(StagingEmergencyCdsTarget targetEmergencyCds,
                                                                    FhirResourceFiler fhirResourceFiler,
                                                                    BartsCsvHelper csvHelper) throws Exception {

        UUID serviceUuid = fhirResourceFiler.getServiceId();
        UUID systemUuid = fhirResourceFiler.getSystemId();

        //we want to delete HL7 Emergency Encounters more than 24 hours older than the extract data date
        Date extractDateTime = fhirResourceFiler.getDataDate();
        Date cutoff = new Date(extractDateTime.getTime() - (12 * 60 * 60 * 1000));

        String sourcePatientId = Integer.toString(targetEmergencyCds.getPersonId());
        UUID patientUuid = IdHelper.getEdsResourceId(serviceUuid, ResourceType.Patient, sourcePatientId);

        //try to locate the patient to obtain the nhs number
        PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();
        PatientSearch patientSearch = patientSearchDal.searchByPatientId(patientUuid);
        if (patientSearch == null) {
            LOG.warn("Cannot find patient using Id: " + patientUuid.toString());
            return;
        }
        String nhsNumber = patientSearch.getNhsNumber();
        Set<String> serviceIds = new HashSet<>();
        serviceIds.add(serviceUuid.toString());
        //get the list of patientId values for this service as Map<patientId, serviceId>
        Map<UUID, UUID> patientIdsForService = patientSearchDal.findPatientIdsForNhsNumber(serviceIds, nhsNumber);
        Set<UUID> patientIds = patientIdsForService.keySet();   //get the unique patientId values, >1 where >1 system

        //loop through all the patientIds for that patient to check the encounters
        for (UUID patientId : patientIds) {

            //LOG.debug("Checking patient: " + patientId.toString() + " for existing service: " + serviceUuid.toString() + " encounters");

            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> resourceWrappers
                    = resourceDal.getResourcesByPatient(serviceUuid, patientId, ResourceType.Encounter.toString());
            for (ResourceWrapper wrapper : resourceWrappers) {

                //if this Encounter is for our own service + system ID (i.e. DW feed), then leave it
                UUID wrapperSystemId = wrapper.getSystemId();
                if (wrapperSystemId.equals(systemUuid)) {
                    continue;
                }

                String json = wrapper.getResourceData();
                Encounter existingEncounter = (Encounter) FhirSerializationHelper.deserializeResource(json);

                //LOG.debug("Existing HL7 Emergency encounter " + existingEncounter.getId() + ", date: " + existingEncounter.getPeriod().getStart().toString() + ", cut off date: " + cutoff.toString());

                //if the HL7 Encounter is before our 24 hr cutoff, look to delete it
                if (existingEncounter.hasPeriod()
                        && existingEncounter.getPeriod().hasStart()
                        && existingEncounter.getPeriod().getStart().before(cutoff)) {

                    //finally, check it is an Emergency encounter and has a matching start date to one just filed before deleting
                    if (existingEncounter.getClass_().equals(Encounter.EncounterClass.EMERGENCY)) {

                        //LOG.debug("Checking existing Emergency encounter date (long): " + existingEncounter.getPeriod().getStart().getTime() + " in dates array: " + patientEmergencyEncounterDates.toArray());

                        if (patientEmergencyEncounterDates.contains(existingEncounter.getPeriod().getStart().getTime())) {

                            GenericBuilder builder = new GenericBuilder(existingEncounter);
                            //we have no audit for deleting these encounters, since it's not triggered by a specific piece of data
                            //builder.setDeletedAudit(...);
                            LOG.debug("Existing Emergency ADT encounterId: " + existingEncounter.getId() + " deleted as matched type and date to DW");
                            fhirResourceFiler.deletePatientResource(null, false, builder);

                            //get the linked episode of care reference and delete the resource so duplication does not occur between DW and ADT
                            if (existingEncounter.hasEpisodeOfCare()) {
                                Reference episodeReference = existingEncounter.getEpisodeOfCare().get(0);
                                String episodeUuid = ReferenceHelper.getReferenceId(episodeReference);

                                //add episode of care for deletion if not already deleted
                                if (patientEmergencyEpisodesDeleted.contains(episodeUuid)) {
                                    continue;
                                }
                                EpisodeOfCare episodeOfCare
                                        = (EpisodeOfCare) resourceDal.getCurrentVersionAsResource(serviceUuid, ResourceType.EpisodeOfCare, episodeUuid);

                                if (episodeOfCare != null) {
                                    GenericBuilder builderEpisode = new GenericBuilder(episodeOfCare);

                                    patientEmergencyEpisodesDeleted.add(episodeUuid);
                                    fhirResourceFiler.deletePatientResource(null, false, builderEpisode);
                                    LOG.debug("Existing Emergency ADT episodeId: " + episodeUuid + " deleted as linked to deleted encounter");
                                }
                            }
                        }
                    }
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
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "attendance_category"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "attendance_category", aeAttendanceCategoryCode, "CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String aeAttendanceSource = targetEmergencyCds.getAttendanceSource();
        if (!Strings.isNullOrEmpty(aeAttendanceSource)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "attendance_source"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(aeAttendanceSource)
                    .setSystem(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String aeDepartmentType = targetEmergencyCds.getDepartmentType();
        if (!Strings.isNullOrEmpty(aeDepartmentType)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "department_type"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "department_type", aeDepartmentType, "CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String aeArrivalMode = targetEmergencyCds.getArrivalMode();
        if (!Strings.isNullOrEmpty(aeArrivalMode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "arrival_mode"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(aeArrivalMode)
                    .setSystem(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String treatmentFunctionCode = targetEmergencyCds.getTreatmentFunctionCode();
        if (!Strings.isNullOrEmpty(treatmentFunctionCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "treatment_function_code"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "treatment_function_code", treatmentFunctionCode, "BartsCerner"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }
    }

    private static void setConclusionContainedParameters(EncounterBuilder encounterBuilder,
                                                         StagingEmergencyCdsTarget targetEmergencyCds) throws Exception {

        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();

        String dischargeDestinationCode = targetEmergencyCds.getDischargeDestination();
        if (!Strings.isNullOrEmpty(dischargeDestinationCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "discharge_destination"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(dischargeDestinationCode)
                    .setSystem(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        //TODO: These might not be IM mapped yet, so check auto create
        String dischargeStatusCode = targetEmergencyCds.getDischargeStatus();
        if (!Strings.isNullOrEmpty(dischargeStatusCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "discharge_status"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(dischargeStatusCode)
                    .setSystem(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String dischargeFollowUp = targetEmergencyCds.getDischargeFollowUp();
        if (!Strings.isNullOrEmpty(dischargeFollowUp)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "emergency",
                    "follow_up"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(dischargeFollowUp)
                    .setSystem(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }
    }

    /*
        The EmergencyCDS dataset contains clinical events which are not handled in other transforms so
        need filing as part of this data transformation
     */
    private static void createEmergencyCdsEncounterClinicalEvents(StagingEmergencyCdsTarget targetEmergencyCds,
                                                                  FhirResourceFiler fhirResourceFiler) throws Exception {

        //Chief complaint is a Snomed coded problem type condition resource
        String chiefComplaint = targetEmergencyCds.getChiefComplaint();
        if (!Strings.isNullOrEmpty(chiefComplaint)) {

            //create the id using the uniqueId for the encounter plus text
            String uniqueId = targetEmergencyCds.getUniqueId()+"chief_complaint";

            // create the FHIR Condition resource
            ConditionBuilder conditionComplaintBuilder
                    = new ConditionBuilder(null, targetEmergencyCds.getAudit());
            conditionComplaintBuilder.setId(uniqueId);

            //arrival date at A&E
            if (targetEmergencyCds.getDtArrival()!= null) {

                DateTimeType conditionDateTime = new DateTimeType(targetEmergencyCds.getDtArrival());
                conditionComplaintBuilder.setOnset(conditionDateTime);
            }

            // set the patient reference
            Integer personId = targetEmergencyCds.getPersonId();
            if (personId != null) {

                Reference patientReference
                        = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
                conditionComplaintBuilder.setPatient(patientReference);
            }

            //Set the encounter reference as the top level encounter Id
            Integer encounterId = targetEmergencyCds.getEncounterId();
            if (encounterId != null) {

                Reference encounterReference
                        = ReferenceHelper.createReference(ResourceType.Encounter, "" + encounterId);
                conditionComplaintBuilder.setEncounter(encounterReference);
            }

            //a complaint is a problem, so set as true
            boolean isProblem = true;
            conditionComplaintBuilder.setAsProblem(isProblem);
            conditionComplaintBuilder.setCategory("complaint");

            //these are confirmed complaints/problems
            conditionComplaintBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

            //the complaint is active at this time
            conditionComplaintBuilder.setEndDateOrBoolean(null);

            // clinician is the A&E practitioner
            Integer performerPersonnelId = targetEmergencyCds.getPerformerPersonnelId();
            if (performerPersonnelId != null && performerPersonnelId != 0) {
                Reference practitionerPerformerReference
                        = ReferenceHelper.createReference(ResourceType.Practitioner, String.valueOf(performerPersonnelId));
                conditionComplaintBuilder.setClinician(practitionerPerformerReference);
            }

            // coded concept - these are Snomed coded
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionComplaintBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(chiefComplaint);

            //only the code is supplied so perform a lookup to derive the term
            String complaintTerm = TerminologyService.lookupSnomedTerm(chiefComplaint);
            if (Strings.isNullOrEmpty(complaintTerm)) {
                throw new Exception("Failed to find term for Snomed code " + chiefComplaint);
            }
            codeableConceptBuilder.setCodingDisplay(complaintTerm);
            codeableConceptBuilder.setText(complaintTerm);

            //file the chief complaint condition
            fhirResourceFiler.savePatientResource(null, conditionComplaintBuilder);
        }

        //Diagnosis records are Snomed coded confirmed conditions separated by |
        String diagnosisRecords = targetEmergencyCds.getDiagnosis();
        if (!Strings.isNullOrEmpty(diagnosisRecords)) {

            String[] records = diagnosisRecords.split("\\|");
            int count = 0;
            for (String diagnosisCode : records) {

                count++;

                //create the id using the uniqueId for the encounter plus text
                String uniqueId = targetEmergencyCds.getUniqueId()+"diagnosis"+count;

                // create the FHIR Condition resource
                ConditionBuilder conditionDiagnosisBuilder
                        = new ConditionBuilder(null, targetEmergencyCds.getAudit());
                conditionDiagnosisBuilder.setId(uniqueId);

                Date assessmentDate = targetEmergencyCds.getDtInitialAssessment();
                Date invAndTreatmentsDate = targetEmergencyCds.getDtSeenForTreatment();
                Date admitDate = targetEmergencyCds.getDtDecidedToAdmit();
                Date arrivalDate = targetEmergencyCds.getDtArrival();
                Date diagnosisDate
                        = ObjectUtils.firstNonNull(assessmentDate, invAndTreatmentsDate, admitDate, arrivalDate);
                //the diagnosis date and time is the first one of these not null
                if (diagnosisDate != null) {

                    DateTimeType conditionDateTime = new DateTimeType(diagnosisDate);
                    conditionDiagnosisBuilder.setOnset(conditionDateTime);
                }

                // set the patient reference
                Integer personId = targetEmergencyCds.getPersonId();
                if (personId != null) {

                    Reference patientReference
                            = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
                    conditionDiagnosisBuilder.setPatient(patientReference);
                }

                //Set the encounter reference as the top level encounter Id
                Integer encounterId = targetEmergencyCds.getEncounterId();
                if (encounterId != null) {

                    Reference encounterReference
                            = ReferenceHelper.createReference(ResourceType.Encounter, "" + encounterId);
                    conditionDiagnosisBuilder.setEncounter(encounterReference);
                }

                //a diganosis is not a problem, so set as false
                boolean isProblem = false;
                conditionDiagnosisBuilder.setAsProblem(isProblem);
                conditionDiagnosisBuilder.setCategory("diagnosis");

                //these are confirmed diagnosis from pre-transform
                conditionDiagnosisBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

                //the diagnosis is active at this time
                conditionDiagnosisBuilder.setEndDateOrBoolean(null);

                //clinician is the A&E practitioner
                Integer performerPersonnelId = targetEmergencyCds.getPerformerPersonnelId();
                if (performerPersonnelId != null && performerPersonnelId != 0) {
                    Reference practitionerPerformerReference
                            = ReferenceHelper.createReference(ResourceType.Practitioner, String.valueOf(performerPersonnelId));
                    conditionDiagnosisBuilder.setClinician(practitionerPerformerReference);
                }

                // coded concept - these are Snomed coded
                CodeableConceptBuilder codeableConceptBuilder
                        = new CodeableConceptBuilder(conditionDiagnosisBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                codeableConceptBuilder.setCodingCode(diagnosisCode);

                //only the code is supplied so perform a lookup to derive the term
                String diagnosisTerm = TerminologyService.lookupSnomedTerm(diagnosisCode);
                if (Strings.isNullOrEmpty(diagnosisCode)) {
                    throw new Exception("Failed to find term for Snomed code " + diagnosisCode);
                }
                codeableConceptBuilder.setCodingDisplay(diagnosisTerm);
                codeableConceptBuilder.setText(diagnosisTerm);

                //file the diagnosis condition
                fhirResourceFiler.savePatientResource(null, conditionDiagnosisBuilder);
            }
        }
    }
}