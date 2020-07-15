package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingInpatientCdsTarget;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
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

            //check if any patient filtering applied
            String personId = Integer.toString(targetInpatientCds.getPersonId());
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                continue;
            }

            boolean isDeleted = targetInpatientCds.isDeleted();
            if (isDeleted) {

                deleteInpatientCdsEncounterAndChildren(targetInpatientCds, fhirResourceFiler, csvHelper);
                continue;
            }

            //process top level encounter - the existing parent encounter
            Integer encounterId = targetInpatientCds.getEncounterId();  //this is used to identify the top level parent encounter

            if (encounterId != null) {

                EncounterBuilder parentEncounterBuilder = null;

                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //update the existing top level encounter
                    updateExistingParentEncounter(existingParentEncounter, targetInpatientCds, fhirResourceFiler, csvHelper);

                    //create the linked child encounters
                    parentEncounterBuilder = new EncounterBuilder(existingParentEncounter, targetInpatientCds.getAudit());
                    createInpatientCdsSubEncounters(targetInpatientCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

                } else {

                    //create top level parent with minimum data including the sub encounters
                    parentEncounterBuilder
                            = createInpatientCdsEncounterParentAndSubs(targetInpatientCds, fhirResourceFiler, csvHelper);
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
                deleteHL7ReceiverPatientInpatientEncounters(patientUuid, fhirResourceFiler, csvHelper);

            } else {

                String uniqueId = targetInpatientCds.getUniqueId();
                //throw new Exception("encounter_id missing for Inpatient CDS record: " + uniqueId);
                LOG.warn("encounter_id missing for Inpatient CDS record: " + uniqueId);
            }
        }
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     StagingInpatientCdsTarget targetInpatientCds,
                                                     BartsCsvHelper csvHelper,
                                                     boolean isChildEncounter,
                                                     FhirResourceFiler fhirResourceFiler) throws Exception {

        //every encounter has the following common attributes
        Integer personId = targetInpatientCds.getPersonId();
        if (personId != null) {
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

            EpisodeOfCareBuilder episodeOfCareBuilder = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(targetInpatientCds);
            if (episodeOfCareBuilder != null) {

                csvHelper.setEpisodeReferenceOnEncounter(episodeOfCareBuilder, builder, fhirResourceFiler);

                Date spellStartDate = targetInpatientCds.getDtSpellStart();
                Date dischargeDate = targetInpatientCds.getDtDischarge();
                if (spellStartDate != null) {

                    //if this episode started in A&E this will not be updated
                    if (episodeOfCareBuilder.getRegistrationStartDate() == null || spellStartDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {
                        episodeOfCareBuilder.setRegistrationStartDate(spellStartDate);
                        episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
                    }

                    // End date
                    if (dischargeDate != null) {
                        if (episodeOfCareBuilder.getRegistrationEndDate() == null || dischargeDate.after(episodeOfCareBuilder.getRegistrationEndDate())) {
                            episodeOfCareBuilder.setRegistrationEndDate(dischargeDate);
                        }
                    }
                } else {
                    if (episodeOfCareBuilder.getRegistrationEndDate() == null) {
                        episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.PLANNED);
                    }
                }
            }
        }
        Integer performerPersonnelId = targetInpatientCds.getPerformerPersonnelId();
        if (performerPersonnelId != null) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId));
            if (builder.isIdMapped()) {

                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }
        String serviceProviderOrgId = targetInpatientCds.getEpisodeStartSiteCode();
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

            Integer encounterId = targetInpatientCds.getEncounterId();
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(encounterId));
            parentEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            builder.setPartOf(parentEncounter);
        }
        //set the CDS identifier against the Encounter
        String cdsUniqueId = targetInpatientCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("IPCDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(builder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(builder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }
    }

    private static void createInpatientCdsSubEncounters(StagingInpatientCdsTarget targetInpatientCds,
                                                        FhirResourceFiler fhirResourceFiler,
                                                        BartsCsvHelper csvHelper,
                                                        EncounterBuilder existingParentEncounterBuilder) throws Exception {

        //unique to the inpatient hospital spell
        String spellId = targetInpatientCds.getSpellNumber();

        //the episode number ranges from 01 (Admission plus episode start / finish), 02, 03 till discharge
        String episodeNumber = targetInpatientCds.getEpisodeNumber();

        //retrieve the parent encounter (if not passed in) to point to any new child encounters created during this method
        ContainedListBuilder existingParentEncounterList;
        if (existingParentEncounterBuilder == null) {

            Integer parentEncounterId = targetInpatientCds.getEncounterId();
            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(parentEncounterId));
            existingParentEncounterBuilder = new EncounterBuilder(existingParentEncounter);
        }
        existingParentEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

        EncounterBuilder admissionEncounterBuilder = null;
        EncounterBuilder dischargeEncounterBuilder = null;

        //episodeNumber = 01 then create the inpatient admission and the discharge encounters (if date set)
        if (episodeNumber.equalsIgnoreCase("01")) {

            admissionEncounterBuilder = new EncounterBuilder();
            admissionEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

            String admissionEncounterId = spellId + ":01:IP:Admission";
            admissionEncounterBuilder.setId(admissionEncounterId);
            Date spellStartDate = targetInpatientCds.getDtSpellStart();
            admissionEncounterBuilder.setPeriodStart(spellStartDate);
            admissionEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderAdmission
                    = new CodeableConceptBuilder(admissionEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAdmission.setText("Inpatient Admission");

            setCommonEncounterAttributes(admissionEncounterBuilder, targetInpatientCds, csvHelper, true, fhirResourceFiler);

            //add in additional extended data as Parameters resource with additional extension
            //TODO: set name and values using IM map once done, i.e. replace ip_admission_source etc.
            ContainedParametersBuilder containedParametersBuilderAdmission
                    = new ContainedParametersBuilder(admissionEncounterBuilder);
            containedParametersBuilderAdmission.removeContainedParameters();

            String adminCategoryCode = targetInpatientCds.getAdministrativeCategoryCode();
            if (!Strings.isNullOrEmpty(adminCategoryCode)) {
                containedParametersBuilderAdmission.addParameter("administrative_category_code", adminCategoryCode);
            }
            String admissionMethodCode = targetInpatientCds.getAdmissionMethodCode();
            if (!Strings.isNullOrEmpty(admissionMethodCode)) {
                containedParametersBuilderAdmission.addParameter("ip_admission_method",  admissionMethodCode);
            }
            String admissionSourceCode = targetInpatientCds.getAdmissionSourceCode();
            if (!Strings.isNullOrEmpty(admissionSourceCode)) {
                containedParametersBuilderAdmission.addParameter("ip_admission_source", admissionSourceCode);
            }
            String patientClassification = targetInpatientCds.getPatientClassification();
            if (!Strings.isNullOrEmpty(patientClassification)) {
                containedParametersBuilderAdmission.addParameter("patient_classification", patientClassification);
            }
            //this is a Cerner code which is mapped to an NHS DD alias
            String treatmentFunctionCode = targetInpatientCds.getTreatmentFunctionCode();
            if (!Strings.isNullOrEmpty(treatmentFunctionCode)) {
                //CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.TREATMENT_FUNCTION, treatmentFunctionCode);
                //if (codeRef != null) {

                    //String treatmentFunctionCodeNHSAliasCode = codeRef.getAliasNhsCdAlias();
                    //containedParametersBuilderAdmission.addParameter("treatment_function", "" + treatmentFunctionCodeNHSAliasCode);

                //todo - codeableconcept etc. -> IM API
                containedParametersBuilderAdmission.addParameter("treatment_function", treatmentFunctionCode);
                //}
            }

            //add the primary and secondary diagnosis codes as additional parameters. Note: these are also filed
            //as part of the diagnosis CDS transform as separate diagnosis records
            String primaryDiagnosis = targetInpatientCds.getPrimaryDiagnosisICD();
            if (!Strings.isNullOrEmpty(primaryDiagnosis)) {
                containedParametersBuilderAdmission.addParameter("primary_diagnosis", primaryDiagnosis);
            }
            String secondaryDiagnosis = targetInpatientCds.getSecondaryDiagnosisICD();
            if (!Strings.isNullOrEmpty(secondaryDiagnosis)) {
                containedParametersBuilderAdmission.addParameter("secondary_diagnosis", secondaryDiagnosis);
            }

            String otherDiagnosis = targetInpatientCds.getOtherDiagnosisICD();

            //if the 01 episode has an episode start date, set the admission status to finished and end date
            Date episodeStartDate = targetInpatientCds.getDtEpisodeStart();
            if (episodeStartDate != null) {

                admissionEncounterBuilder.setPeriodEnd(episodeStartDate);
                admissionEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }

            //and link the parent to this new child encounter
            Reference childAdmissionRef = ReferenceHelper.createReference(ResourceType.Encounter, admissionEncounterId);
            if (existingParentEncounterBuilder.isIdMapped()) {

                childAdmissionRef
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAdmissionRef, csvHelper);
            }
            existingParentEncounterList.addReference(childAdmissionRef);

            //the main encounter has a discharge date so set the end date and create a linked Discharge encounter
            Date spellDischargeDate = targetInpatientCds.getDtDischarge();
            if (spellDischargeDate != null) {

                //create new additional Discharge encounter event to link to the top level parent
                dischargeEncounterBuilder = new EncounterBuilder();
                dischargeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

                String dischargeEncounterId = spellId + ":01:IP:Discharge";
                dischargeEncounterBuilder.setId(dischargeEncounterId);
                dischargeEncounterBuilder.setPeriodStart(spellDischargeDate);
                dischargeEncounterBuilder.setPeriodEnd(spellDischargeDate);
                dischargeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

                CodeableConceptBuilder codeableConceptBuilderDischarge
                        = new CodeableConceptBuilder(dischargeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                codeableConceptBuilderDischarge.setText("Inpatient Discharge");

                setCommonEncounterAttributes(dischargeEncounterBuilder, targetInpatientCds, csvHelper, true, fhirResourceFiler);

                //add in additional extended data as Parameters resource with additional extension
                //TODO: set name and values using IM map once done, i.e. replace ip_discharge_method etc.
                ContainedParametersBuilder containedParametersBuilderDischarge
                        = new ContainedParametersBuilder(dischargeEncounterBuilder);
                containedParametersBuilderDischarge.removeContainedParameters();

                String dischargeMethodCode = targetInpatientCds.getDischargeMethod();
                if (!Strings.isNullOrEmpty(dischargeMethodCode)) {
                    containedParametersBuilderDischarge.addParameter("ip_discharge_method", "" + dischargeMethodCode);
                }
                String dischargeDestinationCode = targetInpatientCds.getDischargeDestinationCode();
                if (!Strings.isNullOrEmpty(dischargeDestinationCode)) {
                    containedParametersBuilderDischarge.addParameter("ip_discharge_destination", "" + dischargeDestinationCode);
                }

                //and link the parent to this new child encounter
                Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, dischargeEncounterId);
                if (existingParentEncounterBuilder.isIdMapped()) {

                    childDischargeRef
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAdmissionRef, csvHelper);
                }

                existingParentEncounterList.addReference(childDischargeRef);
            }
        }

        //these are the 01, 02, 03, 04 subsequent episodes where activity happens, maternity, wards change etc.
        //also, critical care child encounters link back to these as their parents via their own transform
        EncounterBuilder episodeEncounterBuilder = new EncounterBuilder();
        episodeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        String episodeEncounterId = spellId +":"+episodeNumber+":IP:Episode";
        episodeEncounterBuilder.setId(episodeEncounterId);

        //spell episode encounter have their own start and end date/times
        Date episodeStartDate = targetInpatientCds.getDtEpisodeStart();
        episodeEncounterBuilder.setPeriodStart(episodeStartDate);

        Date episodeEndDate = targetInpatientCds.getDtEpisodeEnd();
        if (episodeEndDate != null) {

            episodeEncounterBuilder.setPeriodEnd(episodeEndDate);
            episodeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            episodeEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(episodeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient Episode");

        setCommonEncounterAttributes(episodeEncounterBuilder, targetInpatientCds, csvHelper, true, fhirResourceFiler);

        //and link the parent to this new child encounter
        Reference childEpisodeRef = ReferenceHelper.createReference(ResourceType.Encounter, episodeEncounterId);
        if (existingParentEncounterBuilder.isIdMapped()) {

            childEpisodeRef
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(childEpisodeRef, csvHelper);
        }
        existingParentEncounterList.addReference(childEpisodeRef);

        //add in additional extended data as Parameters resource with additional extension
        //TODO: set name and values using IM map once done - ward start and end?
        ContainedParametersBuilder containedParametersBuilder
                = new ContainedParametersBuilder(episodeEncounterBuilder);
        containedParametersBuilder.removeContainedParameters();

        String episodeStartWardCode = targetInpatientCds.getEpisodeStartWardCode();
        if (!Strings.isNullOrEmpty(episodeStartWardCode)) {
            containedParametersBuilder.addParameter("ip_episode_start_ward", "" + episodeStartWardCode);
        }
        String episodeEndWardCode = targetInpatientCds.getEpisodeEndWardCode();
        if (!Strings.isNullOrEmpty(episodeEndWardCode)) {
            containedParametersBuilder.addParameter("ip_episode_end_ward", "" + episodeEndWardCode);
        }

        //TODO:  procedures associated with episode encounters - already in via specific transforms?
        // targetInpatientCds.getPrimaryProcedureOPCS());
        // targetInpatientCds.getSecondaryProcedureOPCS());
        // targetInpatientCds.getOtherProceduresOPCS());

        //TODO: mothers NHS number linking from birth records at subscriber
        String maternityBirth = targetInpatientCds.getMaternityDataBirth();
        //the encounter is about the baby and contains the mothers nhs number
        if (!Strings.isNullOrEmpty(maternityBirth)) {
            containedParametersBuilder.addParameter("maternity_birth", maternityBirth);
        }

        String maternityDelivery = targetInpatientCds.getMaternityDataDelivery();
        //the encounter is about the mother and this is the birth(s) detail
        if (!Strings.isNullOrEmpty(maternityDelivery)) {
            containedParametersBuilder.addParameter("maternity_delivery", maternityBirth);
        }

        //save the existing parent encounter here with the updated child refs added during this method, then the sub encounters
        //LOG.debug("Saving IP parent encounter: "+ FhirSerializationHelper.serializeResource(existingParentEpisodeBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);

        //then save the child encounter builders if they are set
        if (admissionEncounterBuilder != null) {

            //LOG.debug("Saving child IP admission encounter: "+ FhirSerializationHelper.serializeResource(admissionEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, admissionEncounterBuilder);
        }
        if (dischargeEncounterBuilder != null) {

            //LOG.debug("Saving child IP discharge encounter: "+ FhirSerializationHelper.serializeResource(dischargeEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, dischargeEncounterBuilder);
        }
        //finally, save the episode encounter which always exists
        //LOG.debug("Saving child IP episode encounter: "+ FhirSerializationHelper.serializeResource(episodeEncounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, episodeEncounterBuilder);
    }

    private static void deleteInpatientCdsEncounterAndChildren(StagingInpatientCdsTarget targetInpatientCds,
                                                                FhirResourceFiler fhirResourceFiler,
                                                                BartsCsvHelper csvHelper) throws Exception {

        Integer encounterId = targetInpatientCds.getEncounterId();  //this is used to identify the top level parent episode

        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        if (encounterId != null) {

            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));

            if (existingParentEncounter != null) {

                EncounterBuilder parentEncounterBuilder
                        = new EncounterBuilder(existingParentEncounter, targetInpatientCds.getAudit());

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

    private static EncounterBuilder createInpatientCdsEncounterParentAndSubs(StagingInpatientCdsTarget targetInpatientCds,
                                                                    FhirResourceFiler fhirResourceFiler,
                                                                    BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentEncounterBuilder = new EncounterBuilder();
        parentEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        Integer encounterId = targetInpatientCds.getEncounterId();
        parentEncounterBuilder.setId(Integer.toString(encounterId));

        Date spellStartDate = targetInpatientCds.getDtSpellStart();
        parentEncounterBuilder.setPeriodStart(spellStartDate);

        Date dischargeDate = targetInpatientCds.getDtDischarge();
        if (dischargeDate != null) {

            parentEncounterBuilder.setPeriodEnd(dischargeDate);
            parentEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            parentEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient");

        setCommonEncounterAttributes(parentEncounterBuilder, targetInpatientCds, csvHelper, false, fhirResourceFiler);

        //once the parent is populated, then create the sub encounters, passing through the parent for linking
        createInpatientCdsSubEncounters(targetInpatientCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

        return parentEncounterBuilder;
    }

    private static void updateExistingParentEncounter(Encounter existingEncounter,
                                                StagingInpatientCdsTarget targetInpatientCds,
                                                FhirResourceFiler fhirResourceFiler,
                                                BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder existingEncounterBuilder
                = new EncounterBuilder(existingEncounter, targetInpatientCds.getAudit());

        //todo - decide on how much to update the top level with
        Date dischargeDate = targetInpatientCds.getDtDischarge();
        if (dischargeDate != null) {

            existingEncounterBuilder.setPeriodEnd(dischargeDate);
            existingEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            existingEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        String cdsUniqueId = targetInpatientCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("IPCDS-","");
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
    private static void deleteHL7ReceiverPatientInpatientEncounters(String patientUuid,
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

                //finally, check it is an Outpatient encounter class before deleting
                if (existingEncounter.getClass_().equals(Encounter.EncounterClass.INPATIENT)) {
                    GenericBuilder builder = new GenericBuilder(existingEncounter);
                    //we have no audit for deleting these encounters, since it's not triggered by a specific piece of data
                    //builder.setDeletedAudit(...);
                    fhirResourceFiler.deletePatientResource(null, false, builder);
                }
            }
        }
    }
}