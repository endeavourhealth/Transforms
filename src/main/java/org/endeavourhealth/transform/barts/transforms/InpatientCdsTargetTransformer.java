package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingInpatientCdsTarget;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedParametersBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

            String uniqueId = targetInpatientCds.getUniqueId();         //this is the uniqueId for the entire CDS encounter record
            Integer encounterId = targetInpatientCds.getEncounterId();  //this is used to identify the top level parent episode

            boolean isDeleted = targetInpatientCds.isDeleted();
            if (isDeleted) {

                // retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));

                if (existingParentEncounter != null) {

                    EncounterBuilder parentEncounterBuilder
                            = new EncounterBuilder(existingParentEncounter, targetInpatientCds.getAudit());

                    //has this encounter got child encounters
                    if (existingParentEncounter.hasContained()) {

                        ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
                        ResourceDalI resourceDal = DalProvider.factoryResourceDal();

                        for (List_.ListEntryComponent item: listBuilder.getContainedListItems()) {
                            Reference ref = item.getItem();
                            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                            if (comps.getResourceType() != ResourceType.Encounter) {
                                throw new Exception("Expecting only Encounter references in parent Encounter");
                            }
                            Encounter childEncounter
                                    = (Encounter)resourceDal.getCurrentVersionAsResource(csvHelper.getServiceId(), ResourceType.Encounter, comps.getId());
                            if (childEncounter != null) {
                                LOG.debug("Deleting child encounter " + childEncounter.getId());

                                fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                            }
                        }
                    }

                    //delete the top level parent
                    fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

                } else {
                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", encounterId);
                }

                continue;
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder();
            encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

            String spellId = targetInpatientCds.getSpellNumber();
            String topLevelEncounterId = spellId + ":01:IP";

            //every encounter shares the following four attributes ////////////////////////////////
            Integer personId = targetInpatientCds.getPersonId();
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            encounterBuilder.setPatient(patientReference);

            Integer episodeId = targetInpatientCds.getEpisodeId();
            if (episodeId != null) {

                encounterBuilder.setEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeId.toString()));
            }
            Integer performerPersonnelId = targetInpatientCds.getPerformerPersonnelId();
            if (performerPersonnelId != null) {

                encounterBuilder.setRecordedBy(ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId)));
            }
            String serviceProviderOrgId = targetInpatientCds.getEpisodeStartSiteCode();
            if (!Strings.isNullOrEmpty(serviceProviderOrgId)) {

                encounterBuilder.setServiceProvider(ReferenceHelper.createReference(ResourceType.Organization, serviceProviderOrgId));
            }
            ///////////////////////////////////////////////////////////////////////////////////////

            // NOTE: top level inpatient encounter is only created / updated when episodeNumber = 01 to prevent duplicates
            String episodeNumber = targetInpatientCds.getEpisodeNumber();
            if (episodeNumber.equalsIgnoreCase("01")) {

                encounterBuilder.setId(topLevelEncounterId);
                Date spellStartDate = targetInpatientCds.getDtSpellStart();
                encounterBuilder.setPeriodStart(spellStartDate);

                //get the existing parent encounter set during ADT feed, to link to this top level encounter
                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //TODO what do we do with this for inpatients other that set as the parent?
                    Reference parentEncounter
                            = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(encounterId));
                    encounterBuilder.setPartOf(parentEncounter);

                } else {

                    //TODO what do we do if the parent ENCNTR_ID encounter does not exist, create it with minimum data?
                }

                //add in additional extended data as Parameters resource with additional extension
                //TODO: set name and values using IM map once done
                ContainedParametersBuilder containedParametersBuilderMain
                        = new ContainedParametersBuilder(encounterBuilder);
                containedParametersBuilderMain.removeContainedParameters();

                String adminCategoryCode = targetInpatientCds.getAdministrativeCategoryCode();
                containedParametersBuilderMain.addParameter("DM_hasAdministrativeCategoryCode", "CM_AdminCat" + adminCategoryCode);

                String admissionMethodCode = targetInpatientCds.getAdmissionMethodCode();
                containedParametersBuilderMain.addParameter("", "" + admissionMethodCode);

                String admissionSourceCode = targetInpatientCds.getAdmissionSourceCode();
                containedParametersBuilderMain.addParameter("", "" + admissionSourceCode);

                String patientClassification = targetInpatientCds.getPatientClassification();
                containedParametersBuilderMain.addParameter("", "" + patientClassification);

                //this is a Cerner code which is mapped to an NHS DD alias
                String treatmentFunctionCode = targetInpatientCds.getTreatmentFunctionCode();
                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.TREATMENT_FUNCTION, treatmentFunctionCode);
                if (codeRef != null) {

                    String treatmentFunctionCodeNHSAliasCode = codeRef.getAliasNhsCdAlias();
                    containedParametersBuilderMain.addParameter("", "" + treatmentFunctionCodeNHSAliasCode);
                }

                //the main encounter has a discharge date so set the end date and create a linked Discharge encounter
                if (targetInpatientCds.getDtDischarge() != null) {

                    //set the end date of the 01 encounter here
                    Date spellDischargeDate = targetInpatientCds.getDtDischarge();
                    encounterBuilder.setPeriodEnd(spellDischargeDate);

                    //create new additional Discharge encounter event to link to the top level parent
                    EncounterBuilder dischargeEncounterBuilder = new EncounterBuilder();

                    String dischargeEncounterId = spellId +":01:IP:D";
                    dischargeEncounterBuilder.setId(dischargeEncounterId);
                    dischargeEncounterBuilder.setPeriodStart(spellDischargeDate);

                    //this discharge encounter event is a child of the top level inpatient encounter
                    Reference parentEncounterReference
                            = ReferenceHelper.createReference(ResourceType.Encounter, topLevelEncounterId);
                    if (dischargeEncounterBuilder.isIdMapped()) {
                        parentEncounterReference
                                = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounterReference, csvHelper);
                    }
                    dischargeEncounterBuilder.setPartOf(parentEncounterReference);

                    dischargeEncounterBuilder.setPatient(patientReference);
                    if (episodeId != null) {
                        dischargeEncounterBuilder.setEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeId.toString()));
                    }
                    if (performerPersonnelId != null) {
                        dischargeEncounterBuilder.setRecordedBy(ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId)));
                    }
                    if (!Strings.isNullOrEmpty(serviceProviderOrgId)) {
                        dischargeEncounterBuilder.setServiceProvider(ReferenceHelper.createReference(ResourceType.Organization, serviceProviderOrgId));
                    }

                    //add in additional extended data as Parameters resource with additional extension
                    //TODO: set name and values using IM map once done
                    ContainedParametersBuilder containedParametersBuilderDischarge
                            = new ContainedParametersBuilder(dischargeEncounterBuilder);
                    containedParametersBuilderDischarge.removeContainedParameters();

                    String dischargeMethodCode = targetInpatientCds.getDischargeMethod();
                    containedParametersBuilderMain.addParameter("", "" + dischargeMethodCode);

                    String dischargeDestinationCode = targetInpatientCds.getDischargeDestinationCode();
                    containedParametersBuilderMain.addParameter("", "" + dischargeDestinationCode);

                    //save both encounter builders here
                    fhirResourceFiler.savePatientResource(null, encounterBuilder, dischargeEncounterBuilder);
                } else {

                    //save only the main encounter builder here
                    fhirResourceFiler.savePatientResource(null, encounterBuilder);
                }
            } else {

                //these are the 02, 03, 04 episodes where activity happens, maternity, wards change etc.
                //also, critical care child encounters link back to these as their parents
                String episodeEncounterId = spellId +":"+episodeNumber+":IP";
                encounterBuilder.setId(episodeEncounterId);

                //spell episode encounter have their own start and end date/times
                Date episodeStartDate = targetInpatientCds.getDtEpisodeStart();
                encounterBuilder.setPeriodStart(episodeStartDate);
                Date episodeEndDate = targetInpatientCds.getDtEpisodeEnd();
                encounterBuilder.setPeriodEnd(episodeEndDate);

                //these encounter events are children of the top level inpatient encounter
                Reference parentEncounterReference
                        = ReferenceHelper.createReference(ResourceType.Encounter, topLevelEncounterId);
                if (encounterBuilder.isIdMapped()) {
                    parentEncounterReference
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounterReference, csvHelper);
                }
                encounterBuilder.setPartOf(parentEncounterReference);

                //add in additional extended data as Parameters resource with additional extension
                //TODO: set name and values using IM map once done - ward start and end?
                ContainedParametersBuilder containedParametersBuilder
                        = new ContainedParametersBuilder(encounterBuilder);
                containedParametersBuilder.removeContainedParameters();

                String episodeStartWardCode = targetInpatientCds.getEpisodeStartWardCode();
                containedParametersBuilder.addParameter("", "" + episodeStartWardCode);

                String episodeEndWardCode = targetInpatientCds.getEpisodeEndWardCode();
                containedParametersBuilder.addParameter("", "" + episodeEndWardCode);

                //TODO:  linked diagnosis, procedures and maternity?

                // targetInpatientCds.getPrimaryDiagnosisICD());
                // targetInpatientCds.getSecondaryDiagnosisICD());
                // targetInpatientCds.getOtherDiagnosisICD());
                // targetInpatientCds.getPrimaryProcedureOPCS());
                // targetInpatientCds.getSecondaryProcedureOPCS());
                // targetInpatientCds.getOtherProceduresOPCS());

                // episode entry wil either have none, one or the other maternity json records
                // targetInpatientCds.getMaternityDataBirth());      -- the encounter is about the baby and contains the mothers nhs number
                // targetInpatientCds.getMaternityDataDelivery());   -- the encounter is about the mother and this is the birth(s) detail

                //save only the episode encounter builder here
                fhirResourceFiler.savePatientResource(null, encounterBuilder);
            }
        }
    }
}