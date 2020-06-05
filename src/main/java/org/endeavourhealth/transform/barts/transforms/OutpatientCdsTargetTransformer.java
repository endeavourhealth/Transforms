package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingOutpatientCdsTarget;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedParametersBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.List_;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

            String uniqueId = targetOutpatientCds.getUniqueId();         //this is the uniqueId for the entire CDS encounter record
            Integer encounterId = targetOutpatientCds.getEncounterId();  //this is used to identify the top level parent episode

            boolean isDeleted = targetOutpatientCds.isDeleted();
            if (isDeleted) {

                // retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
                Encounter existingParentEncounter
                      = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));

                if (existingParentEncounter != null) {

                    EncounterBuilder parentEncounterBuilder
                            = new EncounterBuilder(existingParentEncounter, targetOutpatientCds.getAudit());

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

            // set single outpatient encounter, i.e. no child encounters
            EncounterBuilder encounterBuilder = new EncounterBuilder();
            encounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

            //the unique Id for the outpatient encounter based on attendance identifier
            String attendanceId = targetOutpatientCds.getApptAttendanceIdentifier() + ":OP";
            encounterBuilder.setId(attendanceId);

            Integer personId = targetOutpatientCds.getPersonId();
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            encounterBuilder.setPatient(patientReference);

            Date appDate = targetOutpatientCds.getApptDate();
            encounterBuilder.setPeriodStart(appDate);   //no end date

            Integer episodeId = targetOutpatientCds.getEpisodeId();
            if (episodeId != null) {
                encounterBuilder.setEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeId.toString()));
            }

            //get the existing parent encounter set during ADT feed -
            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
            if (existingParentEncounter != null) {

                //TODO what do we do with this for outpatients other that set as the parent?
                Reference parentEncounter
                        = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(encounterId));
                encounterBuilder.setPartOf(parentEncounter);
            } else {

                //TODO what do we do if the parent ENCNTR_ID encounter does not exist, create it with minimum data?
            }

            Integer performerPersonnelId = targetOutpatientCds.getPerformerPersonnelId();
            if (performerPersonnelId != null) {

                encounterBuilder.setRecordedBy(ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId)));
            }
            String serviceProviderOrgId = targetOutpatientCds.getApptSiteCode();
            if (!Strings.isNullOrEmpty(serviceProviderOrgId)) {

                encounterBuilder.setServiceProvider(ReferenceHelper.createReference(ResourceType.Organization, serviceProviderOrgId));
            }

            //add in additional extended data as Parameters resource with additional extension
            //TODO: set name and values using IM map once done
            ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(encounterBuilder);
            containedParametersBuilder.removeContainedParameters();

            String adminCategoryCode = targetOutpatientCds.getAdministrativeCategoryCode();
            containedParametersBuilder.addParameter("DM_hasAdministrativeCategoryCode", "CM_AdminCat" + adminCategoryCode);

            String referralSourceId = targetOutpatientCds.getReferralSource();
            containedParametersBuilder.addParameter("", "" + referralSourceId);

            String apptAttendedCode = targetOutpatientCds.getApptAttendedCode();
            containedParametersBuilder.addParameter("", "" + apptAttendedCode);

            String apptOutcomeCode = targetOutpatientCds.getApptAttendedCode();
            containedParametersBuilder.addParameter("", "" + apptOutcomeCode);

            //this is a Cerner code which is mapped to an NHS DD alias
            String treatmentFunctionCode = targetOutpatientCds.getTreatmentFunctionCode();
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.TREATMENT_FUNCTION, treatmentFunctionCode);
            if (codeRef != null) {

                String treatmentFunctionCodeNHSAliasCode = codeRef.getAliasNhsCdAlias();
                containedParametersBuilder.addParameter("", "" + treatmentFunctionCodeNHSAliasCode);
            }

            String pathwayIdentifier = targetOutpatientCds.getPatientPathwayIdentifier();   //set as an identifier?

            //add in diagnosis or procedure data match the encounter date? - already processed via proc and diag CDS transforms

//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getPrimaryDiagnosisICD())) {
//                additionalArrivalObjs.addProperty("primary_diagnosis", targetOutpatientCds.getPrimaryDiagnosisICD());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getSecondaryDiagnosisICD())) {
//                additionalArrivalObjs.addProperty("secondary_diagnosis", targetOutpatientCds.getSecondaryDiagnosisICD());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getOtherDiagnosisICD())) {
//                additionalArrivalObjs.addProperty("other_diagnosis", targetOutpatientCds.getOtherDiagnosisICD());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getPrimaryProcedureOPCS())) {
//                additionalArrivalObjs.addProperty("primary_procedure", targetOutpatientCds.getPrimaryProcedureOPCS());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getSecondaryProcedureOPCS())) {
//                additionalArrivalObjs.addProperty("secondary_procedure", targetOutpatientCds.getSecondaryProcedureOPCS());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getOtherProceduresOPCS())) {
//                additionalArrivalObjs.addProperty("other_procedures", targetOutpatientCds.getOtherProceduresOPCS());
//            }


            //save encounterBuilder record
            fhirResourceFiler.savePatientResource(null, encounterBuilder);
        }
    }
}