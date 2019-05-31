package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionTarget;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ConditionTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionTargetTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createConditions(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createConditions(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target conditions for the current exchangeId
        List<StagingConditionTarget> targetConditions = csvHelper.retrieveTargetConditions();
        if (targetConditions == null) {
            return;
        }

        TransformWarnings.log(LOG, csvHelper, "Target Conditions to transform to FHIR: {} for exchangeId: {}", targetConditions.size(), csvHelper.getExchangeId());

        for (StagingConditionTarget targetCondition : targetConditions) {

            String uniqueId = targetCondition.getUniqueId();
            boolean isDeleted = targetCondition.isDeleted();
            String problemStatus = targetCondition.getProblemStatus();

            if (isDeleted || problemStatus.equalsIgnoreCase("Canceled")) {

                // retrieve the existing Condition resource to perform the deletion on
                Condition existingCondition
                        = (Condition) csvHelper.retrieveResourceForLocalId(ResourceType.Condition, uniqueId);

                if (existingCondition != null) {
                    ConditionBuilder conditionBuilder = new ConditionBuilder(existingCondition);
                    //TODO: procedureBuilder.setDeletedAudit(activeCell);   //build up audit from text?
                    //remember to pass in false since this existing procedure is already ID mapped
                    fhirResourceFiler.deletePatientResource(null, false, conditionBuilder);
                } else {
                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Condition: {} for deletion", uniqueId);
                }

                continue;
            }

            // create the FHIR Condition resource - NOTE //TODO: no individual audit cells set
            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(uniqueId);

            //we always have a performed date, although it may be an unknown
            if (!isUnknownConditionDate(targetCondition.getDtPerformed())) {

                DateTimeType conditionDateTime = new DateTimeType(targetCondition.getDtPerformed());
                conditionBuilder.setOnset(conditionDateTime);
            } else {
                conditionBuilder.setOnset(null);
            }

            // set the patient reference
            Integer personId = targetCondition.getPersonId();
            if (personId == null) {
                TransformWarnings.log(LOG, csvHelper, "Missing person ID in condition_target for Condition Id: {}", uniqueId);
                continue;
            }
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            conditionBuilder.setPatient(patientReference);

            //Problems do not have EncounterIds
            Integer encounterId = targetCondition.getEncounterId();
            if (encounterId != null) {

                Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(encounterId));
                conditionBuilder.setEncounter(encounterReference);
            }

            String confirmation = targetCondition.getConfirmation();
            if (!Strings.isNullOrEmpty(confirmation)) {

                if (confirmation.equalsIgnoreCase("Confirmed")) {

                    conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);
                } else if (confirmation.equalsIgnoreCase("Suspected")) {

                    //conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.PROVISIONAL);
                    TransformWarnings.log(LOG, csvHelper, "Ignoring Condition Id with status 'Suspected': {}", uniqueId);
                    continue;
                }
            } else {

                TransformWarnings.log(LOG, csvHelper, "Ignoring Condition Id without a confirmation status: {}", uniqueId);
                continue;
            }

            //is it a Problem - use problem status to determine
            Boolean isProblem = !Strings.isNullOrEmpty(problemStatus);
            conditionBuilder.setAsProblem(isProblem);

            //note that status is also used, at the start of this fn, to work out whether to delete the resource
            if (isProblem) {
                if (problemStatus.equalsIgnoreCase("Active")) {
                    conditionBuilder.setEndDateOrBoolean(null);

                } else if (problemStatus.equalsIgnoreCase("Resolved")
                        || problemStatus.equalsIgnoreCase("Inactive")) {

                    //CsvCell statusDateCell = parser.getStatusDate();
                    //if (statusDateCell.isEmpty()) {
                        //if we don't have a status date, use a boolean to indicate the end
                    //TODO: Status date TBC
                        conditionBuilder.setEndDateOrBoolean(new BooleanType(true));

                    //} else {
                    //    DateType dt = new DateType(statusDateCell.getDate());
                    //    conditionBuilder.setEndDateOrBoolean(dt, statusCell);
                    //}
                }
            } else {
                //an active Diagnosis
                conditionBuilder.setEndDateOrBoolean(null);
            }

            // clinician
            Integer performerPersonnelId = targetCondition.getPerformerPersonnelId();
            if (performerPersonnelId != null) {
                Reference practitionerPerformerReference
                        = ReferenceHelper.createReference(ResourceType.Practitioner, String.valueOf(performerPersonnelId));
                conditionBuilder.setClinician(practitionerPerformerReference);
            }

            // coded concept - can be either of these three coded types;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

            String conditionCodeType = targetCondition.getConditionCodeType().trim();

            if (conditionCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED) ||
                    conditionCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED_CT) ||
                    conditionCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_UK_ED_SUBSET)) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

            } else if (conditionCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);

            } else if (conditionCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_CERNER)) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
            } else {
                throw new TransformException("Unknown Condition Target code type [" + conditionCodeType + "]");
            }
            String conditionCode = targetCondition.getConditionCode();
            if (!Strings.isNullOrEmpty(conditionCode)) {

                codeableConceptBuilder.setCodingCode(conditionCode);
            } else {
                TransformWarnings.log(LOG, csvHelper, "Condition Code is empty in Condition Target for Condition Id: {}", uniqueId);
                continue;
            }
            String conditionTerm = targetCondition.getConditionTerm();
            codeableConceptBuilder.setCodingDisplay(conditionTerm);
            codeableConceptBuilder.setText(conditionTerm);

            String conditionType = targetCondition.getConditionType();
            if (!Strings.isNullOrEmpty(conditionType)) {

                //TODO: check target table values against codeSet 17L
                CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(17L, conditionType);
                if (cernerCodeValueRef != null) {

                    String category = cernerCodeValueRef.getCodeDispTxt();
                    conditionBuilder.setCategory(category);
                }
            }

            // sequence number, primary and parent Condition
            Integer sequenceNumber = targetCondition.getSequenceNumber();
            if (sequenceNumber != null) {

                //TODO: CONDITION sequence number extension
                //conditionBuilder.setSequenceNumber(sequenceNumber);
                if (sequenceNumber == 1) {
                    conditionBuilder.setIsPrimary(true);

                } else {
                    // parent resource
                    String parentConditionId = targetCondition.getParentConditionUniqueId();
                    if (!Strings.isNullOrEmpty(parentConditionId)) {

                        Reference parentConditionReference
                                = ReferenceHelper.createReference(ResourceType.Condition, parentConditionId);
                        conditionBuilder.setParentResource(parentConditionReference);
                    }
                }
            }

            // notes / free text
            StringBuilder conditionNotes = new StringBuilder();

            String freeText = targetCondition.getFreeText();
            if (!Strings.isNullOrEmpty(freeText)) {

                conditionNotes.append("Notes: "+freeText + ". ");
            }

            String classification = targetCondition.getClassification();
            if (!Strings.isNullOrEmpty(classification)) {

                conditionNotes.append("Classification: "+classification + ". ");
            }

            String axis = targetCondition.getAxis();
            if (!Strings.isNullOrEmpty(axis)) {

                conditionNotes.append("Axis: "+axis + ". ");
            }

            String ranking = targetCondition.getRanking();
            if (!Strings.isNullOrEmpty(ranking)) {

                conditionNotes.append("Rank: "+ranking + ". ");
            }

            String locationText = targetCondition.getLocation();
            if (!Strings.isNullOrEmpty(locationText)) {

                conditionNotes.append("Location: " + locationText+". ");
            }

            //finally, set notes text
            conditionBuilder.setNotes(conditionNotes.toString());

            if (targetCondition.isConfidential() != null
                    && targetCondition.isConfidential().booleanValue()) {
                conditionBuilder.setIsConfidential(true);
            }

            fhirResourceFiler.savePatientResource(null, conditionBuilder);

            LOG.debug("Transforming conditionId: "+uniqueId+"  Filed");
        }
    }

    //if date = 1900-01-01
    private static Boolean isUnknownConditionDate(Date date) throws Exception {

        Date unknownDate = new SimpleDateFormat("yyyy-MM-dd").parse("1900-01-01");
        return date.equals(unknownDate);
    }

}
