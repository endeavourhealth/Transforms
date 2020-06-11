package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRProblem;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class SRProblemTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRProblemTransformer.class);

    public static final String PROBLEM_ID_TO_CODE_ID = "ProblemIdToCodeId";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRProblem.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRProblem) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRProblem parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell problemId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            //we use the code ID as the unique ID, which isn't present when deleting, so we need to look up what it was
            String codeId = csvHelper.getInternalId(PROBLEM_ID_TO_CODE_ID, problemId.getString());
            if (!Strings.isNullOrEmpty(codeId)) {
                CsvCell dummyCodeIdCell = CsvCell.factoryDummyWrapper(codeId);
                ConditionBuilder conditionBuilder = csvHelper.getConditionResourceCache().getConditionBuilderAndRemoveFromCache(dummyCodeIdCell, csvHelper);

                ResourceType trueResourceType = SRCodeTransformer.wasOriginallySavedAsOtherThanCondition(fhirResourceFiler, dummyCodeIdCell);
                if (trueResourceType != null) {
                    //if the SRCode wouldn't normally have been a Condition, then we'll have doubled up and created a
                    //Condition as well as the other resource, in which case we need to DELETE the condition resource

                    //first, check if already Id mapped and if not, needs Id mapping, i.e. not filed yet which is an erroneous for a delete
                    boolean mapIds = !conditionBuilder.isIdMapped();
                    if (mapIds) {

                        //the condition is being deleted here and needs Id mapping, So it has NOT been previously saved,
                        //so cannot be deleted. Therefore, Log, and return gracefully
                        TransformWarnings.log(LOG, csvHelper, "Cannot find existing Condition: {} for deletion", codeId);
                        return;
                    }

                    conditionBuilder.setDeletedAudit(deleteData);
                    csvHelper.getConditionResourceCache().returnToCacheForDelete(dummyCodeIdCell, conditionBuilder);

                } else {
                    //if a SRProblem refers to an SRCode that should be a Condition, they both SHARE the same FHIR resource
                    //and we need to just down-grade the existing resource to a non-problem.
                    conditionBuilder.setAsProblem(false);

                    //clear down all the problem-specific condition fields
                    conditionBuilder.setEndDateOrBoolean(null);
                    conditionBuilder.setExpectedDuration(null);
                    conditionBuilder.setProblemLastReviewDate(null);
                    conditionBuilder.setProblemLastReviewedBy(null);
                    conditionBuilder.setProblemSignificance(null);
                    conditionBuilder.setParentProblem(null);
                    conditionBuilder.setParentProblemRelationship(null);
                    //conditionBuilder.setAdditionalNotes(null);

                    ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);
                    containedListBuilder.removeContainedList();

                    //don't forget to return to the cache
                    csvHelper.getConditionResourceCache().returnToCache(dummyCodeIdCell, conditionBuilder);
                }
            }

            return;
        }

        //for problems, use the linked observationId as the ID to build up the resource to then add to in SRCode Transformer
        CsvCell linkedObsCodeId = parser.getIDCode();

        ConditionBuilder conditionBuilder = csvHelper.getConditionResourceCache().getConditionBuilderAndRemoveFromCache(linkedObsCodeId, csvHelper);

        //we may need to "upgrade" the existing condition to being a problem
        conditionBuilder.setAsProblem(true);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        if (conditionBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference,fhirResourceFiler);
        }
        conditionBuilder.setPatient(patientReference, patientId);

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!TppCsvHelper.isEmptyOrNegative(profileIdRecordedBy)) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            if (conditionBuilder.isIdMapped()) {
                staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference,fhirResourceFiler);
            }
            conditionBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!TppCsvHelper.isEmptyOrNegative(staffMemberIdDoneBy)) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDOrganisationDoneAt());
            if (staffReference != null) {
                if (conditionBuilder.isIdMapped()) {
                    staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference, fhirResourceFiler);
                }
                conditionBuilder.setClinician(staffReference, staffMemberIdDoneBy);
            }
        }

        //status is mandatory, so set the only value we can
        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            conditionBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateEvent();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            conditionBuilder.setOnset(dateTimeType, effectiveDate);
        }

        //set the category on the condition, so we know it's a problem
        //NOTE: the text of "complaint" is wrong in that it's not the same as a "problem", but this is the String that was used
        conditionBuilder.setCategory("complaint", problemId);
        conditionBuilder.setAsProblem(true);

        CsvCell endDateCell = parser.getDateEnd();
        if (endDateCell.isEmpty()) { //possible to re-activate problems, so support changing TO it being empty
            conditionBuilder.setEndDateOrBoolean(null);

        } else {
            DateType dateType = new DateType(endDateCell.getDate());
            conditionBuilder.setEndDateOrBoolean(dateType, endDateCell);
        }

        CsvCell severityCell = parser.getSeverity();
        if (severityCell.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(severityCell);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                if (mappedTerm.equalsIgnoreCase("minor")) {
                    conditionBuilder.setProblemSignificance(ProblemSignificance.NOT_SIGNIFICANT);

                } else if (mappedTerm.equalsIgnoreCase("major")) {
                    conditionBuilder.setProblemSignificance(ProblemSignificance.SIGNIFICANT);

                } else {
                    throw new Exception("Unexpected SRProblem severity [" + mappedTerm + "]");
                }
            } else {
                conditionBuilder.setProblemSignificance(ProblemSignificance.UNSPECIIED);
            }
        }

        //don't forget to return to the cache
        csvHelper.getConditionResourceCache().returnToCache(linkedObsCodeId, conditionBuilder);
    }
}
