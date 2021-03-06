package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.fhir.schema.ProblemRelationshipType;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisMappingHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Problem;
import org.hl7.fhir.instance.model.BooleanType;
import org.hl7.fhir.instance.model.DateType;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProblemTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Problem parser = (Problem)parsers.get(Problem.class);
        while (parser != null && parser.nextRecord()) {

            try {
                if (csvHelper.shouldProcessRecord(parser)) {
                    createResource(parser, fhirResourceFiler, csvHelper);
                }

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(Problem parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        ConditionBuilder conditionBuilder = new ConditionBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(conditionBuilder, patientGuid, observationGuid);

        Reference patientReference = csvHelper.createPatientReference(patientGuid);
        conditionBuilder.setPatient(patientReference, patientGuid);

        //the deleted fields isn't present in the test pack, so need to check the version first
        if (!parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_0)
                && !parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_1)) {

            //if the problem is deleted, it doesn't necessarily mean the observation is deleted,
            //so let the observation transformer pick up finishing saving this condition
            CsvCell deleted = parser.getDeleted();
            if (deleted.getBoolean()) {

                //if we have a row in the Problem file that's deleted but the row in the Observation file
                //isn't being deleted, then the Problem needs to be down-graded to just a Condition, so set
                //the profile URI accordingly
                conditionBuilder.setAsProblem(false);

                //the problem is actually saved in the ObservationTransformer, so just cache for later
                csvHelper.cacheProblem(observationGuid, patientGuid, conditionBuilder);
                return;
            }
        }

        //set the category on the condition, so we know it's a problem
        //NOTE: the text of "complaint" is wrong in that it's not the same as a "problem", but this is the String that was used
        conditionBuilder.setCategory("complaint", observationGuid);

        CsvCell comments = parser.getComment();
        if (!comments.isEmpty()) {
            //until we see an example of this data, we don't know if this text is a duplication of the AssociatedText
            //field on the observation table or not, so wait until we get something then work out where it should go
            throw new TransformException("Received problem comments - need to see if a duplicate of observation notes and work out FHIR mapping");
            //conditionBuilder.setAdditionalNotes(comments.getString(), comments);
        }

        CsvCell endDate = parser.getEndDate();
        if (!endDate.isEmpty()) {
            CsvCell endDatePrecision = parser.getEndDatePrecision(); //NOTE; documentation refers to this as EffectiveDate, but this should be EndDate
            DateType dateType = EmisDateTimeHelper.createDateType(endDate.getDate(), endDatePrecision.getString());
            conditionBuilder.setEndDateOrBoolean(dateType, endDate, endDatePrecision);

        } else {

            //if there's no end date, the problem may still be ended, which is in the status description
            CsvCell problemStatus = parser.getProblemStatusDescription();
            if (problemStatus.getString().equalsIgnoreCase("Past Problem")) {
                BooleanType booleanType = new BooleanType(true);
                conditionBuilder.setEndDateOrBoolean(booleanType, problemStatus);
            }
        }

        CsvCell expectedDuration = parser.getExpectedDuration();
        if (!expectedDuration.isEmpty()) {
            conditionBuilder.setExpectedDuration(expectedDuration.getInt(), expectedDuration);
        }

        CsvCell lastReviewDate = parser.getLastReviewDate();
        CsvCell lastReviewPrecision = parser.getLastReviewDatePrecision();
        DateType lastReviewDateType = EmisDateTimeHelper.createDateType(lastReviewDate, lastReviewPrecision);
        if (lastReviewDateType != null) {
            conditionBuilder.setProblemLastReviewDate(lastReviewDateType, lastReviewDate, lastReviewPrecision);
        }

        CsvCell lastReviewedByGuid = parser.getLastReviewUserInRoleGuid();
        if (!lastReviewedByGuid.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(lastReviewedByGuid);
            conditionBuilder.setProblemLastReviewedBy(practitionerReference, lastReviewedByGuid);
        }

        CsvCell significanceCell = parser.getSignificanceDescription();
        if (!significanceCell.isEmpty()) {
            //SD-236 change to use mapping CSV file
            ProblemSignificance fhirSignificance = EmisMappingHelper.findProblemSignificance(significanceCell.getString());
            //ProblemSignificance fhirSignificance = convertSignificance(significanceCell, csvHelper);
            conditionBuilder.setProblemSignificance(fhirSignificance, significanceCell);
        }

        CsvCell parentProblemGuid = parser.getParentProblemObservationGuid();
        if (!parentProblemGuid.isEmpty()) {

            CsvCell parentRelationshipCell = parser.getParentProblemRelationship();
            if (!parentRelationshipCell.isEmpty()) {
                //SD-236 change to use mapping CSV file
                ProblemRelationshipType fhirRelationshipType = EmisMappingHelper.findProblemRelationship(parentRelationshipCell.getString());
                //ProblemRelationshipType fhirRelationshipType = convertRelationshipType(parentRelationship.getString());
                conditionBuilder.setParentProblemRelationship(fhirRelationshipType, parentRelationshipCell);
            }

            Reference problemReference = csvHelper.createProblemReference(parentProblemGuid, patientGuid);
            conditionBuilder.setParentProblem(problemReference, parentProblemGuid);
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

        //carry over linked items from any previous instance of this problem
        ReferenceList previousReferences = csvHelper.findProblemPreviousLinkedResources(conditionBuilder.getResourceId());
        containedListBuilder.addReferences(previousReferences);

        //apply any linked items from this extract
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewProblemChildren(observationGuid, patientGuid);
        containedListBuilder.addReferences(newLinkedResources);

        //the problem is actually saved in the ObservationTransformer, so just cache for later
        csvHelper.cacheProblem(observationGuid, patientGuid, conditionBuilder);
    }


    /*private static ProblemRelationshipType convertRelationshipType(String relationshipType) throws Exception {

        if (relationshipType.equalsIgnoreCase("grouped")) {
            return ProblemRelationshipType.GROUPED;
        } else if (relationshipType.equalsIgnoreCase("combined")) {
            return ProblemRelationshipType.COMBINED;
        } else if (relationshipType.equalsIgnoreCase("evolved")) {
            return ProblemRelationshipType.EVOLVED_FROM;
        } else if (relationshipType.equalsIgnoreCase("replaced")) {
            return ProblemRelationshipType.REPLACES;
        } else {
            throw new IllegalArgumentException("Unhanded problem relationship type " + relationshipType);
        }
    }

    private static ProblemSignificance convertSignificance(CsvCell significanceCell, EmisCsvHelper csvHelper) throws Exception {

        String significance = significanceCell.getString();
        if (significance.equalsIgnoreCase("Significant Problem")) {
            return ProblemSignificance.SIGNIFICANT;

        } else if (significance.equalsIgnoreCase("Minor Problem")) {
            return ProblemSignificance.NOT_SIGNIFICANT;

        } else {
            TransformWarnings.log(LOG, csvHelper, "Unmapped problem significance {}", significanceCell);
            return ProblemSignificance.UNSPECIIED;
        }
    }*/


}
