package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.ProblemTable;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProblemTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                        createCondition((ProblemTable) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createCondition(ProblemTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper) throws Exception {

        CsvCell problemIdCell = parser.getProblemId();

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setId(problemIdCell.getString(), problemIdCell);
        conditionBuilder.setAsProblem(true);
        conditionBuilder.setCategory("complaint");

        // set patient reference
        CsvCell personIdCell = parser.getPersonId();
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
        conditionBuilder.setPatient(patientReference, personIdCell);

        //the life cycle status cell tells us to delete
        CsvCell statusCell = parser.getStatusLifecycle();
        String status = statusCell.getString();
        if (status.equalsIgnoreCase("Canceled")) { //note the US spelling used

            conditionBuilder.setDeletedAudit(statusCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            return;
        }

        //TODO - what if Onset date is blank/missing?
        CsvCell onsetDateCell = parser.getOnsetDate();
        if (!onsetDateCell.isEmpty()) {
            DateTimeType onsetDate = new DateTimeType(onsetDateCell.getDate());
            conditionBuilder.setOnset(onsetDate, onsetDateCell);
        }

        // set code to coded problem
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

        // it's rare, but there are cases where records have a textual term but not vocab or code
        CsvCell problemCodeCell = parser.getProblemCode();
        CsvCell vocabCell = parser.getVocabulary();
        CsvCell problemTermCell = parser.getProblemDescriptionText();
        if (!vocabCell.isEmpty() && !problemCodeCell.isEmpty()) {
            String vocab = vocabCell.getString();
            String code = problemCodeCell.getString();

            if (vocab.equalsIgnoreCase("SNOMED CT")) {
                // the code is a SNOMED description ID, not concept ID, so we need to look up the term differently
                SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(code);
                if (snomedCode == null) {
                    TransformWarnings.log(LOG, parser, "Failed to lookup Snomed term for descriptionId {}", code);

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID, vocabCell);
                    codeableConceptBuilder.setCodingCode(code, problemCodeCell);

                } else {
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, vocabCell);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode(), problemCodeCell);
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                }
            } else if (vocab.equalsIgnoreCase("Cerner")) {
                // in this file, Cerner VOCAB doesn't seem to mean it refers to the CVREF file, so don't make any
                // attempt to look up an official term but instead use the problem description for the display
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_BARTS_CERNER_CODE_ID, vocabCell);
                codeableConceptBuilder.setCodingCode(code, problemCodeCell);

                if (!problemTermCell.isEmpty()) {
                    codeableConceptBuilder.setCodingDisplay(problemTermCell.getString(), problemTermCell);
                }

            } else {
                throw new TransformException("Unexpected problem VOCAB [" + vocab + "]");
            }
        }

        // set the raw term on the codeable concept text
        if (!problemTermCell.isEmpty()) {
            String term = problemTermCell.getString();
            codeableConceptBuilder.setText(term, problemTermCell);
        }

        // note that status is also used, at the start of this fn, to work out whether to delete the resource
        if (status.equalsIgnoreCase("Active")) {
            conditionBuilder.setEndDateOrBoolean(null, statusCell);

        } else if (status.equalsIgnoreCase("Resolved")
                || status.equalsIgnoreCase("Inactive")) {

            CsvCell lifeCycleDateTimeCell = parser.getLifeCycleDateTime();
            if (lifeCycleDateTimeCell.isEmpty()) {
                //if we don't have a life cycle date, use a boolean to indicate the end
                conditionBuilder.setEndDateOrBoolean(new BooleanType(true), lifeCycleDateTimeCell);

            } else {
                DateType dt = new DateType(lifeCycleDateTimeCell.getDate());
                conditionBuilder.setEndDateOrBoolean(dt, lifeCycleDateTimeCell);
            }
        }

        // no other confirmation status except confirmed
        conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED, statusCell);

        // set notes
        CsvCell annotatedDisplayCell = parser.getProblemAnnotatedDisplay();
        if (!annotatedDisplayCell.isEmpty()) {
            //in the vast majority of cases, this just contains a duplicate of the term so only set as notes if different
            String annotatedText = annotatedDisplayCell.getString();
            String term = problemTermCell.getString();

            if (!annotatedText.equals(term)) {
                conditionBuilder.setNotes(annotatedText, annotatedDisplayCell);
            }
        }

        // save resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }



}
