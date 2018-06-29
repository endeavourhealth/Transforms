package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.Problem;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class ProblemTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    createConditionProblem((Problem)parser, fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createConditionProblem(Problem parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setAsProblem(true);
        conditionBuilder.setContext("clinical coding");

        CsvCell problemIdCell = parser.getProblemId();
        conditionBuilder.setId(problemIdCell.getString(), problemIdCell);

        // set patient reference
        //this file uses MRN, so we need to map that to a Cerner Person ID
        CsvCell mrnCell = parser.getMrn();
        String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, mrnCell.getString());
        if (personId == null) {
            //TransformWarnings.log(LOG, parser, "Skipping problem ID {} because no Person ID could be found for MRN {}", problemIdCell.getString(), mrnCell.getString());
            return;
        }

        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }

        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId);
        conditionBuilder.setPatient(patientReference);

        //the status cell tells us to delete
        CsvCell statusCell = parser.getStatusLifecycle();
        String status = statusCell.getString();
        if (status.equalsIgnoreCase("Canceled")) { //note the US spelling used

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            return;
        }

        // Date recorded
        CsvCell updatedDateCell = parser.getUpdateDateTime();
        Date updatedDate = updatedDateCell.getDate();
        conditionBuilder.setRecordedDate(updatedDate, updatedDateCell);

        // set code to coded problem
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

        //it's rare, but there are cases where records have a textual term but not vocab or code
        CsvCell problemCodeCell = parser.getProblemCode();
        CsvCell vocabCell = parser.getVocabulary();
        if (!vocabCell.isEmpty() && !problemCodeCell.isEmpty()) {
            String vocab = vocabCell.getString();
            String code = problemCodeCell.getString();

            if (vocab.equalsIgnoreCase("SNOMED CT")) {
                //the code is a SNOMED description ID, not concept ID, so we need to look up the term differently
                SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(code);
                if (snomedCode == null) {
                    TransformWarnings.log(LOG, parser, "Failed to lookup Snomed term for code {}", code);

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID, vocabCell);
                    codeableConceptBuilder.setCodingCode(code, problemCodeCell);

                } else {
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, vocabCell);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode(), problemCodeCell);
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                }

                /*String term = TerminologyService.lookupSnomedTerm(code);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, parser, "Failed to lookup Snomed term for code {}", code);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, vocabCell);
                codeableConceptBuilder.setCodingCode(code, problemCodeCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived*/

            } else if (vocab.equalsIgnoreCase("ICD-10")) {
                String term = TerminologyService.lookupIcd10CodeDescription(code);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, parser, "Failed to lookup ICD-10 term for code {}", code);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, vocabCell);
                codeableConceptBuilder.setCodingCode(code, problemCodeCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived

            } else if (vocab.equalsIgnoreCase("Cerner")) {
                //in this file, Cerner VOCAB doesn't seem to mean it refers to the CVREF file, so don't make any attempt to look up an official term
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID, vocabCell);
                codeableConceptBuilder.setCodingCode(code, problemCodeCell);

            } else {
                throw new TransformException("Unexpected problem VOCAB [" + vocab + "]");
            }
        }

        //set the raw term on the codeable concept text
        CsvCell problemTermCell = parser.getProblem();
        if (!problemTermCell.isEmpty()) {
            String term = problemTermCell.getString();
            codeableConceptBuilder.setText(term, problemTermCell);
        }

        // set category to 'complaint'
        //NOTE: the text of "complaint" is wrong in that it's not the same as a "problem", but this is the String that was used
        conditionBuilder.setCategory("complaint");

        // set onset to field  to field 10 + 11
        CsvCell onsetDateCell = parser.getOnsetDate();
        DateTimeType onsetDate = new DateTimeType(onsetDateCell.getDate());
        conditionBuilder.setOnset(onsetDate, onsetDateCell);

        //note that status is also used, at the start of this fn, to work out whether to delete the resource
        if (status.equalsIgnoreCase("Active")) {
            conditionBuilder.setEndDateOrBoolean(null, statusCell);

        } else if (status.equalsIgnoreCase("Resolved")
                || status.equalsIgnoreCase("Inactive")) {

            CsvCell statusDateCell = parser.getStatusDate();
            if (statusDateCell.isEmpty()) {
                //if we don't have a status date, use a boolean to indicate the end
                conditionBuilder.setEndDateOrBoolean(new BooleanType(true), statusCell);

            } else {
                DateType dt = new DateType(statusDateCell.getDate());
                conditionBuilder.setEndDateOrBoolean(dt, statusCell);
            }
        }

        CsvCell confirmation = parser.getConfirmation();
        if (!confirmation.isEmpty()) {
            String confirmationDesc = confirmation.getString();
            if (confirmationDesc.equalsIgnoreCase("Confirmed")) {
                conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED, statusCell);

            } else {
                conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.PROVISIONAL, statusCell);
            }
        }

        // set notes
        CsvCell annotatedDisplayCell = parser.getAnnotatedDisp();
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
