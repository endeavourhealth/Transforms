package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.CASEQUESTIONS;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.QuestionnaireResponseBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.QuestionnaireResponse;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CASEQUESTIONSTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(CASEQUESTIONSTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(CASEQUESTIONS.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((CASEQUESTIONS) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(CASEQUESTIONS parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        //use the CaseId as the Id for the Questionnaire
        CsvCell caseIdCell = parser.getCaseId();

        QuestionnaireResponseBuilder questionnaireResponseBuilder
                = csvHelper.getQuestionnaireResponseCache().getOrCreateQuestionnaireResponseBuilder(caseIdCell,csvHelper,fhirResourceFiler);

        //does the patient have a Case record?
        CsvCell casePatientIdCell = csvHelper.findCasePatient(caseIdCell.getString());
        if (casePatientIdCell == null) {
            TransformWarnings.log(LOG, parser, "No Case record match found for case {},  file: {}",
                    caseIdCell.getString(), parser.getFilePath());
            return;
        }

        boolean isResourceMapped
                = csvHelper.isResourceIdMapped(caseIdCell.getString(), questionnaireResponseBuilder.getResource());

        Reference patientReference = csvHelper.createPatientReference(casePatientIdCell);
        if (isResourceMapped) {

            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        questionnaireResponseBuilder.setSubject(patientReference);

        //set the authored date to that of the Case start date
        CsvCell caseStartDateCell = csvHelper.findCaseStartDate(caseIdCell.getString());
        if (caseStartDateCell != null) {

            questionnaireResponseBuilder.setAuthoredDate(caseStartDateCell.getDateTime());
        }

        //set the author to that of the Case User derived from the Case
        CsvCell caseUserCell = csvHelper.findCaseUser(caseIdCell.getString());
        if (caseUserCell != null) {

            Reference practitionerReference = csvHelper.createPractitionerReference(caseUserCell.getString());
            if (isResourceMapped) {

                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            questionnaireResponseBuilder.setAuthor(practitionerReference);
        }

        //store the Case Number as the business identifier
        CsvCell caseNoCell = csvHelper.findCaseCaseNo(caseIdCell.getString());
        if (!caseNoCell.isEmpty()) {

            Identifier identifier = new Identifier();
            identifier.setUse(Identifier.IdentifierUse.SECONDARY);
            identifier.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ADASTRA_CASENO);
            identifier.setValue(caseNoCell.getString());
            questionnaireResponseBuilder.setIdentifier(identifier, caseNoCell);
        }

        CsvCell questionGroupCell = parser.getQuestionSetName();
        CsvCell questionCell = parser.getQuestion();
        CsvCell questionAnswerCell = parser.getAnswer();

        //first, get or create the main group
        QuestionnaireResponse.GroupComponent mainGroup = questionnaireResponseBuilder.getOrCreateMainGroup();

        //does the sub / question group already exist?, otherwise, add it to the main group
        QuestionnaireResponse.GroupComponent subGroup
                = questionnaireResponseBuilder.getGroup(mainGroup, questionGroupCell.getString());
        if (subGroup == null) {
            subGroup = mainGroup.addGroup().setTitle(questionGroupCell.getString());
        }

        //now add the question and answer for the sub group item
        QuestionnaireResponse.QuestionComponent question = subGroup.addQuestion().setText(questionCell.getString());
        question.addAnswer().setValue(new StringType(questionAnswerCell.getString()));

        // return the builder back to the cache
        csvHelper.getQuestionnaireResponseCache().returnQuestionnaireResponseBuilder(caseIdCell, questionnaireResponseBuilder);
    }
}
