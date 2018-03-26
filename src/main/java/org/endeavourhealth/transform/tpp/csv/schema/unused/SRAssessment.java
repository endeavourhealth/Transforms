package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAssessment extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRAssessment.class); 

  public SRAssessment(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "IDOrganisationVisibleTo",
                      "DateEventRecorded",
                      "DateEvent",
                      "IDProfileEnteredBy",
                      "IDDoneBy",
                      "TextualEventDoneBy",
                      "IDOrganisationDoneAt",
                      "AssessmentFor",
                      "IDPatientRelationship",
                      "AssessmentType",
                      "AssessmentReview",
                      "IDContact",
                      "QuestionnaireName",
                      "DateAssessmentTarget",
                      "DateAssessmentCompleted",
                      "DateAssessmentReview",
                      "AssessmentScore",
                      "ScoreOverrideReason",
                      "AssessmentBudget",
                      "IDReferralIn",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "ReasonForReview",
                      "ReasonForUnplannedReview"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");};
 public CsvCell getDateEvent() { return super.getCell("DateEvent");};
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");};
 public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");};
 public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");};
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");};
 public CsvCell getAssessmentFor() { return super.getCell("AssessmentFor");};
 public CsvCell getIDPatientRelationship() { return super.getCell("IDPatientRelationship");};
 public CsvCell getAssessmentType() { return super.getCell("AssessmentType");};
 public CsvCell getAssessmentReview() { return super.getCell("AssessmentReview");};
 public CsvCell getIDContact() { return super.getCell("IDContact");};
 public CsvCell getQuestionnaireName() { return super.getCell("QuestionnaireName");};
 public CsvCell getDateAssessmentTarget() { return super.getCell("DateAssessmentTarget");};
 public CsvCell getDateAssessmentCompleted() { return super.getCell("DateAssessmentCompleted");};
 public CsvCell getDateAssessmentReview() { return super.getCell("DateAssessmentReview");};
 public CsvCell getAssessmentScore() { return super.getCell("AssessmentScore");};
 public CsvCell getScoreOverrideReason() { return super.getCell("ScoreOverrideReason");};
 public CsvCell getAssessmentBudget() { return super.getCell("AssessmentBudget");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getReasonForReview() { return super.getCell("ReasonForReview");};
 public CsvCell getReasonForUnplannedReview() { return super.getCell("ReasonForUnplannedReview");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRAssessment Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
