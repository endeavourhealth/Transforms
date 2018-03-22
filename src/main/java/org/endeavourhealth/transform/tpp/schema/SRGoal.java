package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRGoal extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRGoal.class); 

  public SRGoal(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TPPCsvToFhirTransformer.CSV_FORMAT,
                    TPPCsvToFhirTransformer.DATE_FORMAT,
                    TPPCsvToFhirTransformer.TIME_FORMAT);
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
                      "GoalCategory",
                      "GoalType",
                      "GoalOutcome",
                      "GoalImportance",
                      "DateGoalTarget",
                      "ActionPlanCategory",
                      "ActionPlanPatientConfidence",
                      "ActionPlanOutcome",
                      "DateEndedActionPlan",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation"
                    

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
 public CsvCell getGoalCategory() { return super.getCell("GoalCategory");};
 public CsvCell getGoalType() { return super.getCell("GoalType");};
 public CsvCell getGoalOutcome() { return super.getCell("GoalOutcome");};
 public CsvCell getGoalImportance() { return super.getCell("GoalImportance");};
 public CsvCell getDateGoalTarget() { return super.getCell("DateGoalTarget");};
 public CsvCell getActionPlanCategory() { return super.getCell("ActionPlanCategory");};
 public CsvCell getActionPlanPatientConfidence() { return super.getCell("ActionPlanPatientConfidence");};
 public CsvCell getActionPlanOutcome() { return super.getCell("ActionPlanOutcome");};
 public CsvCell getDateEndedActionPlan() { return super.getCell("DateEndedActionPlan");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRGoal Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
