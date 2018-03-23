package org.endeavourhealth.transform.tpp.schema.OutOfHours;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROohAssessment extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SROohAssessment.class); 

  public SROohAssessment(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateAssessment",
                      "AssessmentType",
                      "Priority",
                      "PriorityType",
                      "DateEventRecorded",
                      "IDProfileEnteredBy",
                      "IDCase",
                      "IDOohCase",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateAssessment() { return super.getCell("DateAssessment");};
 public CsvCell getAssessmentType() { return super.getCell("AssessmentType");};
 public CsvCell getPriority() { return super.getCell("Priority");};
 public CsvCell getPriorityType() { return super.getCell("PriorityType");};
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");};
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");};
 public CsvCell getIDCase() { return super.getCell("IDCase");};
 public CsvCell getIDOohCase() { return super.getCell("IDOohCase");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SROohAssessment Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
