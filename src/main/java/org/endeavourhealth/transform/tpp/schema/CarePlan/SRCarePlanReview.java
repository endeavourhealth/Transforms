package org.endeavourhealth.transform.tpp.schema.CarePlan;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCarePlanReview extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRCarePlanReview.class); 

  public SRCarePlanReview(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDProfileEnteredBy",
                      "DateRequested",
                      "IDRequestedBy",
                      "TextualRequestedBy",
                      "IDOrganisationRequestedAt",
                      "DateDue",
                      "DatePerformed",
                      "Outcome",
                      "IDProfilePerformedBy",
                      "IDCarePlan",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");};
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");};
 public CsvCell getDateRequested() { return super.getCell("DateRequested");};
 public CsvCell getIDRequestedBy() { return super.getCell("IDRequestedBy");};
 public CsvCell getTextualRequestedBy() { return super.getCell("TextualRequestedBy");};
 public CsvCell getIDOrganisationRequestedAt() { return super.getCell("IDOrganisationRequestedAt");};
 public CsvCell getDateDue() { return super.getCell("DateDue");};
 public CsvCell getDatePerformed() { return super.getCell("DatePerformed");};
 public CsvCell getOutcome() { return super.getCell("Outcome");};
 public CsvCell getIDProfilePerformedBy() { return super.getCell("IDProfilePerformedBy");};
 public CsvCell getIDCarePlan() { return super.getCell("IDCarePlan");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRCarePlanReview Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
