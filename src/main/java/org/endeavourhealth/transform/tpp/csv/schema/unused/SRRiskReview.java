package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRRiskReview extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRRiskReview.class); 

  public SRRiskReview(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDPatient",
                      "RiskCategory",
                      "RiskSubCategory",
                      "RiskRating",
                      "RiskReviewDate",
                      "RiskReviewDueDate",
                      "DateEventRecorded",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getRiskCategory() { return super.getCell("RiskCategory");}
 public CsvCell getRiskSubCategory() { return super.getCell("RiskSubCategory");}
 public CsvCell getRiskRating() { return super.getCell("RiskRating");}
 public CsvCell getRiskReviewDate() { return super.getCell("RiskReviewDate");}
 public CsvCell getRiskReviewDueDate() { return super.getCell("RiskReviewDueDate");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRRiskReview Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
