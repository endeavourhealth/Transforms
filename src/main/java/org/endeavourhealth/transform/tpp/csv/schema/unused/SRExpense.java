package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRExpense extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRExpense.class); 

  public SRExpense(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateCreated",
                      "IdProfileCreatedBy",
                      "IDActivityEvent",
                      "IDStaffActivity",
                      "ExpenseType",
                      "ExpenseTypeCost",
                      "ReceiptObtained",
                      "BudgetCode",
                      "Miles",
                      "Duration",
                      "Occurences",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateCreated() { return super.getCell("DateCreated");}
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");}
 public CsvCell getIDActivityEvent() { return super.getCell("IDActivityEvent");}
 public CsvCell getIDStaffActivity() { return super.getCell("IDStaffActivity");}
 public CsvCell getExpenseType() { return super.getCell("ExpenseType");}
 public CsvCell getExpenseTypeCost() { return super.getCell("ExpenseTypeCost");}
 public CsvCell getReceiptObtained() { return super.getCell("ReceiptObtained");}
 public CsvCell getBudgetCode() { return super.getCell("BudgetCode");}
 public CsvCell getMiles() { return super.getCell("Miles");}
 public CsvCell getDuration() { return super.getCell("Duration");}
 public CsvCell getOccurences() { return super.getCell("Occurences");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRExpense Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
