package org.endeavourhealth.transform.tpp.csv.schema.unused.mentalhealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRSectionRightsExplained extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRSectionRightsExplained.class); 

  public SRSectionRightsExplained(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDEvent",
                      "IDSection",
                      "DateRightsExplained",
                      "PatientUnderstandingType",
                      "DateReview",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDEvent() { return super.getCell("IDEvent");}
 public CsvCell getIDSection() { return super.getCell("IDSection");}
 public CsvCell getDateRightsExplained() { return super.getCell("DateRightsExplained");}
 public CsvCell getPatientUnderstandingType() { return super.getCell("PatientUnderstandingType");}
 public CsvCell getDateReview() { return super.getCell("DateReview");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRSectionRightsExplained Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
