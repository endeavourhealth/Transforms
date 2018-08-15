package org.endeavourhealth.transform.tpp.csv.schema.unused.outofhours;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROohTransport extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SROohTransport.class); 

  public SROohTransport(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT,
                    TppCsvToFhirTransformer.ENCODING);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "IDOrganisationVisibleTo",
                      "DateEventRecorded",
                      "IDProfileEnteredBy",
                      "DateRequired",
                      "DateRequested",
                      "DateBooked",
                      "DateCancelled",
                      "IDProfileAssignedTo",
                      "IDOohCase",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getDateRequired() { return super.getCell("DateRequired");}
 public CsvCell getDateRequested() { return super.getCell("DateRequested");}
 public CsvCell getDateBooked() { return super.getCell("DateBooked");}
 public CsvCell getDateCancelled() { return super.getCell("DateCancelled");}
 public CsvCell getIDProfileAssignedTo() { return super.getCell("IDProfileAssignedTo");}
 public CsvCell getIDOohCase() { return super.getCell("IDOohCase");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SROohTransport Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
