package org.endeavourhealth.transform.tpp.csv.schema.unused.outofhours;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROohVisit extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SROohVisit.class); 

  public SROohVisit(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateEventRecordedContact",
                      "IDProfileEnteredByContact",
                      "DateContact",
                      "DateEventRecordedRequestReceived",
                      "IDProfileEnteredByReceived",
                      "DateRequestReceived",
                      "DateEventRecordedRequestAcknowledged",
                      "IDProfileEnteredByRequestAcknowledged",
                      "DateRequestAcknowledged",
                      "DateEventRecordedArrivalAtDestination",
                      "IDProfileEnteredByArrivalAtDestination",
                      "DateArrivalAtDestination",
                      "DateEventRecordedContactFinished",
                      "IDProfileEnteredByContactFinished",
                      "DateContactFinished",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateEventRecordedContact() { return super.getCell("DateEventRecordedContact");}
 public CsvCell getIDProfileEnteredByContact() { return super.getCell("IDProfileEnteredByContact");}
 public CsvCell getDateContact() { return super.getCell("DateContact");}
 public CsvCell getDateEventRecordedRequestReceived() { return super.getCell("DateEventRecordedRequestReceived");}
 public CsvCell getIDProfileEnteredByReceived() { return super.getCell("IDProfileEnteredByReceived");}
 public CsvCell getDateRequestReceived() { return super.getCell("DateRequestReceived");}
 public CsvCell getDateEventRecordedRequestAcknowledged() { return super.getCell("DateEventRecordedRequestAcknowledged");}
 public CsvCell getIDProfileEnteredByRequestAcknowledged() { return super.getCell("IDProfileEnteredByRequestAcknowledged");}
 public CsvCell getDateRequestAcknowledged() { return super.getCell("DateRequestAcknowledged");}
 public CsvCell getDateEventRecordedArrivalAtDestination() { return super.getCell("DateEventRecordedArrivalAtDestination");}
 public CsvCell getIDProfileEnteredByArrivalAtDestination() { return super.getCell("IDProfileEnteredByArrivalAtDestination");}
 public CsvCell getDateArrivalAtDestination() { return super.getCell("DateArrivalAtDestination");}
 public CsvCell getDateEventRecordedContactFinished() { return super.getCell("DateEventRecordedContactFinished");}
 public CsvCell getIDProfileEnteredByContactFinished() { return super.getCell("IDProfileEnteredByContactFinished");}
 public CsvCell getDateContactFinished() { return super.getCell("DateContactFinished");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SROohVisit Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
