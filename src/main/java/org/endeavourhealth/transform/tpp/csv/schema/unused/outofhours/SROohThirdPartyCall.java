package org.endeavourhealth.transform.tpp.csv.schema.unused.outofhours;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROohThirdPartyCall extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SROohThirdPartyCall.class); 

  public SROohThirdPartyCall(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateCall",
                      "Contact",
                      "Outcome",
                      "IDOohCase",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getDateCall() { return super.getCell("DateCall");}
 public CsvCell getContact() { return super.getCell("Contact");}
 public CsvCell getOutcome() { return super.getCell("Outcome");}
 public CsvCell getIDOohCase() { return super.getCell("IDOohCase");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SROohThirdPartyCall Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
