package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAnsweredQuestionnaire extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRAnsweredQuestionnaire.class); 

  public SRAnsweredQuestionnaire(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDPatient",
                      "IDEvent",
                      "IDQuestionnaire",
                      "DateEvent",
                      "IDDoneBy",
                      "DateEventRecorded",
                      "IDProfileEnteredBy",
                      "IDOrganisationDoneAt"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDEvent() { return super.getCell("IDEvent");}
 public CsvCell getIDQuestionnaire() { return super.getCell("IDQuestionnaire");}
 public CsvCell getDateEvent() { return super.getCell("DateEvent");}
 public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRAnsweredQuestionnaire Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
