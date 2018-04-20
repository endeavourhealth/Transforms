package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPatientLocation extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRPatientLocation.class); 

  public SRPatientLocation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDLocation",
                      "DateRecorded",
                      "IDProfileEnteredBy",
                      "DateStart",
                      "DateEnd",
                      "DateDeleted",
                      "Status",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDLocation() { return super.getCell("IDLocation");}
 public CsvCell getDateRecorded() { return super.getCell("DateRecorded");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getDateStart() { return super.getCell("DateStart");}
 public CsvCell getDateEnd() { return super.getCell("DateEnd");}
 public CsvCell getDateDeleted() { return super.getCell("DateDeleted");}
 public CsvCell getStatus() { return super.getCell("Status");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRPatientLocation Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
