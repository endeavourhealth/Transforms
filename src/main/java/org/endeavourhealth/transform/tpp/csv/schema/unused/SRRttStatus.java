package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRRttStatus extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRRttStatus.class); 

  public SRRttStatus(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateStatus",
                      "RttStatus",
                      "IDReferralIn",
                      "IDPatient",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getDateStatus() { return super.getCell("DateStatus");}
 public CsvCell getRttStatus() { return super.getCell("RttStatus");}
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRRttStatus Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
