package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRRtt extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRRtt.class); 

  public SRRtt(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateStart",
                      "DateClockStop",
                      "RttStatus",
                      "BreachReason",
                      "ErrorInClockCausedBreach",
                      "LedBy",
                      "WaitType",
                      "IDPatientPathWay",
                      "CancerSite",
                      "RequestRaisedReason",
                      "IDReferralIn",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateStart() { return super.getCell("DateStart");}
 public CsvCell getDateClockStop() { return super.getCell("DateClockStop");}
 public CsvCell getRttStatus() { return super.getCell("RttStatus");}
 public CsvCell getBreachReason() { return super.getCell("BreachReason");}
 public CsvCell getErrorInClockCausedBreach() { return super.getCell("ErrorInClockCausedBreach");}
 public CsvCell getLedBy() { return super.getCell("LedBy");}
 public CsvCell getWaitType() { return super.getCell("WaitType");}
 public CsvCell getIDPatientPathWay() { return super.getCell("IDPatientPathWay");}
 public CsvCell getCancerSite() { return super.getCell("CancerSite");}
 public CsvCell getRequestRaisedReason() { return super.getCell("RequestRaisedReason");}
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRRtt Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
