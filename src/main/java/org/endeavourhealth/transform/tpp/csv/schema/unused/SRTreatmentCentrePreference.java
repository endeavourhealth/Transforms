package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRTreatmentCentrePreference extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRTreatmentCentrePreference.class); 

  public SRTreatmentCentrePreference(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateEvent",
                      "IDProfileEnteredBy",
                      "IDDoneBy",
                      "TextualEventDoneBy",
                      "IDOrganisationDoneAt",
                      "TreatmentCentreName",
                      "TreatmentCentrePostCode",
                      "TreatmentType",
                      "DateStart",
                      "DateEnd",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getDateEvent() { return super.getCell("DateEvent");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");}
 public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");}
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");}
 public CsvCell getTreatmentCentreName() { return super.getCell("TreatmentCentreName");}
 public CsvCell getTreatmentCentrePostCode() { return super.getCell("TreatmentCentrePostCode");}
 public CsvCell getTreatmentType() { return super.getCell("TreatmentType");}
 public CsvCell getDateStart() { return super.getCell("DateStart");}
 public CsvCell getDateEnd() { return super.getCell("DateEnd");}
 public CsvCell getIDEvent() { return super.getCell("IDEvent");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRTreatmentCentrePreference Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
