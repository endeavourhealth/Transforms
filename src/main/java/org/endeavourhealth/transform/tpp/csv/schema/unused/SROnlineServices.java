package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROnlineServices extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SROnlineServices.class); 

  public SROnlineServices(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDEvent",
                      "IDPatient",
                      "DateEventRecorded",
                      "DateEvent",
                      "IDProfileEnteredBy",
                      "IDDoneBy",
                      "TextualEventDoneBy",
                      "IDOrganisation",
                      "IDOrganisationDoneAt",
                      "ViewRecord",
                      "Appointments",
                      "Prescriptions",
                      "Questionnaires",
                      "SummaryCareRecord",
                      "DateDeleted",
                      "IDProfileDeletedBy",
                      "DetailedCodedRecord",
                      "IDOnlineUser",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getIDEvent() { return super.getCell("IDEvent");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getDateEvent() { return super.getCell("DateEvent");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");}
 public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");}
 public CsvCell getViewRecord() { return super.getCell("ViewRecord");}
 public CsvCell getAppointments() { return super.getCell("Appointments");}
 public CsvCell getPrescriptions() { return super.getCell("Prescriptions");}
 public CsvCell getQuestionnaires() { return super.getCell("Questionnaires");}
 public CsvCell getSummaryCareRecord() { return super.getCell("SummaryCareRecord");}
 public CsvCell getDateDeleted() { return super.getCell("DateDeleted");}
 public CsvCell getIDProfileDeletedBy() { return super.getCell("IDProfileDeletedBy");}
 public CsvCell getDetailedCodedRecord() { return super.getCell("DetailedCodedRecord");}
 public CsvCell getIDOnlineUser() { return super.getCell("IDOnlineUser");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SROnlineServices Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
