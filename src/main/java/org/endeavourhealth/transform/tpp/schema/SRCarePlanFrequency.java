package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRCarePlanFrequency extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRCarePlanFrequency.class); 

  public SRCarePlanFrequency(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TPPCsvToFhirTransformer.CSV_FORMAT,
                    TPPCsvToFhirTransformer.DATE_FORMAT,
                    TPPCsvToFhirTransformer.TIME_FORMAT);
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
                      "DateStart",
                      "DateEnd",
                      "FrequencyOfCareOccurrence",
                      "FrequencyTypeOfCareOccurrence",
                      "Location",
                      "IDProfileUsualCarer",
                      "EstimatedDuration",
                      "PreferredTime",
                      "IDCarePlanDetail",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");};
 public CsvCell getDateEvent() { return super.getCell("DateEvent");};
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");};
 public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");};
 public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");};
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");};
 public CsvCell getDateStart() { return super.getCell("DateStart");};
 public CsvCell getDateEnd() { return super.getCell("DateEnd");};
 public CsvCell getFrequencyOfCareOccurrence() { return super.getCell("FrequencyOfCareOccurrence");};
 public CsvCell getFrequencyTypeOfCareOccurrence() { return super.getCell("FrequencyTypeOfCareOccurrence");};
 public CsvCell getLocation() { return super.getCell("Location");};
 public CsvCell getIDProfileUsualCarer() { return super.getCell("IDProfileUsualCarer");};
 public CsvCell getEstimatedDuration() { return super.getCell("EstimatedDuration");};
 public CsvCell getPreferredTime() { return super.getCell("PreferredTime");};
 public CsvCell getIDCarePlanDetail() { return super.getCell("IDCarePlanDetail");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRCarePlanFrequency Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
