package org.endeavourhealth.transform.tpp.csv.schema.unused.careplan;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCarePlan extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRCarePlan.class); 

  public SRCarePlan(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateStart",
                      "DateEnd",
                      "CarePlanCategory",
                      "CarePlanSubCategory",
                      "NameOfCarePlanUsed",
                      "Aim",
                      "Location",
                      "FrequencyOfCareOccurrence",
                      "FrequencyTypeOfCareOccurrence",
                      "UsualCarer",
                      "IDProfileUsualCarer",
                      "EstimatedDuration",
                      "PreferredTime",
                      "Outcome",
                      "IDCarePlan",
                      "IDReferralIn",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation"
                    

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
 public CsvCell getCarePlanCategory() { return super.getCell("CarePlanCategory");};
 public CsvCell getCarePlanSubCategory() { return super.getCell("CarePlanSubCategory");};
 public CsvCell getNameOfCarePlanUsed() { return super.getCell("NameOfCarePlanUsed");};
 public CsvCell getAim() { return super.getCell("Aim");};
 public CsvCell getLocation() { return super.getCell("Location");};
 public CsvCell getFrequencyOfCareOccurrence() { return super.getCell("FrequencyOfCareOccurrence");};
 public CsvCell getFrequencyTypeOfCareOccurrence() { return super.getCell("FrequencyTypeOfCareOccurrence");};
 public CsvCell getUsualCarer() { return super.getCell("UsualCarer");};
 public CsvCell getIDProfileUsualCarer() { return super.getCell("IDProfileUsualCarer");};
 public CsvCell getEstimatedDuration() { return super.getCell("EstimatedDuration");};
 public CsvCell getPreferredTime() { return super.getCell("PreferredTime");};
 public CsvCell getOutcome() { return super.getCell("Outcome");};
 public CsvCell getIDCarePlan() { return super.getCell("IDCarePlan");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRCarePlan Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
