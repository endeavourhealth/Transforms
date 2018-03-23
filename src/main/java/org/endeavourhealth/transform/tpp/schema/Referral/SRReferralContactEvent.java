package org.endeavourhealth.transform.tpp.schema.Referral;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRReferralContactEvent extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRReferralContactEvent.class); 

  public SRReferralContactEvent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "Location",
                      "Length",
                      "Category",
                      "SubCategory",
                      "ClinicallyRelevant",
                      "Team",
                      "ContactMethod",
                      "DateInitialContact",
                      "IDRegisteredPracticeAtTimeOfEvent",
                      "IDRegisteredGpAtTimeOfEvent",
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
 public CsvCell getLocation() { return super.getCell("Location");};
 public CsvCell getLength() { return super.getCell("Length");};
 public CsvCell getCategory() { return super.getCell("Category");};
 public CsvCell getSubCategory() { return super.getCell("SubCategory");};
 public CsvCell getClinicallyRelevant() { return super.getCell("ClinicallyRelevant");};
 public CsvCell getTeam() { return super.getCell("Team");};
 public CsvCell getContactMethod() { return super.getCell("ContactMethod");};
 public CsvCell getDateInitialContact() { return super.getCell("DateInitialContact");};
 public CsvCell getIDRegisteredPracticeAtTimeOfEvent() { return super.getCell("IDRegisteredPracticeAtTimeOfEvent");};
 public CsvCell getIDRegisteredGpAtTimeOfEvent() { return super.getCell("IDRegisteredGpAtTimeOfEvent");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRReferralContactEvent Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
