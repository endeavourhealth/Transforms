package org.endeavourhealth.transform.tpp.schema.Referral;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRReferralOut extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRReferralOut.class); 

  public SRReferralOut(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "TypeOfReferral",
                      "Reason",
                      "IDProfileReferrer",
                      "ServiceOffered",
                      "ReReferral",
                      "Urgency",
                      "PrimaryDiagnosis",
                      "RecipientID",
                      "RecipientIDType",
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
 public CsvCell getTypeOfReferral() { return super.getCell("TypeOfReferral");};
 public CsvCell getReason() { return super.getCell("Reason");};
 public CsvCell getIDProfileReferrer() { return super.getCell("IDProfileReferrer");};
 public CsvCell getServiceOffered() { return super.getCell("ServiceOffered");};
 public CsvCell getReReferral() { return super.getCell("ReReferral");};
 public CsvCell getUrgency() { return super.getCell("Urgency");};
 public CsvCell getPrimaryDiagnosis() { return super.getCell("PrimaryDiagnosis");};
 public CsvCell getRecipientID() { return super.getCell("RecipientID");};
 public CsvCell getRecipientIDType() { return super.getCell("RecipientIDType");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRReferralOut Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
