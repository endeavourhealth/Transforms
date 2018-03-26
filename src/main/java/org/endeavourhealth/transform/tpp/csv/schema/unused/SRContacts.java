package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRContacts extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRContacts.class); 

  public SRContacts(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateCreated",
                      "IdProfileCreatedBy",
                      "DateContact",
                      "ContactMethod",
                      "CommissioningTeam",
                      "ContactWith",
                      "ContactSource",
                      "ClientAwareOfContact",
                      "ResponsibleAdultAware",
                      "ContactReason",
                      "RouteOfAccess",
                      "ContactOutcome",
                      "DateClosed",
                      "IDReferralIn",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");};
 public CsvCell getDateContact() { return super.getCell("DateContact");};
 public CsvCell getContactMethod() { return super.getCell("ContactMethod");};
 public CsvCell getCommissioningTeam() { return super.getCell("CommissioningTeam");};
 public CsvCell getContactWith() { return super.getCell("ContactWith");};
 public CsvCell getContactSource() { return super.getCell("ContactSource");};
 public CsvCell getClientAwareOfContact() { return super.getCell("ClientAwareOfContact");};
 public CsvCell getResponsibleAdultAware() { return super.getCell("ResponsibleAdultAware");};
 public CsvCell getContactReason() { return super.getCell("ContactReason");};
 public CsvCell getRouteOfAccess() { return super.getCell("RouteOfAccess");};
 public CsvCell getContactOutcome() { return super.getCell("ContactOutcome");};
 public CsvCell getDateClosed() { return super.getCell("DateClosed");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRContacts Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
