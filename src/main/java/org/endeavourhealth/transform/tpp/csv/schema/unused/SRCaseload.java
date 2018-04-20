package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCaseload extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRCaseload.class); 

  public SRCaseload(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "CaseloadName",
                      "CaseloadTeam",
                      "IDProfileCaseloadOwner",
                      "DateCreated",
                      "IDProfileCreated",
                      "DateDeleted",
                      "IDProfileDeleted",
                      "AgencyCode"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getCaseloadName() { return super.getCell("CaseloadName");}
 public CsvCell getCaseloadTeam() { return super.getCell("CaseloadTeam");}
 public CsvCell getIDProfileCaseloadOwner() { return super.getCell("IDProfileCaseloadOwner");}
 public CsvCell getDateCreated() { return super.getCell("DateCreated");}
 public CsvCell getIDProfileCreated() { return super.getCell("IDProfileCreated");}
 public CsvCell getDateDeleted() { return super.getCell("DateDeleted");}
 public CsvCell getIDProfileDeleted() { return super.getCell("IDProfileDeleted");}
 public CsvCell getAgencyCode() { return super.getCell("AgencyCode");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRCaseload Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
