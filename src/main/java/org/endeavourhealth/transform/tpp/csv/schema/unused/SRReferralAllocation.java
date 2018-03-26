package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRReferralAllocation extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRReferralAllocation.class); 

  public SRReferralAllocation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDPatient",
                      "IDReferralIn",
                      "IDTeam",
                      "DateStart",
                      "DateEnd",
                      "DateCreated",
                      "IDProfileCreatedBy",
                      "DateDeleted",
                      "IDProfileDeletedBy"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDTeam() { return super.getCell("IDTeam");};
 public CsvCell getDateStart() { return super.getCell("DateStart");};
 public CsvCell getDateEnd() { return super.getCell("DateEnd");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getDateDeleted() { return super.getCell("DateDeleted");};
 public CsvCell getIDProfileDeletedBy() { return super.getCell("IDProfileDeletedBy");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRReferralAllocation Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
