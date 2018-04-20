package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRTeamMember extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRTeamMember.class); 

  public SRTeamMember(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "TeamStaff",
                      "IDTeam",
                      "MemberCreated",
                      "MemberDeleted",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getTeamStaff() { return super.getCell("TeamStaff");}
 public CsvCell getIDTeam() { return super.getCell("IDTeam");}
 public CsvCell getMemberCreated() { return super.getCell("MemberCreated");}
 public CsvCell getMemberDeleted() { return super.getCell("MemberDeleted");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRTeamMember Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
