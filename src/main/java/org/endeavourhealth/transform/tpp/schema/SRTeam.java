package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRTeam extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRTeam.class); 

  public SRTeam(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "TeamName",
                      "TeamType",
                      "IDOrganisation",
                      "TeamCreated",
                      "TeamDeleted",
                      "IDParentTeam"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getTeamName() { return super.getCell("TeamName");};
 public CsvCell getTeamType() { return super.getCell("TeamType");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getTeamCreated() { return super.getCell("TeamCreated");};
 public CsvCell getTeamDeleted() { return super.getCell("TeamDeleted");};
 public CsvCell getIDParentTeam() { return super.getCell("IDParentTeam");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRTeam Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
