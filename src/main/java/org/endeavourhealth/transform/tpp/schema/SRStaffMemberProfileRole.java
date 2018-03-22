package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRStaffMemberProfileRole extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfileRole.class); 

  public SRStaffMemberProfileRole(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TPPCsvToFhirTransformer.CSV_FORMAT,
                    TPPCsvToFhirTransformer.DATE_FORMAT,
                    TPPCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "RoleDescription",
                      "IsSpineRole",
                      "DateLastAmended",
                      "DateDeleted"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getRoleDescription() { return super.getCell("RoleDescription");};
 public CsvCell getIsSpineRole() { return super.getCell("IsSpineRole");};
 public CsvCell getDateLastAmended() { return super.getCell("DateLastAmended");};
 public CsvCell getDateDeleted() { return super.getCell("DateDeleted");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRStaffMemberProfileRole Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
