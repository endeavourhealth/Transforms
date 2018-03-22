package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRStaffMember extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRStaffMember.class); 

  public SRStaffMember(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TPPCsvToFhirTransformer.CSV_FORMAT,
                    TPPCsvToFhirTransformer.DATE_FORMAT,
                    TPPCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "StaffName",
                      "StaffUserName",
                      "NationalIdType",
                      "IDNational",
                      "IDSmartCard",
                      "Obsolete",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getStaffName() { return super.getCell("StaffName");};
 public CsvCell getStaffUserName() { return super.getCell("StaffUserName");};
 public CsvCell getNationalIdType() { return super.getCell("NationalIdType");};
 public CsvCell getIDNational() { return super.getCell("IDNational");};
 public CsvCell getIDSmartCard() { return super.getCell("IDSmartCard");};
 public CsvCell getObsolete() { return super.getCell("Obsolete");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRStaffMember Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
