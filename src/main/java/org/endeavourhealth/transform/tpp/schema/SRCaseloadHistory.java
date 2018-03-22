package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRCaseloadHistory extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRCaseloadHistory.class); 

  public SRCaseloadHistory(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDPatient",
                      "IDReferralIn",
                      "IDCaseload",
                      "DateTo",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDCaseload() { return super.getCell("IDCaseload");};
 public CsvCell getDateTo() { return super.getCell("DateTo");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRCaseloadHistory Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
