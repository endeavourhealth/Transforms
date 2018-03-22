package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRSectionedBy extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRSectionedBy.class); 

  public SRSectionedBy(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDSection",
                      "SourceTableForSectionedBy",
                      "IDForSectionedBy",
                      "Role",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDSection() { return super.getCell("IDSection");};
 public CsvCell getSourceTableForSectionedBy() { return super.getCell("SourceTableForSectionedBy");};
 public CsvCell getIDForSectionedBy() { return super.getCell("IDForSectionedBy");};
 public CsvCell getRole() { return super.getCell("Role");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRSectionedBy Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
