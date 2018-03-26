package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCtv3Hierarchy extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRCtv3Hierarchy.class); 

  public SRCtv3Hierarchy(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "Ctv3CodeParent",
                      "Ctv3CodeChild",
                      "ChildLevel",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getCtv3CodeParent() { return super.getCell("Ctv3CodeParent");};
 public CsvCell getCtv3CodeChild() { return super.getCell("Ctv3CodeChild");};
 public CsvCell getChildLevel() { return super.getCell("ChildLevel");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP Ctv3 Hierarchy Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
