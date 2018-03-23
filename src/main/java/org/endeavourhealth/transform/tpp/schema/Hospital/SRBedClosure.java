package org.endeavourhealth.transform.tpp.schema.Hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRBedClosure extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRBedClosure.class); 

  public SRBedClosure(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "BedName",
                      "DateBedClosureStart",
                      "DateBedClosureEnd",
                      "BedClosureReason",
                      "IDLocation",
                      "IDOrganisation",
                      "DateDeleted",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getBedName() { return super.getCell("BedName");};
 public CsvCell getDateBedClosureStart() { return super.getCell("DateBedClosureStart");};
 public CsvCell getDateBedClosureEnd() { return super.getCell("DateBedClosureEnd");};
 public CsvCell getBedClosureReason() { return super.getCell("BedClosureReason");};
 public CsvCell getIDLocation() { return super.getCell("IDLocation");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getDateDeleted() { return super.getCell("DateDeleted");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRBedClosure Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
