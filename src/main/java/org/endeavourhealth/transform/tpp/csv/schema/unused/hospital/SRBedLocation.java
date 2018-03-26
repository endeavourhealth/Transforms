package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRBedLocation extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRBedLocation.class); 

  public SRBedLocation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IdProfileCreatedBy",
                      "BedName",
                      "DateBedLocationStart",
                      "DateBedLocationEnd",
                      "IDLocation",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");};
 public CsvCell getBedName() { return super.getCell("BedName");};
 public CsvCell getDateBedLocationStart() { return super.getCell("DateBedLocationStart");};
 public CsvCell getDateBedLocationEnd() { return super.getCell("DateBedLocationEnd");};
 public CsvCell getIDLocation() { return super.getCell("IDLocation");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRBedLocation Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
