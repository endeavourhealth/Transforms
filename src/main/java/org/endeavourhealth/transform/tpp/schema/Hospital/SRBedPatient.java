package org.endeavourhealth.transform.tpp.schema.Hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRBedPatient extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRBedPatient.class); 

  public SRBedPatient(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IdProfileCreatedBy",
                      "DateCreated",
                      "BedName",
                      "IsLodger",
                      "DateBedDetailsStart",
                      "DateBedDetailsEnd",
                      "IDLocation",
                      "IDReferralIn",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getBedName() { return super.getCell("BedName");};
 public CsvCell getIsLodger() { return super.getCell("IsLodger");};
 public CsvCell getDateBedDetailsStart() { return super.getCell("DateBedDetailsStart");};
 public CsvCell getDateBedDetailsEnd() { return super.getCell("DateBedDetailsEnd");};
 public CsvCell getIDLocation() { return super.getCell("IDLocation");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRBedPatient Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
