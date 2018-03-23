package org.endeavourhealth.transform.tpp.schema.Treatment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRTreatmentCentreExclusions extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRTreatmentCentreExclusions.class); 

  public SRTreatmentCentreExclusions(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "DateRemoval",
                      "IDRemovalBy",
                      "IDProfileRemovalBy",
                      "DateExclusionFrom",
                      "DateExclusionTo",
                      "IDOrganisationTreatmentCentre",
                      "IDOrganisationBranchTreatmentCentre",
                      "IDBranchTreatmentCentre",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateCreation() { return super.getCell("DateCreation");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getDateRemoval() { return super.getCell("DateRemoval");};
 public CsvCell getIDRemovalBy() { return super.getCell("IDRemovalBy");};
 public CsvCell getIDProfileRemovalBy() { return super.getCell("IDProfileRemovalBy");};
 public CsvCell getDateExclusionFrom() { return super.getCell("DateExclusionFrom");};
 public CsvCell getDateExclusionTo() { return super.getCell("DateExclusionTo");};
 public CsvCell getIDOrganisationTreatmentCentre() { return super.getCell("IDOrganisationTreatmentCentre");};
 public CsvCell getIDOrganisationBranchTreatmentCentre() { return super.getCell("IDOrganisationBranchTreatmentCentre");};
 public CsvCell getIDBranchTreatmentCentre() { return super.getCell("IDBranchTreatmentCentre");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRTreatmentCentreExclusions Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
