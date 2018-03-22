package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRCluster extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRCluster.class); 

  public SRCluster(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "Cluster",
                      "ClusterDateStart",
                      "ReviewDate",
                      "ClusterDateEnd",
                      "EndReason",
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getCluster() { return super.getCell("Cluster");};
 public CsvCell getClusterDateStart() { return super.getCell("ClusterDateStart");};
 public CsvCell getReviewDate() { return super.getCell("ReviewDate");};
 public CsvCell getClusterDateEnd() { return super.getCell("ClusterDateEnd");};
 public CsvCell getEndReason() { return super.getCell("EndReason");};
 public CsvCell getDateCreation() { return super.getCell("DateCreation");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRCluster Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
