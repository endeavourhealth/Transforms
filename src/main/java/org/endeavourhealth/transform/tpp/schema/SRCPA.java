package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRCPA extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRCPA.class); 

  public SRCPA(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "CPADateStart",
                      "CPADateEnd",
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "CPAReviewDate"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getCPADateStart() { return super.getCell("CPADateStart");};
 public CsvCell getCPADateEnd() { return super.getCell("CPADateEnd");};
 public CsvCell getDateCreation() { return super.getCell("DateCreation");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getCPAReviewDate() { return super.getCell("CPAReviewDate");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRCPA Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
