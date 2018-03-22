package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRSectionRecall extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRSectionRecall.class); 

  public SRSectionRecall(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "SectionRecallStartDate",
                      "SectionRecallExpiryDate",
                      "SectionRecallEndDate",
                      "SectionRecallCategory",
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "IDSection",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getSectionRecallStartDate() { return super.getCell("SectionRecallStartDate");};
 public CsvCell getSectionRecallExpiryDate() { return super.getCell("SectionRecallExpiryDate");};
 public CsvCell getSectionRecallEndDate() { return super.getCell("SectionRecallEndDate");};
 public CsvCell getSectionRecallCategory() { return super.getCell("SectionRecallCategory");};
 public CsvCell getDateCreation() { return super.getCell("DateCreation");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getIDSection() { return super.getCell("IDSection");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRSectionRecall Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
