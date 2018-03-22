package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRStaffSkillSet extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRStaffSkillSet.class); 

  public SRStaffSkillSet(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateCreated",
                      "IdProfileCreatedBy",
                      "IDStaffProfile",
                      "DateSkillSetStart",
                      "DateSkillSetEnd",
                      "SkillSetName",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");};
 public CsvCell getIDStaffProfile() { return super.getCell("IDStaffProfile");};
 public CsvCell getDateSkillSetStart() { return super.getCell("DateSkillSetStart");};
 public CsvCell getDateSkillSetEnd() { return super.getCell("DateSkillSetEnd");};
 public CsvCell getSkillSetName() { return super.getCell("SkillSetName");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRStaffSkillSet Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
