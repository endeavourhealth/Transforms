package org.endeavourhealth.transform.tpp.csv.schema.staff;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRStaffMemberProfile extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfile.class); 

  public SRStaffMemberProfile(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "DateProfileCreated",
                      "IdProfileCreatedBy",
                      "IDStaffMemberProfileRole",
                      "StaffRole",
                      "DateEmploymentStart",
                      "DateEmploymentEnd",
                      "PPAID",
                      "GPLocalCode",
                      "IDStaffMember",
                      "IDOrganisation",
                      "GmpID"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getDateProfileCreated() { return super.getCell("DateProfileCreated");};
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");};
 public CsvCell getIDStaffMemberProfileRole() { return super.getCell("IDStaffMemberProfileRole");};
 public CsvCell getStaffRole() { return super.getCell("StaffRole");};
 public CsvCell getDateEmploymentStart() { return super.getCell("DateEmploymentStart");};
 public CsvCell getDateEmploymentEnd() { return super.getCell("DateEmploymentEnd");};
 public CsvCell getPPAID() { return super.getCell("PPAID");};
 public CsvCell getGPLocalCode() { return super.getCell("GPLocalCode");};
 public CsvCell getIDStaffMember() { return super.getCell("IDStaffMember");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getGmpID() { return super.getCell("GmpID");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRStaffMemberProfile Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
