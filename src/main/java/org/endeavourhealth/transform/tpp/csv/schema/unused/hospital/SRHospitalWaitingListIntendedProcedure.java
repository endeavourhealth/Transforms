package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalWaitingListIntendedProcedure extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRHospitalWaitingListIntendedProcedure.class); 

  public SRHospitalWaitingListIntendedProcedure(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "ProcedureTest",
                      "ProcedureTestCode",
                      "SiteLaterality",
                      "Notes",
                      "Primary",
                      "IsDeleted",
                      "IDHospitalWaitingList",
                      "IDReferralIn",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getProcedureTest() { return super.getCell("ProcedureTest");}
 public CsvCell getProcedureTestCode() { return super.getCell("ProcedureTestCode");}
 public CsvCell getSiteLaterality() { return super.getCell("SiteLaterality");}
 public CsvCell getNotes() { return super.getCell("Notes");}
 public CsvCell getPrimary() { return super.getCell("Primary");}
 public CsvCell getIsDeleted() { return super.getCell("IsDeleted");}
 public CsvCell getIDHospitalWaitingList() { return super.getCell("IDHospitalWaitingList");}
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRHospitalWaitingListIntendedProcedure Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
