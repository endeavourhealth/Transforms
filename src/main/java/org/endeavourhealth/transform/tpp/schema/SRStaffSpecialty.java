package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRStaffSpecialty extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRStaffSpecialty.class); 

  public SRStaffSpecialty(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IdStaffProfile",
                      "DateSpecialtyStart",
                      "DateSpecialtyEnd",
                      "SpecialtyName",
                      "SpecialtyType",
                      "DateSpecialtyDeleted",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");};
 public CsvCell getIdStaffProfile() { return super.getCell("IdStaffProfile");};
 public CsvCell getDateSpecialtyStart() { return super.getCell("DateSpecialtyStart");};
 public CsvCell getDateSpecialtyEnd() { return super.getCell("DateSpecialtyEnd");};
 public CsvCell getSpecialtyName() { return super.getCell("SpecialtyName");};
 public CsvCell getSpecialtyType() { return super.getCell("SpecialtyType");};
 public CsvCell getDateSpecialtyDeleted() { return super.getCell("DateSpecialtyDeleted");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRStaffSpecialty Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
