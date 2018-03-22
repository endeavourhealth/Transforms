package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRMHAWOL extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRMHAWOL.class); 

  public SRMHAWOL(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "AWOLDepartedDate",
                      "AWOLRecordedBy",
                      "AWOLEndDate",
                      "AWOLEndReason",
                      "AWOLEndRecordedBy",
                      "DateCreation",
                      "IDHospitalAdmission",
                      "IDHospitalAdmissionAndDischarge",
                      "IDSection",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getAWOLDepartedDate() { return super.getCell("AWOLDepartedDate");};
 public CsvCell getAWOLRecordedBy() { return super.getCell("AWOLRecordedBy");};
 public CsvCell getAWOLEndDate() { return super.getCell("AWOLEndDate");};
 public CsvCell getAWOLEndReason() { return super.getCell("AWOLEndReason");};
 public CsvCell getAWOLEndRecordedBy() { return super.getCell("AWOLEndRecordedBy");};
 public CsvCell getDateCreation() { return super.getCell("DateCreation");};
 public CsvCell getIDHospitalAdmission() { return super.getCell("IDHospitalAdmission");};
 public CsvCell getIDHospitalAdmissionAndDischarge() { return super.getCell("IDHospitalAdmissionAndDischarge");};
 public CsvCell getIDSection() { return super.getCell("IDSection");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRMHAWOL Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
