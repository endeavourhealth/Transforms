package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRDischargeDelay extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRDischargeDelay.class); 

  public SRDischargeDelay(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateCreated",
                      "IdProfileCreatedBy",
                      "DateAdmitted",
                      "DateDischarged",
                      "IDDischargedBy",
                      "IDProfileDischargedBy",
                      "MedicallyReadyForDischarge",
                      "DateMedicallyReadyForDischarge",
                      "ReasonForDelay",
                      "Responsibility",
                      "DateDelayStart",
                      "DateDelayEnd",
                      "IDAdmission",
                      "IDHospitalAdmissionAndDischarge",
                      "IDReferralIn",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateCreated() { return super.getCell("DateCreated");}
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");}
 public CsvCell getDateAdmitted() { return super.getCell("DateAdmitted");}
 public CsvCell getDateDischarged() { return super.getCell("DateDischarged");}
 public CsvCell getIDDischargedBy() { return super.getCell("IDDischargedBy");}
 public CsvCell getIDProfileDischargedBy() { return super.getCell("IDProfileDischargedBy");}
 public CsvCell getMedicallyReadyForDischarge() { return super.getCell("MedicallyReadyForDischarge");}
 public CsvCell getDateMedicallyReadyForDischarge() { return super.getCell("DateMedicallyReadyForDischarge");}
 public CsvCell getReasonForDelay() { return super.getCell("ReasonForDelay");}
 public CsvCell getResponsibility() { return super.getCell("Responsibility");}
 public CsvCell getDateDelayStart() { return super.getCell("DateDelayStart");}
 public CsvCell getDateDelayEnd() { return super.getCell("DateDelayEnd");}
 public CsvCell getIDAdmission() { return super.getCell("IDAdmission");}
 public CsvCell getIDHospitalAdmissionAndDischarge() { return super.getCell("IDHospitalAdmissionAndDischarge");}
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRDischargeDelay Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
