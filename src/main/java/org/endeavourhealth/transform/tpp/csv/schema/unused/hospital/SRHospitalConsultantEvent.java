package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalConsultantEvent extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRHospitalConsultantEvent.class); 

  public SRHospitalConsultantEvent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateEpisodeStart",
                      "DateEpisodeEnd",
                      "IDProfileConsultant",
                      "ConsultantSpecialty",
                      "OperationStatus",
                      "AdministrativeCategory",
                      "TreatmentCode",
                      "LegalClassification",
                      "IDHospitalAdmission",
                      "IDHospitalAdmissionAndDischarge",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateCreated() { return super.getCell("DateCreated");}
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");}
 public CsvCell getDateEpisodeStart() { return super.getCell("DateEpisodeStart");}
 public CsvCell getDateEpisodeEnd() { return super.getCell("DateEpisodeEnd");}
 public CsvCell getIDProfileConsultant() { return super.getCell("IDProfileConsultant");}
 public CsvCell getConsultantSpecialty() { return super.getCell("ConsultantSpecialty");}
 public CsvCell getOperationStatus() { return super.getCell("OperationStatus");}
 public CsvCell getAdministrativeCategory() { return super.getCell("AdministrativeCategory");}
 public CsvCell getTreatmentCode() { return super.getCell("TreatmentCode");}
 public CsvCell getLegalClassification() { return super.getCell("LegalClassification");}
 public CsvCell getIDHospitalAdmission() { return super.getCell("IDHospitalAdmission");}
 public CsvCell getIDHospitalAdmissionAndDischarge() { return super.getCell("IDHospitalAdmissionAndDischarge");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRHospitalConsultantEvent Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
