package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAppointmentVisitOutcomes extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRAppointmentVisitOutcomes.class); 

  public SRAppointmentVisitOutcomes(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT,
                    TppCsvToFhirTransformer.ENCODING);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "IDOrganisationVisibleTo",
                      "DateCreated",
                      "IdProfileCreatedBy",
                      "DateRecorded",
                      "IDProfileResponsibleProfessional",
                      "ConsultantSpecialtyMappingCode",
                      "TreatmentCodeMappingCode",
                      "TreatmentCode",
                      "OutcomeOfAttendanceMappingCode",
                      "OperationStatus",
                      "AdministrativeCategory",
                      "IDAppointment",
                      "IDVisit",
                      "IDReferralIn",
                      "IDPatient",
                      "OutcomeOfAttendance",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateCreated() { return super.getCell("DateCreated");}
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");}
 public CsvCell getDateRecorded() { return super.getCell("DateRecorded");}
 public CsvCell getIDProfileResponsibleProfessional() { return super.getCell("IDProfileResponsibleProfessional");}
 public CsvCell getConsultantSpecialtyMappingCode() { return super.getCell("ConsultantSpecialtyMappingCode");}
 public CsvCell getTreatmentCodeMappingCode() { return super.getCell("TreatmentCodeMappingCode");}
 public CsvCell getTreatmentCode() { return super.getCell("TreatmentCode");}
 public CsvCell getOutcomeOfAttendanceMappingCode() { return super.getCell("OutcomeOfAttendanceMappingCode");}
 public CsvCell getOperationStatus() { return super.getCell("OperationStatus");}
 public CsvCell getAdministrativeCategory() { return super.getCell("AdministrativeCategory");}
 public CsvCell getIDAppointment() { return super.getCell("IDAppointment");}
 public CsvCell getIDVisit() { return super.getCell("IDVisit");}
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getOutcomeOfAttendance() { return super.getCell("OutcomeOfAttendance");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRAppointmentVisitOutcomes Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
