package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalWaitingList extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRHospitalWaitingList.class); 

  public SRHospitalWaitingList(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "WaitingListName",
                      "WaitingListCategory",
                      "AdtID",
                      "DateAdded",
                      "DateWaitingStart",
                      "DateRemoved",
                      "RemovalReason",
                      "WaitingListType",
                      "IDConsultant",
                      "IDProfileConsultant",
                      "TreatmentCode",
                      "AdministrativeCategory",
                      "IntendedRTTStatus",
                      "ProcedureDiagnostic",
                      "ProcedureStatus",
                      "TestRequestType",
                      "WaitingListNotes",
                      "ShortNotice",
                      "AppointmentNotBookedYet",
                      "FirstFollowUp",
                      "AdmissionMethod",
                      "AAndEUnitType",
                      "IntendedManagement",
                      "IDReferralIn",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getWaitingListName() { return super.getCell("WaitingListName");};
 public CsvCell getWaitingListCategory() { return super.getCell("WaitingListCategory");};
 public CsvCell getAdtID() { return super.getCell("AdtID");};
 public CsvCell getDateAdded() { return super.getCell("DateAdded");};
 public CsvCell getDateWaitingStart() { return super.getCell("DateWaitingStart");};
 public CsvCell getDateRemoved() { return super.getCell("DateRemoved");};
 public CsvCell getRemovalReason() { return super.getCell("RemovalReason");};
 public CsvCell getWaitingListType() { return super.getCell("WaitingListType");};
 public CsvCell getIDConsultant() { return super.getCell("IDConsultant");};
 public CsvCell getIDProfileConsultant() { return super.getCell("IDProfileConsultant");};
 public CsvCell getTreatmentCode() { return super.getCell("TreatmentCode");};
 public CsvCell getAdministrativeCategory() { return super.getCell("AdministrativeCategory");};
 public CsvCell getIntendedRTTStatus() { return super.getCell("IntendedRTTStatus");};
 public CsvCell getProcedureDiagnostic() { return super.getCell("ProcedureDiagnostic");};
 public CsvCell getProcedureStatus() { return super.getCell("ProcedureStatus");};
 public CsvCell getTestRequestType() { return super.getCell("TestRequestType");};
 public CsvCell getWaitingListNotes() { return super.getCell("WaitingListNotes");};
 public CsvCell getShortNotice() { return super.getCell("ShortNotice");};
 public CsvCell getAppointmentNotBookedYet() { return super.getCell("AppointmentNotBookedYet");};
 public CsvCell getFirstFollowUp() { return super.getCell("FirstFollowUp");};
 public CsvCell getAdmissionMethod() { return super.getCell("AdmissionMethod");};
 public CsvCell getAAndEUnitType() { return super.getCell("AAndEUnitType");};
 public CsvCell getIntendedManagement() { return super.getCell("IntendedManagement");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRHospitalWaitingList Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
