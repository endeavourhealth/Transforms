package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRTreatmentCentreSession extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRTreatmentCentreSession.class); 

  public SRTreatmentCentreSession(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "TreatmentCentreName",
                      "TreatmentCentrePostCode",
                      "DateSessionStart",
                      "DateSessionEnd",
                      "ActionGroups",
                      "SessionFrequencyType",
                      "SessionFixedFrequency",
                      "SessionWeekInMonthFrequency",
                      "SessionWeekOfSchedulingFrequency",
                      "SessionDay",
                      "SessionSpecifiedTimeStart",
                      "SessionSpecifiedTimeEnd",
                      "PatientNumber",
                      "SlotDuration",
                      "PatientsPerAppointment",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getTreatmentCentreName() { return super.getCell("TreatmentCentreName");};
 public CsvCell getTreatmentCentrePostCode() { return super.getCell("TreatmentCentrePostCode");};
 public CsvCell getDateSessionStart() { return super.getCell("DateSessionStart");};
 public CsvCell getDateSessionEnd() { return super.getCell("DateSessionEnd");};
 public CsvCell getActionGroups() { return super.getCell("ActionGroups");};
 public CsvCell getSessionFrequencyType() { return super.getCell("SessionFrequencyType");};
 public CsvCell getSessionFixedFrequency() { return super.getCell("SessionFixedFrequency");};
 public CsvCell getSessionWeekInMonthFrequency() { return super.getCell("SessionWeekInMonthFrequency");};
 public CsvCell getSessionWeekOfSchedulingFrequency() { return super.getCell("SessionWeekOfSchedulingFrequency");};
 public CsvCell getSessionDay() { return super.getCell("SessionDay");};
 public CsvCell getSessionSpecifiedTimeStart() { return super.getCell("SessionSpecifiedTimeStart");};
 public CsvCell getSessionSpecifiedTimeEnd() { return super.getCell("SessionSpecifiedTimeEnd");};
 public CsvCell getPatientNumber() { return super.getCell("PatientNumber");};
 public CsvCell getSlotDuration() { return super.getCell("SlotDuration");};
 public CsvCell getPatientsPerAppointment() { return super.getCell("PatientsPerAppointment");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRTreatmentCentreSession Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
