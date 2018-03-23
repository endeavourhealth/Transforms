package org.endeavourhealth.transform.tpp.schema.Unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAAndEAttendance extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRAAndEAttendance.class); 

  public SRAAndEAttendance(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDProfileCreatedBy",
                      "ArrivalMode",
                      "AttendanceCategory",
                      "ReferralSource",
                      "IDProfileLeadClinician",
                      "AccompaniedBy",
                      "IncidentLocationType",
                      "PatientGroup",
                      "PresentingComplaint",
                      "PresentingComplaintText",
                      "IncidentNotes",
                      "TriageCategory",
                      "PatientCategory",
                      "DateAttendanceConcluded",
                      "DepartureMethod",
                      "PrimaryBreachReason",
                      "SecondaryBreachReason",
                      "AAndEAttendanceComplete",
                      "ExpectedOutcome",
                      "DateExpectedOutcomeConfirmed",
                      "AdditionalGpInfo",
                      "DateDeleted",
                      "IDProfileDeletedBy",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getArrivalMode() { return super.getCell("ArrivalMode");};
 public CsvCell getAttendanceCategory() { return super.getCell("AttendanceCategory");};
 public CsvCell getReferralSource() { return super.getCell("ReferralSource");};
 public CsvCell getIDProfileLeadClinician() { return super.getCell("IDProfileLeadClinician");};
 public CsvCell getAccompaniedBy() { return super.getCell("AccompaniedBy");};
 public CsvCell getIncidentLocationType() { return super.getCell("IncidentLocationType");};
 public CsvCell getPatientGroup() { return super.getCell("PatientGroup");};
 public CsvCell getPresentingComplaint() { return super.getCell("PresentingComplaint");};
 public CsvCell getPresentingComplaintText() { return super.getCell("PresentingComplaintText");};
 public CsvCell getIncidentNotes() { return super.getCell("IncidentNotes");};
 public CsvCell getTriageCategory() { return super.getCell("TriageCategory");};
 public CsvCell getPatientCategory() { return super.getCell("PatientCategory");};
 public CsvCell getDateAttendanceConcluded() { return super.getCell("DateAttendanceConcluded");};
 public CsvCell getDepartureMethod() { return super.getCell("DepartureMethod");};
 public CsvCell getPrimaryBreachReason() { return super.getCell("PrimaryBreachReason");};
 public CsvCell getSecondaryBreachReason() { return super.getCell("SecondaryBreachReason");};
 public CsvCell getAAndEAttendanceComplete() { return super.getCell("AAndEAttendanceComplete");};
 public CsvCell getExpectedOutcome() { return super.getCell("ExpectedOutcome");};
 public CsvCell getDateExpectedOutcomeConfirmed() { return super.getCell("DateExpectedOutcomeConfirmed");};
 public CsvCell getAdditionalGpInfo() { return super.getCell("AdditionalGpInfo");};
 public CsvCell getDateDeleted() { return super.getCell("DateDeleted");};
 public CsvCell getIDProfileDeletedBy() { return super.getCell("IDProfileDeletedBy");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRAAndEAttendance Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
