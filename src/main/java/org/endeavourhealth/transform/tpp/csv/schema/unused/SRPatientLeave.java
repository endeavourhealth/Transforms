package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPatientLeave extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRPatientLeave.class); 

  public SRPatientLeave(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "LeaveType",
                      "DateExpectedStart",
                      "DateExpectedEnd",
                      "DateActualStart",
                      "DateActualEnd",
                      "EndReason",
                      "SCTConsideredDate",
                      "LeaveApproved",
                      "IDSection",
                      "IDHospitalAdmission",
                      "IDHospitalAdmissionAndDischarge",
                      "IDPatient",
                      "IDOrganisation",
                      "FreeBed",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getLeaveType() { return super.getCell("LeaveType");}
 public CsvCell getDateExpectedStart() { return super.getCell("DateExpectedStart");}
 public CsvCell getDateExpectedEnd() { return super.getCell("DateExpectedEnd");}
 public CsvCell getDateActualStart() { return super.getCell("DateActualStart");}
 public CsvCell getDateActualEnd() { return super.getCell("DateActualEnd");}
 public CsvCell getEndReason() { return super.getCell("EndReason");}
 public CsvCell getSCTConsideredDate() { return super.getCell("SCTConsideredDate");}
 public CsvCell getLeaveApproved() { return super.getCell("LeaveApproved");}
 public CsvCell getIDSection() { return super.getCell("IDSection");}
 public CsvCell getIDHospitalAdmission() { return super.getCell("IDHospitalAdmission");}
 public CsvCell getIDHospitalAdmissionAndDischarge() { return super.getCell("IDHospitalAdmissionAndDischarge");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getFreeBed() { return super.getCell("FreeBed");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRPatientLeave Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
