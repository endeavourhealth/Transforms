package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRStaffActivity extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRStaffActivity.class); 

  public SRStaffActivity(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateEventRecorded",
                      "IDProfileEnteredBy",
                      "DateActivity",
                      "IDProfileDoneBy",
                      "DoneByTeam",
                      "ActivityType",
                      "Attendance",
                      "MaleAttendance",
                      "FemaleAttendance",
                      "LowerAgeAttendance",
                      "UpperAgeAttendance",
                      "Location",
                      "TimeTaken",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getDateActivity() { return super.getCell("DateActivity");}
 public CsvCell getIDProfileDoneBy() { return super.getCell("IDProfileDoneBy");}
 public CsvCell getDoneByTeam() { return super.getCell("DoneByTeam");}
 public CsvCell getActivityType() { return super.getCell("ActivityType");}
 public CsvCell getAttendance() { return super.getCell("Attendance");}
 public CsvCell getMaleAttendance() { return super.getCell("MaleAttendance");}
 public CsvCell getFemaleAttendance() { return super.getCell("FemaleAttendance");}
 public CsvCell getLowerAgeAttendance() { return super.getCell("LowerAgeAttendance");}
 public CsvCell getUpperAgeAttendance() { return super.getCell("UpperAgeAttendance");}
 public CsvCell getLocation() { return super.getCell("Location");}
 public CsvCell getTimeTaken() { return super.getCell("TimeTaken");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRStaffActivity Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
