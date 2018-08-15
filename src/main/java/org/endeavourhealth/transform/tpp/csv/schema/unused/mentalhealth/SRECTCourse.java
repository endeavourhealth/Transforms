package org.endeavourhealth.transform.tpp.csv.schema.unused.mentalhealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRECTCourse extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRECTCourse.class); 

  public SRECTCourse(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "CourseStartDate",
                      "CourseEndDate",
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getCourseStartDate() { return super.getCell("CourseStartDate");}
 public CsvCell getCourseEndDate() { return super.getCell("CourseEndDate");}
 public CsvCell getDateCreation() { return super.getCell("DateCreation");}
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");}
 public CsvCell getIDEvent() { return super.getCell("IDEvent");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRECTCourse Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
