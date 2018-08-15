package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPatientGroups extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRPatientGroups.class); 

  public SRPatientGroups(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateEvent",
                      "IDProfileEnteredBy",
                      "Name",
                      "GroupType",
                      "Members",
                      "DateRemoved",
                      "IDStaffRemoved",
                      "IDProfileRemoved",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
 public CsvCell getDateEvent() { return super.getCell("DateEvent");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getName() { return super.getCell("Name");}
 public CsvCell getGroupType() { return super.getCell("GroupType");}
 public CsvCell getMembers() { return super.getCell("Members");}
 public CsvCell getDateRemoved() { return super.getCell("DateRemoved");}
 public CsvCell getIDStaffRemoved() { return super.getCell("IDStaffRemoved");}
 public CsvCell getIDProfileRemoved() { return super.getCell("IDProfileRemoved");}
 public CsvCell getIDEvent() { return super.getCell("IDEvent");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRPatientGroups Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
