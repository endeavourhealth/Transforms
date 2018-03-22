package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRRota extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRRota.class); 

  public SRRota(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "Name",
                      "RotaType",
                      "Location",
                      "Code",
                      "IDProfileOwner",
                      "AllowOverBooking",
                      "BookingContactNumber",
                      "IDAppointmentRoom",
                      "IDBranch"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateCreation() { return super.getCell("DateCreation");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getName() { return super.getCell("Name");};
 public CsvCell getRotaType() { return super.getCell("RotaType");};
 public CsvCell getLocation() { return super.getCell("Location");};
 public CsvCell getCode() { return super.getCell("Code");};
 public CsvCell getIDProfileOwner() { return super.getCell("IDProfileOwner");};
 public CsvCell getAllowOverBooking() { return super.getCell("AllowOverBooking");};
 public CsvCell getBookingContactNumber() { return super.getCell("BookingContactNumber");};
 public CsvCell getIDAppointmentRoom() { return super.getCell("IDAppointmentRoom");};
 public CsvCell getIDBranch() { return super.getCell("IDBranch");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRRota Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
