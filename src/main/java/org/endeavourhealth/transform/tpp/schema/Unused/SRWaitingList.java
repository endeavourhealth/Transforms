package org.endeavourhealth.transform.tpp.schema.Unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRWaitingList extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRWaitingList.class); 

  public SRWaitingList(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateAdded",
                      "DateAppointmentStart",
                      "DateAppointmentEnd",
                      "RotaType",
                      "RestrictedToCaseload",
                      "TargetMaximumWait",
                      "IDClinician",
                      "IDProfileClinician",
                      "Priority",
                      "DateDue",
                      "Notes",
                      "DateWaitCompleted",
                      "DateDeletedFromWaitingList",
                      "IDReferralIn",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getWaitingListName() { return super.getCell("WaitingListName");};
 public CsvCell getDateAdded() { return super.getCell("DateAdded");};
 public CsvCell getDateAppointmentStart() { return super.getCell("DateAppointmentStart");};
 public CsvCell getDateAppointmentEnd() { return super.getCell("DateAppointmentEnd");};
 public CsvCell getRotaType() { return super.getCell("RotaType");};
 public CsvCell getRestrictedToCaseload() { return super.getCell("RestrictedToCaseload");};
 public CsvCell getTargetMaximumWait() { return super.getCell("TargetMaximumWait");};
 public CsvCell getIDClinician() { return super.getCell("IDClinician");};
 public CsvCell getIDProfileClinician() { return super.getCell("IDProfileClinician");};
 public CsvCell getPriority() { return super.getCell("Priority");};
 public CsvCell getDateDue() { return super.getCell("DateDue");};
 public CsvCell getNotes() { return super.getCell("Notes");};
 public CsvCell getDateWaitCompleted() { return super.getCell("DateWaitCompleted");};
 public CsvCell getDateDeletedFromWaitingList() { return super.getCell("DateDeletedFromWaitingList");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRWaitingList Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
