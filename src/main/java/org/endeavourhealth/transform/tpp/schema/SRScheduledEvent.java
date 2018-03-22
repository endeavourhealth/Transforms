package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRScheduledEvent extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRScheduledEvent.class); 

  public SRScheduledEvent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "ScheduleAction",
                      "DateAppointmentExact",
                      "DateAppointmentBetweenStart",
                      "DateAppointmentBetweenEnd",
                      "DateAppointmentWeekCommencing",
                      "DateScheduled",
                      "IDProfileRecordedBy",
                      "EventStatus",
                      "IDProfileUpdatedBy",
                      "BatchNumber",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getTreatmentCentreName() { return super.getCell("TreatmentCentreName");};
 public CsvCell getTreatmentCentrePostCode() { return super.getCell("TreatmentCentrePostCode");};
 public CsvCell getScheduleAction() { return super.getCell("ScheduleAction");};
 public CsvCell getDateAppointmentExact() { return super.getCell("DateAppointmentExact");};
 public CsvCell getDateAppointmentBetweenStart() { return super.getCell("DateAppointmentBetweenStart");};
 public CsvCell getDateAppointmentBetweenEnd() { return super.getCell("DateAppointmentBetweenEnd");};
 public CsvCell getDateAppointmentWeekCommencing() { return super.getCell("DateAppointmentWeekCommencing");};
 public CsvCell getDateScheduled() { return super.getCell("DateScheduled");};
 public CsvCell getIDProfileRecordedBy() { return super.getCell("IDProfileRecordedBy");};
 public CsvCell getEventStatus() { return super.getCell("EventStatus");};
 public CsvCell getIDProfileUpdatedBy() { return super.getCell("IDProfileUpdatedBy");};
 public CsvCell getBatchNumber() { return super.getCell("BatchNumber");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRScheduledEvent Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
