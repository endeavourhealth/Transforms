package org.endeavourhealth.transform.tpp.csv.schema.unused.theatre;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRTheatreSessionAttendees extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRTheatreSessionAttendees.class);

    public SRTheatreSessionAttendees(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateEventRecorded",
                "DateEvent",
                "IDProfileEnteredBy",
                "IDDoneBy",
                "TextualEventDoneBy",
                "IDOrganisationDoneAt",
                "IDStaffAttendee",
                "IDProfileAttendee",
                "StaffAttendeeRole",
                "DateArrival",
                "DateDeparted",
                "IdBooking",
                "IDTheatreBooking",
                "IDEvent",
                "IDPatient",
                "IDOrganisation",
                "RemovedData"


        };

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateEventRecorded() {
        return super.getCell("DateEventRecorded");
    }

    public CsvCell getDateEvent() {
        return super.getCell("DateEvent");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getIDDoneBy() {
        return super.getCell("IDDoneBy");
    }

    public CsvCell getTextualEventDoneBy() {
        return super.getCell("TextualEventDoneBy");
    }

    public CsvCell getIDOrganisationDoneAt() {
        return super.getCell("IDOrganisationDoneAt");
    }

    public CsvCell getIDStaffAttendee() {
        return super.getCell("IDStaffAttendee");
    }

    public CsvCell getIDProfileAttendee() {
        return super.getCell("IDProfileAttendee");
    }

    public CsvCell getStaffAttendeeRole() {
        return super.getCell("StaffAttendeeRole");
    }

    public CsvCell getDateArrival() {
        return super.getCell("DateArrival");
    }

    public CsvCell getDateDeparted() {
        return super.getCell("DateDeparted");
    }

    public CsvCell getIdBooking() {
        return super.getCell("IdBooking");
    }

    public CsvCell getIDTheatreBooking() {
        return super.getCell("IDTheatreBooking");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    //fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRTheatreSessionAttendees Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
