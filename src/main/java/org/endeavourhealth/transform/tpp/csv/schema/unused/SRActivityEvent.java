package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRActivityEvent extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRActivityEvent.class);

    public SRActivityEvent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "IDDoneBy",
                "TextualEventDoneBy",
                "IDOrganisationDoneAt",
                "EventDetails",
                "EventDetailsWith",
                "EventDetailsMethod",
                "EventDetailsDuration",
                "IDEvent",
                "IDReferralIn",
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

    public CsvCell getEventDetails() {
        return super.getCell("EventDetails");
    }

    public CsvCell getEventDetailsWith() {
        return super.getCell("EventDetailsWith");
    }

    public CsvCell getEventDetailsMethod() {
        return super.getCell("EventDetailsMethod");
    }

    public CsvCell getEventDetailsDuration() {
        return super.getCell("EventDetailsDuration");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getIDReferralIn() {
        return super.getCell("IDReferralIn");
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


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
