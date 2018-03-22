package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SR18WeekWait extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SR18WeekWait.class);

    public SR18WeekWait(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateEventRecorded",
                "DateEvent",
                "IDProfileEnteredBy",
                "IDDoneBy",
                "TextualEventDoneBy",
                "IDNumber",
                "IDOrganisationDoneAt",
                "TreatmentFunction",
                "DateStart",
                "DateExpectedEnd",
                "DateCompleted",
                "DateAppointmentOffered",
                "ReasonableOffer",
                "ClockStarted",
                "DateStatus",
                "Status",
                "IDReferralIn",
                "IDEvent",
                "IDPatient",
                "IDOrganisation",
                "RemovedData"


        };

    }
    public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
    public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
    public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");};
    public CsvCell getDateEvent() { return super.getCell("DateEvent");};
    public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");};
    public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");};
    public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");};
    public CsvCell getIDNumber() { return super.getCell("IDNumber");};
    public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");};
    public CsvCell getTreatmentFunction() { return super.getCell("TreatmentFunction");};
    public CsvCell getDateStart() { return super.getCell("DateStart");};
    public CsvCell getDateExpectedEnd() { return super.getCell("DateExpectedEnd");};
    public CsvCell getDateCompleted() { return super.getCell("DateCompleted");};
    public CsvCell getDateAppointmentOffered() { return super.getCell("DateAppointmentOffered");};
    public CsvCell getReasonableOffer() { return super.getCell("ReasonableOffer");};
    public CsvCell getClockStarted() { return super.getCell("ClockStarted");};
    public CsvCell getDateStatus() { return super.getCell("DateStatus");};
    public CsvCell getStatus() { return super.getCell("Status");};
    public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
    public CsvCell getIDEvent() { return super.getCell("IDEvent");};
    public CsvCell getIDPatient() { return super.getCell("IDPatient");};
    public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
    public CsvCell getRemovedData() { return super.getCell("RemovedData");};


    //TODO file the string below
    @Override
    protected String getFileTypeDescription() {return "TPP SR18WeekWait Entry file ";}

    @Override
    protected boolean isFileAudited() {return true;}
}
