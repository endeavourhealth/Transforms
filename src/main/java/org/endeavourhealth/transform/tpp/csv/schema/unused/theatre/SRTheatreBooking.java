package org.endeavourhealth.transform.tpp.csv.schema.unused.theatre;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRTheatreBooking extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRTheatreBooking.class);

    public SRTheatreBooking(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateStart",
                "DateEnd",
                "AnaestheticType",
                "DateAnaestheticAdministered",
                "DateAnaestheticReady",
                "DateInTheatre",
                "DateInRecovery",
                "MainProcedure",
                "SecondaryProcedure",
                "Priority",
                "DateCreation",
                "DateCancellation",
                "CancellationReason",
                "IDTheatreSession",
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

    public CsvCell getDateStart() {
        return super.getCell("DateStart");
    }

    public CsvCell getDateEnd() {
        return super.getCell("DateEnd");
    }

    public CsvCell getAnaestheticType() {
        return super.getCell("AnaestheticType");
    }

    public CsvCell getDateAnaestheticAdministered() {
        return super.getCell("DateAnaestheticAdministered");
    }

    public CsvCell getDateAnaestheticReady() {
        return super.getCell("DateAnaestheticReady");
    }

    public CsvCell getDateInTheatre() {
        return super.getCell("DateInTheatre");
    }

    public CsvCell getDateInRecovery() {
        return super.getCell("DateInRecovery");
    }

    public CsvCell getMainProcedure() {
        return super.getCell("MainProcedure");
    }

    public CsvCell getSecondaryProcedure() {
        return super.getCell("SecondaryProcedure");
    }

    public CsvCell getPriority() {
        return super.getCell("Priority");
    }

    public CsvCell getDateCreation() {
        return super.getCell("DateCreation");
    }

    public CsvCell getDateCancellation() {
        return super.getCell("DateCancellation");
    }

    public CsvCell getCancellationReason() {
        return super.getCell("CancellationReason");
    }

    public CsvCell getIDTheatreSession() {
        return super.getCell("IDTheatreSession");
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
