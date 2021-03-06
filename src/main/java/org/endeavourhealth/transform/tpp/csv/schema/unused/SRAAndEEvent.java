package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAAndEEvent extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRAAndEEvent.class);

    public SRAAndEEvent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateCreated",
                "IdProfileCreatedBy",
                "IDAttendance",
                "IDAAndEAttendance",
                "DateEventPerformed",
                "EventType",
                "IDProfileEventPerformedBy",
                "DateEventDeleted",
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

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getIdProfileCreatedBy() {
        return super.getCell("IdProfileCreatedBy");
    }

    public CsvCell getIDAttendance() {
        return super.getCell("IDAttendance");
    }

    public CsvCell getIDAAndEAttendance() {
        return super.getCell("IDAAndEAttendance");
    }

    public CsvCell getDateEventPerformed() {
        return super.getCell("DateEventPerformed");
    }

    public CsvCell getEventType() {
        return super.getCell("EventType");
    }

    public CsvCell getIDProfileEventPerformedBy() {
        return super.getCell("IDProfileEventPerformedBy");
    }

    public CsvCell getDateEventDeleted() {
        return super.getCell("DateEventDeleted");
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
