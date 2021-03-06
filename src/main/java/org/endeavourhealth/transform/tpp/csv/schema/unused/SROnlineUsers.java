package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROnlineUsers extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SROnlineUsers.class);

    public SROnlineUsers(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "Title",
                "FirstName",
                "MiddleNames",
                "Surname",
                "NHSNumber",
                "DateBirth",
                "Gender",
                "EmailAddress",
                "RemovedData"


        };

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getTitle() {
        return super.getCell("Title");
    }

    public CsvCell getFirstName() {
        return super.getCell("FirstName");
    }

    public CsvCell getMiddleNames() {
        return super.getCell("MiddleNames");
    }

    public CsvCell getSurname() {
        return super.getCell("Surname");
    }

    public CsvCell getNHSNumber() {
        return super.getCell("NHSNumber");
    }

    public CsvCell getDateBirth() {
        return super.getCell("DateBirth");
    }

    public CsvCell getGender() {
        return super.getCell("Gender");
    }

    public CsvCell getEmailAddress() {
        return super.getCell("EmailAddress");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
