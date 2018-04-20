package org.endeavourhealth.transform.tpp.csv.schema.unused.theatre;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRTheatreSession extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRTheatreSession.class);

    public SRTheatreSession(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateCreated",
                "DateDeleted",
                "DatePreparationStarted",
                "DateTheatreSessionStarted",
                "DateTheatreSessionEnded",
                "DateClearUpOfTheatreFinished",
                "TheatreDescription",
                "Specialty",
                "IDConsultant",
                "IDProfileConsultant",
                "IDAnaesthetist",
                "IDProfileAnaesthetist",
                "IDOda",
                "IDProfileOda",
                "IDOrganisation"


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

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    public CsvCell getDatePreparationStarted() {
        return super.getCell("DatePreparationStarted");
    }

    public CsvCell getDateTheatreSessionStarted() {
        return super.getCell("DateTheatreSessionStarted");
    }

    public CsvCell getDateTheatreSessionEnded() {
        return super.getCell("DateTheatreSessionEnded");
    }

    public CsvCell getDateClearUpOfTheatreFinished() {
        return super.getCell("DateClearUpOfTheatreFinished");
    }

    public CsvCell getTheatreDescription() {
        return super.getCell("TheatreDescription");
    }

    public CsvCell getSpecialty() {
        return super.getCell("Specialty");
    }

    public CsvCell getIDConsultant() {
        return super.getCell("IDConsultant");
    }

    public CsvCell getIDProfileConsultant() {
        return super.getCell("IDProfileConsultant");
    }

    public CsvCell getIDAnaesthetist() {
        return super.getCell("IDAnaesthetist");
    }

    public CsvCell getIDProfileAnaesthetist() {
        return super.getCell("IDProfileAnaesthetist");
    }

    public CsvCell getIDOda() {
        return super.getCell("IDOda");
    }

    public CsvCell getIDProfileOda() {
        return super.getCell("IDProfileOda");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }


    //TODO fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRTheatreSession Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
