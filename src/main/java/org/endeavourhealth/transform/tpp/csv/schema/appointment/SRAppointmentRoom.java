package org.endeavourhealth.transform.tpp.csv.schema.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAppointmentRoom extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRAppointmentRoom.class);

    public SRAppointmentRoom(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if  (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Name",
                    "DateCreated",
                    "IDCreatedBy",
                    "IDProfileCreatedBy",
                    "DateDeleted",
                    "IDDeletedBy",
                    "IDProfileDeletedBy",
                    "IdBranch",
                    "IDOrganisationBranch",
                    "IDLocation",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_92)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Name",
                    "DateCreated",
                    "IDCreatedBy",
                    "IDProfileCreatedBy",
                    "DateDeleted",
                    "IDDeletedBy",
                    "IDProfileDeletedBy",
                    "IdBranch",
                    "IDOrganisationBranch",
                    "IDLocation"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Name",
                    "DateCreated",
                    "IDCreatedBy",
                    "IDProfileCreatedBy",
                    "DateDeleted",
                    "IDDeletedBy",
                    "IDProfileDeletedBy",
                    "IdBranch",
                    "IDOrganisationBranch",
                    "IDLocation"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getName() {
        return super.getCell("Name");
    }

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getIDCreatedBy() {
        return super.getCell("IDCreatedBy");
    }

    public CsvCell getIDProfileCreatedBy() {
        return super.getCell("IDProfileCreatedBy");
    }

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    public CsvCell getIDDeletedBy() {
        return super.getCell("IDDeletedBy");
    }

    public CsvCell getIDProfileDeletedBy() {
        return super.getCell("IDProfileDeletedBy");
    }

    public CsvCell getIdBranch() {
        return super.getCell("IdBranch");
    }

    public CsvCell getIDOrganisationBranch() {
        return super.getCell("IDOrganisationBranch");
    }

    public CsvCell getIDLocation() {
        return super.getCell("IDLocation");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
