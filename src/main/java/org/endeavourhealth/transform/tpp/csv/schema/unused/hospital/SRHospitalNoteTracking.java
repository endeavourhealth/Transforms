package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalNoteTracking extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRHospitalNoteTracking.class);

    public SRHospitalNoteTracking(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "VolumeNumber",
                "Specialty",
                "DateCreated",
                "IDProfileCreatedBy",
                "DateStart",
                "DateEnd",
                "Archived",
                "IDProfileArchivedBy",
                "Destroyed",
                "IDProfileDestroyedBy",
                "IDLocation",
                "HomeLocation",
                "IDLocationHome",
                "Facia",
                "IDPatient",
                "IDOrganisation"


        };

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getVolumeNumber() {
        return super.getCell("VolumeNumber");
    }

    public CsvCell getSpecialty() {
        return super.getCell("Specialty");
    }

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getIDProfileCreatedBy() {
        return super.getCell("IDProfileCreatedBy");
    }

    public CsvCell getDateStart() {
        return super.getCell("DateStart");
    }

    public CsvCell getDateEnd() {
        return super.getCell("DateEnd");
    }

    public CsvCell getArchived() {
        return super.getCell("Archived");
    }

    public CsvCell getIDProfileArchivedBy() {
        return super.getCell("IDProfileArchivedBy");
    }

    public CsvCell getDestroyed() {
        return super.getCell("Destroyed");
    }

    public CsvCell getIDProfileDestroyedBy() {
        return super.getCell("IDProfileDestroyedBy");
    }

    public CsvCell getIDLocation() {
        return super.getCell("IDLocation");
    }

    public CsvCell getHomeLocation() {
        return super.getCell("HomeLocation");
    }

    public CsvCell getIDLocationHome() {
        return super.getCell("IDLocationHome");
    }

    public CsvCell getFacia() {
        return super.getCell("Facia");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }


    //fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRHospitalNoteTracking Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
