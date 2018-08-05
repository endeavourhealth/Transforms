package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalWardBooking extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRHospitalWardBooking.class);

    public SRHospitalWardBooking(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "IdProfileCreatedBy",
                "WardBookingName",
                "DateWardStart",
                "DateWardEnd",
                "WardBookingStatus",
                "WardReady",
                "IDIntendedAdmission",
                "IDHospitalAdmissionAndDischarge",
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

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getIdProfileCreatedBy() {
        return super.getCell("IdProfileCreatedBy");
    }

    public CsvCell getWardBookingName() {
        return super.getCell("WardBookingName");
    }

    public CsvCell getDateWardStart() {
        return super.getCell("DateWardStart");
    }

    public CsvCell getDateWardEnd() {
        return super.getCell("DateWardEnd");
    }

    public CsvCell getWardBookingStatus() {
        return super.getCell("WardBookingStatus");
    }

    public CsvCell getWardReady() {
        return super.getCell("WardReady");
    }

    public CsvCell getIDIntendedAdmission() {
        return super.getCell("IDIntendedAdmission");
    }

    public CsvCell getIDHospitalAdmissionAndDischarge() {
        return super.getCell("IDHospitalAdmissionAndDischarge");
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
        return "TPP SRHospitalWardBooking Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
