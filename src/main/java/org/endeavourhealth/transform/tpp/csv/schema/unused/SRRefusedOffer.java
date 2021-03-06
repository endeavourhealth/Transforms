package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRRefusedOffer extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRRefusedOffer.class);

    public SRRefusedOffer(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "OfferType",
                "DateOfferedFor",
                "DateOfferMade",
                "IDProfileOfferMadeBy",
                "DateOfferRefused",
                "Reasonable",
                "DateDeleted",
                "IDHospitalWaitingList",
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

    public CsvCell getOfferType() {
        return super.getCell("OfferType");
    }

    public CsvCell getDateOfferedFor() {
        return super.getCell("DateOfferedFor");
    }

    public CsvCell getDateOfferMade() {
        return super.getCell("DateOfferMade");
    }

    public CsvCell getIDProfileOfferMadeBy() {
        return super.getCell("IDProfileOfferMadeBy");
    }

    public CsvCell getDateOfferRefused() {
        return super.getCell("DateOfferRefused");
    }

    public CsvCell getReasonable() {
        return super.getCell("Reasonable");
    }

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    public CsvCell getIDHospitalWaitingList() {
        return super.getCell("IDHospitalWaitingList");
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
