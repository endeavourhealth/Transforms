package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRWardStay extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRWardStay.class);

    public SRWardStay(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "IDProfileEnteredBy",
                "PlaceName",
                "DateStayStart",
                "DateExpectedEnd",
                "DateEnd",
                "IDHospitalAdmissionAndDischarge",
                "IDReferralIn",
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

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getPlaceName() {
        return super.getCell("PlaceName");
    }

    public CsvCell getDateStayStart() {
        return super.getCell("DateStayStart");
    }

    public CsvCell getDateExpectedEnd() {
        return super.getCell("DateExpectedEnd");
    }

    public CsvCell getDateEnd() {
        return super.getCell("DateEnd");
    }

    public CsvCell getIDHospitalAdmissionAndDischarge() {
        return super.getCell("IDHospitalAdmissionAndDischarge");
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

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
