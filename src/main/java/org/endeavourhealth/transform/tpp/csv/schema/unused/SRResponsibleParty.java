package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRResponsibleParty extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRResponsibleParty.class);

    public SRResponsibleParty(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateEventRecorded",
                "IDProfileEnteredBy",
                "ResponsiblePartyType",
                "IDProfileResponsibleParty",
                "DateStart",
                "DateEnd",
                "IDHospitalAdmission",
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

    public CsvCell getDateEventRecorded() {
        return super.getCell("DateEventRecorded");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getResponsiblePartyType() {
        return super.getCell("ResponsiblePartyType");
    }

    public CsvCell getIDProfileResponsibleParty() {
        return super.getCell("IDProfileResponsibleParty");
    }

    public CsvCell getDateStart() {
        return super.getCell("DateStart");
    }

    public CsvCell getDateEnd() {
        return super.getCell("DateEnd");
    }

    public CsvCell getIDHospitalAdmission() {
        return super.getCell("IDHospitalAdmission");
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


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
