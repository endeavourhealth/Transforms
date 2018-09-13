package org.endeavourhealth.transform.tpp.csv.schema.unused.mentalhealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRMHAWOL extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRMHAWOL.class);

    public SRMHAWOL(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "AWOLDepartedDate",
                "AWOLRecordedBy",
                "AWOLEndDate",
                "AWOLEndReason",
                "AWOLEndRecordedBy",
                "DateCreation",
                "IDHospitalAdmission",
                "IDHospitalAdmissionAndDischarge",
                "IDSection",
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

    public CsvCell getAWOLDepartedDate() {
        return super.getCell("AWOLDepartedDate");
    }

    public CsvCell getAWOLRecordedBy() {
        return super.getCell("AWOLRecordedBy");
    }

    public CsvCell getAWOLEndDate() {
        return super.getCell("AWOLEndDate");
    }

    public CsvCell getAWOLEndReason() {
        return super.getCell("AWOLEndReason");
    }

    public CsvCell getAWOLEndRecordedBy() {
        return super.getCell("AWOLEndRecordedBy");
    }

    public CsvCell getDateCreation() {
        return super.getCell("DateCreation");
    }

    public CsvCell getIDHospitalAdmission() {
        return super.getCell("IDHospitalAdmission");
    }

    public CsvCell getIDHospitalAdmissionAndDischarge() {
        return super.getCell("IDHospitalAdmissionAndDischarge");
    }

    public CsvCell getIDSection() {
        return super.getCell("IDSection");
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
