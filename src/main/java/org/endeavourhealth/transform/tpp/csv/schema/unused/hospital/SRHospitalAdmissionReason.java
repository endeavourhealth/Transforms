package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalAdmissionReason extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRHospitalAdmissionReason.class);

    public SRHospitalAdmissionReason(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "IDHospitalAdmission",
                "IDPatient",
                "AdmissionReason",
                "DateDeleted",
                "DateModified",
                "RemovedData"


        };

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getIDHospitalAdmission() {
        return super.getCell("IDHospitalAdmission");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getAdmissionReason() {
        return super.getCell("AdmissionReason");
    }

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    public CsvCell getDateModified() {
        return super.getCell("DateModified");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    //fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRHospitalAdmissionReason Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
