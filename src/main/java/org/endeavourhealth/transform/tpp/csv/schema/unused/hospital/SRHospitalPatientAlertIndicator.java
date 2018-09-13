package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalPatientAlertIndicator extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRHospitalPatientAlertIndicator.class);

    public SRHospitalPatientAlertIndicator(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateDeleted",
                "RecurringAlert",
                "DateDormant",
                "DateReview",
                "IDHospitalAlertIndicator",
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

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    public CsvCell getRecurringAlert() {
        return super.getCell("RecurringAlert");
    }

    public CsvCell getDateDormant() {
        return super.getCell("DateDormant");
    }

    public CsvCell getDateReview() {
        return super.getCell("DateReview");
    }

    public CsvCell getIDHospitalAlertIndicator() {
        return super.getCell("IDHospitalAlertIndicator");
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
