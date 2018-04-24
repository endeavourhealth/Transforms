package org.endeavourhealth.transform.tpp.csv.schema.patient;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPatientRegistration extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientRegistration.class);

    public SRPatientRegistration(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        //TODO - update transform to check for null cells when using fields not in the older version
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "IDProfileEnteredBy",
                    "DateRegistration",
                    "DateDeRegistration",
                    "RegistrationStatus",
                    "PreferredPharmacy",
                    "IDPatient",
                    "IDOrganisation"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "IDProfileEnteredBy",
                    "DateRegistration",
                    "DateDeRegistration",
                    "RegistrationStatus",
                    "PreferredPharmacy",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        }
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

    public CsvCell getDateRegistration() {
        return super.getCell("DateRegistration");
    }

    public CsvCell getDateDeRegistration() {
        return super.getCell("DateDeRegistration");
    }

    public CsvCell getRegistrationStatus() {
        return super.getCell("RegistrationStatus");
    }

    public CsvCell getPreferredPharmacy() {
        return super.getCell("PreferredPharmacy");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getIDOrganisationRegisteredAt() {
        return super.getCell("IDOrganisationRegisteredAt");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    //TODO fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP Patient Registration Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
