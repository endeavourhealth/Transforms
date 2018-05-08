package org.endeavourhealth.transform.tpp.csv.schema.patient;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPatientAddressHistory extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientAddressHistory.class);

    public SRPatientAddressHistory(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "NameOfBuilding",
                    "NumberOfBuilding",
                    "NameOfRoad",
                    "NameOfLocality",
                    "NameOfTown",
                    "NameOfCounty",
                    "FullPostCode",
                    "DateTo",
                    "AddressType",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "CcgOfResidence"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String [] {
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "NameOfBuilding",
                    "NumberOfBuilding",
                    "NameOfRoad",
                    "NameOfLocality",
                    "NameOfTown",
                    "NameOfCounty",
                    "FullPostCode",
                    "DateTo",
                    "AddressType",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "CcgOfResidence",
                    "IDOrganisationRegisteredAt"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "NameOfBuilding",
                    "NumberOfBuilding",
                    "NameOfRoad",
                    "NameOfLocality",
                    "NameOfTown",
                    "NameOfCounty",
                    "FullPostCode",
                    "DateTo",
                    "AddressType",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "CcgOfResidence",
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

    public CsvCell getDateEvent() {
        return super.getCell("DateEvent");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getIDDoneBy() {
        return super.getCell("IDDoneBy");
    }

    public CsvCell getTextualEventDoneBy() {
        return super.getCell("TextualEventDoneBy");
    }

    public CsvCell getIDOrganisationDoneAt() {
        return super.getCell("IDOrganisationDoneAt");
    }

    public CsvCell getNameOfBuilding() {
        return super.getCell("NameOfBuilding");
    }

    public CsvCell getNumberOfBuilding() {
        return super.getCell("NumberOfBuilding");
    }

    public CsvCell getNameOfRoad() {
        return super.getCell("NameOfRoad");
    }

    public CsvCell getNameOfLocality() {
        return super.getCell("NameOfLocality");
    }

    public CsvCell getNameOfTown() {
        return super.getCell("NameOfTown");
    }

    public CsvCell getNameOfCounty() {
        return super.getCell("NameOfCounty");
    }

    public CsvCell getFullPostCode() {
        return super.getCell("FullPostCode");
    }

    public CsvCell getDateTo() {
        return super.getCell("DateTo");
    }

    public CsvCell getAddressType() {
        return super.getCell("AddressType");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getCcgOfResidence() {
        return super.getCell("CcgOfResidence");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected String getFileTypeDescription() {
        return "TPP Patient Address History Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}
