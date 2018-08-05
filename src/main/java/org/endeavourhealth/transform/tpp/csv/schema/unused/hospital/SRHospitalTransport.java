package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalTransport extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRHospitalTransport.class);

    public SRHospitalTransport(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "TransportType",
                "TransportDirection",
                "DateRequired",
                "DateArranged",
                "DateCompleted",
                "DateCancelled",
                "DestinationHouseName",
                "DestinationHouseNumber",
                "DestinationPostCode",
                "TelephoneNumber",
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

    public CsvCell getTransportType() {
        return super.getCell("TransportType");
    }

    public CsvCell getTransportDirection() {
        return super.getCell("TransportDirection");
    }

    public CsvCell getDateRequired() {
        return super.getCell("DateRequired");
    }

    public CsvCell getDateArranged() {
        return super.getCell("DateArranged");
    }

    public CsvCell getDateCompleted() {
        return super.getCell("DateCompleted");
    }

    public CsvCell getDateCancelled() {
        return super.getCell("DateCancelled");
    }

    public CsvCell getDestinationHouseName() {
        return super.getCell("DestinationHouseName");
    }

    public CsvCell getDestinationHouseNumber() {
        return super.getCell("DestinationHouseNumber");
    }

    public CsvCell getDestinationPostCode() {
        return super.getCell("DestinationPostCode");
    }

    public CsvCell getTelephoneNumber() {
        return super.getCell("TelephoneNumber");
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


    //fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRHospitalTransport Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
