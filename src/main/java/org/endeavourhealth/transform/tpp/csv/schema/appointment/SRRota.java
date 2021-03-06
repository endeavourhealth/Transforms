package org.endeavourhealth.transform.tpp.csv.schema.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRRota extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRRota.class);

    public SRRota(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateCreation",
                    "IDProfileCreatedBy",
                    "Name",
                    "RotaType",
                    "Location",
                    "Code",
                    "IDProfileOwner",
                    "AllowOverBooking",
                    "BookingContactNumber",
                    "IDAppointmentRoom",
                    "IDBranch",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_92)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateCreation",
                    "IDProfileCreatedBy",
                    "Name",
                    "RotaType",
                    "Location",
                    "Code",
                    "IDProfileOwner",
                    "DateStart",
                    "AllowOverBooking",
                    "BookingContactNumber",
                    "IDAppointmentRoom",
                    "IDBranch"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_93)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateCreation",
                    "IDProfileCreatedBy",
                    "Name",
                    "RotaType",
                    "Location",
                    "Code",
                    "IDProfileOwner",
                    "DateStart",
                    "AllowOverBooking",
                    "BookingContactNumber",
                    "IDAppointmentRoom",
                    "IDBranch",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateCreation",
                    "IDProfileCreatedBy",
                    "Name",
                    "RotaType",
                    "Location",
                    "Code",
                    "IDProfileOwner",
                    "AllowOverBooking",
                    "BookingContactNumber",
                    "IDAppointmentRoom",
                    "IDBranch",
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateCreation() {
        return super.getCell("DateCreation");
    }

    public CsvCell getIDProfileCreatedBy() {
        return super.getCell("IDProfileCreatedBy");
    }

    public CsvCell getName() {
        return super.getCell("Name");
    }

    public CsvCell getRotaType() {
        return super.getCell("RotaType");
    }

    public CsvCell getLocation() {
        return super.getCell("Location");
    }

    public CsvCell getCode() {
        return super.getCell("Code");
    }

    /**
     * NOTE: this column is NOT what it appears to be. It does not contain details on who
     * the owning practitioner of the rota is. According to the TPP spec this is only used in
     * secondary care settings.
     */
    public CsvCell getIDProfileOwner() {
        return super.getCell("IDProfileOwner");
    }

    /**
     * datetime e.g. 30 Dec 2020 08:00:00
     */
    public CsvCell getDateStart() {
        return super.getCell("DateStart");
    }

    public CsvCell getAllowOverBooking() {
        return super.getCell("AllowOverBooking");
    }

    public CsvCell getBookingContactNumber() {
        return super.getCell("BookingContactNumber");
    }

    public CsvCell getIDAppointmentRoom() {
        return super.getCell("IDAppointmentRoom");
    }

    public CsvCell getIDBranch() {
        return super.getCell("IDBranch");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
