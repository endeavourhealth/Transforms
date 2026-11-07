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

        if (version.equals(TppCsvToFhirTransformer.VERSION_A)) {
            //used in test packs and early feeds
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
                    "IDBranch"
            };

        } else if (version.equals(TppCsvToFhirTransformer.VERSION_B)) {
            //version of above with RemovedData col
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
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_C)) {
            //adding DateStart to the VERSION_A
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
                    "DateStart", //new field
                    "AllowOverBooking",
                    "BookingContactNumber",
                    "IDAppointmentRoom",
                    "IDBranch"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_D)) {
            //version of above with RemovedData
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
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_E)) {
            //SD-453 two new cols added
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
                    "Specialty", //new field
                    "TreatmentCode" //new field
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_F)) {
            //version of above with RemovedData
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
                    "Specialty", //new field
                    "TreatmentCode", //new field
                    "RemovedData" //new field
            };
        } else {
            throw new IllegalArgumentException("Unsupported version " + version);
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

    public CsvCell getSpecialty() {
        return super.getCell("Specialty");
    }

    public CsvCell getTreatmentCode() {
        return super.getCell("TreatmentCode");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
