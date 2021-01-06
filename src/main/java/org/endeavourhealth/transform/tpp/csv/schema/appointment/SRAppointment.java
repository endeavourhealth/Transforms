package org.endeavourhealth.transform.tpp.csv.schema.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAppointment extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRAppointment.class);

    public SRAppointment(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_90) ||
                version.equals(TppCsvToFhirTransformer.VERSION_89) ||
                version.equals(TppCsvToFhirTransformer.VERSION_87) ||
                        version.equals(TppCsvToFhirTransformer.VERSION_93)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateStart",
                    "DateEnd",
                    "AppointmentStatus",
                    "DateAppointmentBooked",
                    "DatePatientArrival",
                    "DatePatientSeen",
                    "FollowUpAppointment",
                    "TelephoneAppointment",
                    "IDClinician",
                    "IDProfileClinician",
                    "RotaName",
                    "RotaType",
                    "RotaLocation",
                    "RotaCode",
                    "IDRotaOwner",
                    "IDProfileRotaOwner",
                    "AllowsOverbooking",
                    "BookingContactNumber",
                    "DateAppointmentCancelled",
                    "IDRota",
                    "IDReferralIn",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_91) ||
                version.equals(TppCsvToFhirTransformer.VERSION_88) ||
                version.equals(TppCsvToFhirTransformer.VERSION_92)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateStart",
                    "DateEnd",
                    "AppointmentStatus",
                    "DateAppointmentBooked",
                    "DatePatientArrival",
                    "DatePatientSeen",
                    "FollowUpAppointment",
                    "TelephoneAppointment",
                    "IDClinician",
                    "IDProfileClinician",
                    "RotaName",
                    "RotaType",
                    "RotaLocation",
                    "RotaCode",
                    "IDRotaOwner",
                    "IDProfileRotaOwner",
                    "AllowsOverbooking",
                    "BookingContactNumber",
                    "DateAppointmentCancelled",
                    "IDRota",
                    "IDReferralIn",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };

        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateStart",
                    "DateEnd",
                    "AppointmentStatus",
                    "DateAppointmentBooked",
                    "DatePatientArrival",
                    "DatePatientSeen",
                    "FollowUpAppointment",
                    "TelephoneAppointment",
                    "IDClinician",
                    "IDProfileClinician",
                    "RotaName",
                    "RotaType",
                    "RotaLocation",
                    "RotaCode",
                    "IDRotaOwner",
                    "IDProfileRotaOwner",
                    "AllowsOverbooking",
                    "BookingContactNumber",
                    "DateAppointmentCancelled",
                    "IDRota",
                    "IDReferralIn",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };

        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateStart() {
        return super.getCell("DateStart");
    }

    public CsvCell getDateEnd() {
        return super.getCell("DateEnd");
    }

    public CsvCell getAppointmentStatus() {
        return super.getCell("AppointmentStatus");
    }

    public CsvCell getDateAppointmentBooked() {
        return super.getCell("DateAppointmentBooked");
    }

    public CsvCell getDatePatientArrival() {
        return super.getCell("DatePatientArrival");
    }

    public CsvCell getDatePatientSeen() {
        return super.getCell("DatePatientSeen");
    }

    public CsvCell getFollowUpAppointment() {
        return super.getCell("FollowUpAppointment");
    }

    public CsvCell getTelephoneAppointment() {
        return super.getCell("TelephoneAppointment");
    }

    public CsvCell getIDClinician() {
        return super.getCell("IDClinician");
    }

    public CsvCell getIDProfileClinician() {
        return super.getCell("IDProfileClinician");
    }

    public CsvCell getRotaName() {
        return super.getCell("RotaName");
    }

    public CsvCell getRotaType() {
        return super.getCell("RotaType");
    }

    public CsvCell getRotaLocation() {
        return super.getCell("RotaLocation");
    }

    public CsvCell getRotaCode() {
        return super.getCell("RotaCode");
    }

    public CsvCell getIDRotaOwner() {
        return super.getCell("IDRotaOwner");
    }

    /**
     * NOTE: this column is NOT what it appears to be. It does not contain details on who
     * the owning practitioner of the rota is. According to the TPP spec this is only used in
     * secondary care settings.
     */
    public CsvCell getIDProfileRotaOwner() {
        return super.getCell("IDProfileRotaOwner");
    }

    public CsvCell getAllowsOverbooking() {
        return super.getCell("AllowsOverbooking");
    }

    public CsvCell getBookingContactNumber() {
        return super.getCell("BookingContactNumber");
    }

    public CsvCell getDateAppointmentCancelled() {
        return super.getCell("DateAppointmentCancelled");
    }

    public CsvCell getIDRota() {
        return super.getCell("IDRota");
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

    public CsvCell getIDOrganisationRegisteredAt() {
        return super.getCell("IDOrganisationRegisteredAt");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
