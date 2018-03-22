package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.barts.schema.PPALI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


    public class SRAAppointments extends AbstractCsvParser {
        private static final Logger LOG = LoggerFactory.getLogger(SRAAppointments.class);

        public SRAAppointments(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TPPCsvToFhirTransformer.CSV_FORMAT,
                    TPPCsvToFhirTransformer.DATE_FORMAT,
                    TPPCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
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
                    "IDOrganisation"

            };

        }
            public CsvCell getRowId() { return super.getCell("RowIdentifier");};
            public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
            public CsvCell getDateStart() { return super.getCell("DateStart");};
            public CsvCell getDateEnd() { return super.getCell("DateEnd");};
            public CsvCell getAppointmentStatus() { return super.getCell("AppointmentStatus");};
            public CsvCell getDateAppointmentBooked() { return super.getCell("DateAppointmentBooked");};
            public CsvCell getDatePatientArrival() { return super.getCell("DatePatientArrival");};
            public CsvCell getDatePatientSeen() { return super.getCell("DatePatientSeen");};
            public CsvCell getFollowUpAppointment() { return super.getCell("FollowUpAppointment");};
            public CsvCell getTelephoneAppointment() { return super.getCell("TelephoneAppointment");};
            public CsvCell getIdClinician() { return super.getCell("IDClinician");};
            public CsvCell getIdProfileClinician() { return super.getCell("IDProfileClinician");};
            public CsvCell getRotaName() { return super.getCell("RotaName");};
            public CsvCell getRotaType() { return super.getCell("RotaType");};
            public CsvCell getRotaLocation() { return super.getCell("RotaLocation");};
            public CsvCell getRotaCode() { return super.getCell("RotaCode");};
            public CsvCell getIdRotaOwner() { return super.getCell("IDRotaOwner");};
            public CsvCell getIdProfileRotaOwner() { return super.getCell("IDProfileRotaOwner");};
            public CsvCell getAllowsOverbooking() { return super.getCell("AllowsOverbooking");};
            public CsvCell getBookingContactNumber() { return super.getCell("BookingContactNumber");};
            public CsvCell getDateAppointmentCancelled() { return super.getCell("DateAppointmentCancelled");};
            public CsvCell getIdRota() { return super.getCell("IDRota");};
            public CsvCell getIdReferralIn() { return super.getCell("IDReferralIn");};
            public CsvCell getIdPatient() { return super.getCell("IDPatient");};
            public CsvCell getIdOrganization() { return super.getCell("IDOrganisation");};

     @Override
     protected String getFileTypeDescription() {return "TPP SRA Appointments file";}

     @Override
     protected boolean isFileAudited() {return true;}
}
