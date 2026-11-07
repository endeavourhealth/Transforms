package org.endeavourhealth.transform.emis.csv.schema.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Slot extends AbstractCsvParser {

    public Slot(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if(version.equals(EmisCsvToFhirTransformer.VERSION_8_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_0_1)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_1)) {
            return new String[]{
                    "SlotGuid",
                    "AppointmentDate",
                    "AppointmentStartTime",
                    "PlannedDurationInMinutes",
                    "PatientGuid",
                    "SendInTime",
                    "LeftTime",
                    "DidNotAttend",
                    "PatientWaitInMin",
                    "AppointmentDelayInMin",
                    "ActualDurationInMinutes",
                    "OrganisationGuid",
                    "SessionGuid",
                    "DnaReasonCodeId",
                    "BookedDate",
                    "BookedTime",
                    "SlotStatus",
                    "SlotType",
                    "IsBookableOnline",
                    "BookingMethod",
                    "ExternalPatientGuid",
                    "ExternalPatientOrganisation",
                    "ModeOfContact",
                    "Deleted",
                    "ProcessingId"
            };
        } else if (version.equals(EmisCsvToFhirTransformer.VERSION_8_2) ||
                version.equals(EmisCsvToFhirTransformer.VERSION_8_3)){
           return new String[]{"SlotGuid",
                   "AppointmentDate",
                   "AppointmentStartTime",
                   "PlannedDurationInMinutes",
                   "PatientGuid",
                   "SendInTime",
                   "LeftTime",
                   "DidNotAttend",
                   "PatientWaitInMin",
                   "AppointmentDelayInMin",
                   "ActualDurationInMinutes",
                   "OrganisationGuid",
                   "SessionGuid",
                   "DnaReasonCodeId",
                   "BookedDate",
                   "BookedTime",
                   "SlotStatus",
                   "SlotType",
                   "IsBlocked",
                   "ServiceSetting",
                   "ContextType",
                   "NationalSlotCategoryName",
                   "NationalSlotCategoryDescription",
                   "IsBookableOnline",
                   "BookingMethod",
                   "ExternalPatientGuid",
                   "ExternalPatientOrganisation",
                   "ModeOfContact",
                   "Deleted",
                   "ProcessingId"
           };
        } else {
            return new String[]{
                    "SlotGuid",
                    "AppointmentDate",
                    "AppointmentStartTime",
                    "PlannedDurationInMinutes",
                    "PatientGuid",
                    "SendInTime",
                    "LeftTime",
                    "DidNotAttend",
                    "PatientWaitInMin",
                    "AppointmentDelayInMin",
                    "ActualDurationInMinutes",
                    "OrganisationGuid",
                    "SessionGuid",
                    "DnaReasonCodeId",
                    "Deleted",
                    "ProcessingId"
            };
        }
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getSlotGuid() {
        return super.getCell("SlotGuid");
    }
    public CsvCell getAppointmentStartDate() {
        return super.getCell("AppointmentDate");
    }
    public CsvCell getAppointmentStartTime() {
        return super.getCell("AppointmentStartTime");
    }
    public CsvCell getPlannedDurationInMinutes() {
        return super.getCell("PlannedDurationInMinutes");
    }
    public CsvCell getPatientGuid() {
        return super.getCell("PatientGuid");
    }
    public CsvCell getSendInTime() {
        return super.getCell("SendInTime");
    }
    public CsvCell getLeftTime() {
        return super.getCell("LeftTime");
    }
    public CsvCell getDidNotAttend() {
        return super.getCell("DidNotAttend");
    }
    public CsvCell getPatientWaitInMin() {
        return super.getCell("PatientWaitInMin");
    }
    public CsvCell getAppointmentDelayInMin() {
        return super.getCell("AppointmentDelayInMin");
    }
    public CsvCell getActualDurationInMinutes() {
        return super.getCell("ActualDurationInMinutes");
    }
    public CsvCell getOrganisationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getSessionGuid() {
        return super.getCell("SessionGuid");
    }
    public CsvCell getDnaReasonCodeId() {
        return super.getCell("DnaReasonCodeId");
    }
    public CsvCell getBookedDate() {
        return super.getCell("BookedDate");
    }
    public CsvCell getBookedTime() {
        return super.getCell("BookedTime");
    }
    public CsvCell getSlotStatus() {
        return super.getCell("SlotStatus");
    }
    public CsvCell getSlotType() {
        return super.getCell("SlotType");
    }
    public CsvCell getIsBookableOnline() {
        return super.getCell("IsBookableOnline");
    }
    public CsvCell getBookingMethod() {
        return super.getCell("BookingMethod");
    }
    public CsvCell getIsBlocked(){
        return super.getCell("IsBlocked");
    }
    public CsvCell getServiceSetting(){
        return super.getCell("ServiceSetting");
    }

    public CsvCell getNationalSlotCategoryName(){
        return super.getCell("NationalSlotCategoryName");
    }

    public CsvCell getContextType(){
        return super.getCell("ContextType");
    }

    public CsvCell getExternalPatientGuid() {
        return super.getCell("ExternalPatientGuid");
    }
    public CsvCell getExternalPatientOrganisation() {
        return super.getCell("ExternalPatientOrganisation");
    }
    public CsvCell getModeOfContact() {
        return super.getCell("ModeOfContact");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

    /*public String getSlotGuid() {
        return super.getString("SlotGuid");
    }
    public Date getAppointmentStartDateTime() throws TransformException {
        return super.getDateTime("AppointmentDate", "AppointmentStartTime");
    }
    public Integer getPlannedDurationInMinutes() {
        return super.getInt("PlannedDurationInMinutes");
    }
    public String getPatientGuid() {
        return super.getString("PatientGuid");
    }
    public Date getSendInDateTime() throws TransformException {
        return super.getDateTime("AppointmentDate", "SendInTime");
    }
    public Date getLeftDateTime() throws TransformException {
        return super.getDateTime("AppointmentDate", "LeftTime");
    }
    public boolean getDidNotAttend() {
        return super.getBoolean("DidNotAttend");
    }
    public Integer getPatientWaitInMin() {
        return super.getInt("PatientWaitInMin");
    }
    public Integer getAppointmentDelayInMin() {
        return super.getInt("AppointmentDelayInMin");
    }
    public Integer getActualDurationInMinutes() {
        return super.getInt("ActualDurationInMinutes");
    }
    public String getOrganisationGuid() {
        return super.getString("OrganisationGuid");
    }
    public String getSessionGuid() {
        return super.getString("SessionGuid");
    }
    public Long getDnaReasonCodeId() {
        return super.getLong("DnaReasonCodeId");
    }
    public boolean getDeleted() {
        return super.getBoolean("Deleted");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }*/
}
