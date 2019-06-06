package org.endeavourhealth.transform.emis.csv.schema.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Session extends AbstractCsvParser {

    public Session(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "AppointmentSessionGuid",
                "Description",
                "LocationGuid",
                "SessionTypeDescription",
                "SessionCategoryDisplayName",
                "StartDate",
                "StartTime",
                "EndDate",
                "EndTime",
                "Private",
                "OrganisationGuid",
                "Deleted",
                "ProcessingId"
        };
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getAppointmnetSessionGuid() {
        return super.getCell("AppointmentSessionGuid");
    }
    public CsvCell getDescription() {
        return super.getCell("Description");
    }
    public CsvCell getLocationGuid() {
        return super.getCell("LocationGuid");
    }
    public CsvCell getSessionTypeDescription() {
        return super.getCell("SessionTypeDescription");
    }
    public CsvCell getSessionCategoryDisplayName() {
        return super.getCell("SessionCategoryDisplayName");
    }
    public CsvCell getStartDate() {
        return super.getCell("StartDate");
    }
    public CsvCell getStartTime() {
        return super.getCell("StartTime");
    }
    public CsvCell getEndDate() {
        return super.getCell("EndDate");
    }
    public CsvCell getEndTime() {
        return super.getCell("EndTime");
    }
    public CsvCell getPrivate() {
        return super.getCell("Private");
    }
    public CsvCell getOrganisationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

}
