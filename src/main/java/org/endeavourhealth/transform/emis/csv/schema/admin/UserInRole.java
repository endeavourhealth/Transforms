package org.endeavourhealth.transform.emis.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class UserInRole extends AbstractCsvParser {

    public UserInRole(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "UserInRoleGuid",
                "OrganisationGuid",
                "Title",
                "GivenName",
                "Surname",
                "JobCategoryCode",
                "JobCategoryName",
                "ContractStartDate",
                "ContractEndDate",
                "ProcessingId"
        };
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getUserInRoleGuid() {
        return super.getCell("UserInRoleGuid");
    }
    public CsvCell getOrganisationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getTitle() {
        return super.getCell("Title");
    }
    public CsvCell getGivenName() {
        return super.getCell("GivenName");
    }
    public CsvCell getSurname() {
        return super.getCell("Surname");
    }
    public CsvCell getJobCategoryCode() {
        return super.getCell("JobCategoryCode");
    }
    public CsvCell getJobCategoryName() {
        return super.getCell("JobCategoryName");
    }
    public CsvCell getContractStartDate() {
        return super.getCell("ContractStartDate");
    }
    public CsvCell getContractEndDate() {
        return super.getCell("ContractEndDate");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

    /*public String getUserInRoleGuid() {
        return super.getString("UserInRoleGuid");
    }
    public String getOrganisationGuid() {
        return super.getString("OrganisationGuid");
    }
    public String getTitle() {
        return super.getString("Title");
    }
    public String getGivenName() {
        return super.getString("GivenName");
    }
    public String getSurname() {
        return super.getString("Surname");
    }
    public String getJobCategoryCode() {
        return super.getString("JobCategoryCode");
    }
    public String getJobCategoryName() {
        return super.getString("JobCategoryName");
    }
    public Date getContractStartDate() throws TransformException {
        return super.getDate("ContractStartDate");
    }
    public Date getContractEndDate() throws TransformException {
        return super.getDate("ContractEndDate");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }*/
}
