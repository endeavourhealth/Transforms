package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.transform.common.CsvCell;

import java.util.Date;

// Pojo to help cache StaffMemberProfile records.
public class StaffMemberProfilePojo {

    private CsvCell staffMemberProfileIdCell;
    private String staffRole;
    private Date dateEmploymentStart;
    private Date dateEmploymentEnd;
    private String ppaid;
    private String gpLocalCode;
    private String idOrganisation;
    private String gmpId;
    private boolean deleted;

    public CsvCell getStaffMemberProfileIdCell() {
        return staffMemberProfileIdCell;
    }

    public void setStaffMemberProfileIdCell(CsvCell staffMemberProfileIdCell) {
        this.staffMemberProfileIdCell = staffMemberProfileIdCell;
    }

    public String getStaffRole() {
        return staffRole;
    }

    public void setStaffRole(String staffRole) {
        this.staffRole = staffRole;
    }

    public Date getDateEmploymentStart() {
        return dateEmploymentStart;
    }

    public void setDateEmploymentStart(Date dateEmploymentStart) {
        this.dateEmploymentStart = dateEmploymentStart;
    }

    public Date getDateEmploymentEnd() {
        return dateEmploymentEnd;
    }

    public void setDateEmploymentEnd(Date dateEmploymentEnd) {
        this.dateEmploymentEnd = dateEmploymentEnd;
    }

    public String getPpaid() {
        return ppaid;
    }

    public void setPpaid(String ppaid) {
        this.ppaid = ppaid;
    }

    public String getGpLocalCode() {
        return gpLocalCode;
    }

    public void setGpLocalCode(String gpLocalCode) {
        this.gpLocalCode = gpLocalCode;
    }

    public String getIdOrganisation() {
        return idOrganisation;
    }

    public void setIdOrganisation(String idOrganisation) {
        this.idOrganisation = idOrganisation;
    }

    public String getGmpId() {
        return gmpId;
    }

    public void setGmpId(String gmpId) {
        this.gmpId = gmpId;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
}
