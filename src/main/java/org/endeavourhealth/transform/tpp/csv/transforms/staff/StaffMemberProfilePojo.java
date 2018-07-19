package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;

import java.util.Date;
import java.util.Objects;

public class StaffMemberProfilePojo {
    public StaffMemberProfilePojo() {
    }


    // Pojo to help cache StaffMemberProfile records.
    private Long IDStaffMember;  // For use as a key
    //private String IDStaffMemberCell;
    private CsvCell RowIdentifier;
    private Date DateProfileCreated;
    private String IdProfileCreatedBy;
    private String IDStaffMemberProfileRole;
    private String StaffRole;
    private Date DateEmploymentStart;
    private Date DateEmploymentEnd;
    private String PPAID;
    private String GPLocalCode;
    private String IDOrganisation;
    private String GmpID;
    private int RemovedData;
    private CsvCell auditCsv;
    private CsvCurrentState parserState;

    public CsvCurrentState getParserState() {
        return parserState;
    }

    public void setParserState(CsvCurrentState parserState) { this.parserState = parserState;}

    //public String getIDStaffMemberCell() { return IDStaffMemberCell;}

    //public void setIDStaffMemberCell(String idStaffMemberCell) { this.IDStaffMemberCell = idStaffMemberCell;}

    public Date getDateProfileCreated() {
        return DateProfileCreated;
    }


    public void setDateProfileCreated(Date dateProfileCreated) {
        DateProfileCreated = dateProfileCreated;
    }

    public String getIdProfileCreatedBy() {
        return IdProfileCreatedBy;
    }

    public void setIdProfileCreatedBy(String idProfileCreatedBy) {
        IdProfileCreatedBy = idProfileCreatedBy;
    }

    public CsvCell getRowIdentifier() {
        return RowIdentifier;
    }

    public void setRowIdentifier(CsvCell rowIdentifier) {
        RowIdentifier = rowIdentifier;
    }

    public Long getIDStaffMember() {
        return IDStaffMember;
    }

    public void setIDStaffMember(Long IDStaffMember) {
        this.IDStaffMember = IDStaffMember;
    }

    public String getIDStaffMemberProfileRole() {
        return IDStaffMemberProfileRole;
    }

    public void setIDStaffMemberProfileRole(String IDStaffMemberProfileRole) {
        this.IDStaffMemberProfileRole = IDStaffMemberProfileRole;
    }

    public String getStaffRole() {
        return StaffRole;
    }

    public void setStaffRole(String staffRole) {
        StaffRole = staffRole;
    }

    public Date getDateEmploymentStart() {
        return DateEmploymentStart;
    }

    public void setDateEmploymentStart(Date dateEmploymentStart) {
        DateEmploymentStart = dateEmploymentStart;
    }

    public Date getDateEmploymentEnd() {
        return DateEmploymentEnd;
    }

    public void setDateEmploymentEnd(Date dateEmploymentEnd) {
        DateEmploymentEnd = dateEmploymentEnd;
    }

    public String getPPAID() {
        return PPAID;
    }

    public void setPPAID(String PPAID) {
        this.PPAID = PPAID;
    }

    public String getGPLocalCode() {
        return GPLocalCode;
    }

    public void setGPLocalCode(String GPLocalCode) {
        this.GPLocalCode = GPLocalCode;
    }

    public String getIDOrganisation() {
        return IDOrganisation;
    }

    public void setIDOrganisation(String IDOrganisation) {
        this.IDOrganisation = IDOrganisation;
    }

    public String getGmpID() {
        return GmpID;
    }

    public void setGmpID(String gmpID) {
        GmpID = gmpID;
    }


    public int getRemovedData() {
        return RemovedData;
    }

    public void setRemovedData(int removedData) {
        RemovedData = removedData;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StaffMemberProfilePojo that = (StaffMemberProfilePojo) o;
        return RemovedData == that.RemovedData &&
                Objects.equals(IDStaffMember, that.IDStaffMember);
    }

    @Override
    public int hashCode() {

        return Objects.hash(IDStaffMember, RemovedData);
    }
}
