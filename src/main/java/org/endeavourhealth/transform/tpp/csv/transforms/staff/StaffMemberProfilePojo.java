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
    private CsvCell IDStaffMemberCell;
    private CsvCell RowIdentifier;
    private CsvCell DateProfileCreated;
    private CsvCell IdProfileCreatedBy;
    private CsvCell IDStaffMemberProfileRole;
    private CsvCell StaffRole;
    private CsvCell DateEmploymentStart;
    private CsvCell DateEmploymentEnd;
    private CsvCell PPAID;
    private CsvCell GPLocalCode;
    private CsvCell IDOrganisation;
    private CsvCell GmpID;
    private CsvCell RemovedData;
    private CsvCell auditCsv;
    private CsvCurrentState parserState;

    public CsvCurrentState getParserState() {
        return parserState;
    }

    public void setParserState(CsvCurrentState parserState) { this.parserState = parserState;}
    public CsvCell getIDStaffMemberCell() { return IDStaffMemberCell;}

    public void setIDStaffMemberCell(CsvCell idStaffMemberCell) { this.IDStaffMemberCell = idStaffMemberCell;}

    public CsvCell getDateProfileCreated() {
        return DateProfileCreated;
    }


    public void setDateProfileCreated(CsvCell dateProfileCreated) {
        DateProfileCreated = dateProfileCreated;
    }

    public CsvCell getIdProfileCreatedBy() {
        return IdProfileCreatedBy;
    }

    public void setIdProfileCreatedBy(CsvCell idProfileCreatedBy) {
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

    public CsvCell getIDStaffMemberProfileRole() {
        return IDStaffMemberProfileRole;
    }

    public void setIDStaffMemberProfileRole(CsvCell IDStaffMemberProfileRole) {
        this.IDStaffMemberProfileRole = IDStaffMemberProfileRole;
    }

    public CsvCell getStaffRole() {
        return StaffRole;
    }

    public void setStaffRole(CsvCell staffRole) {
        StaffRole = staffRole;
    }

    public CsvCell getDateEmploymentStart() {
        return DateEmploymentStart;
    }

    public void setDateEmploymentStart(CsvCell dateEmploymentStart) {
        DateEmploymentStart = dateEmploymentStart;
    }

    public CsvCell getDateEmploymentEnd() {
        return DateEmploymentEnd;
    }

    public void setDateEmploymentEnd(CsvCell dateEmploymentEnd) {
        DateEmploymentEnd = dateEmploymentEnd;
    }

    public CsvCell getPPAID() {
        return PPAID;
    }

    public void setPPAID(CsvCell PPAID) {
        this.PPAID = PPAID;
    }

    public CsvCell getGPLocalCode() {
        return GPLocalCode;
    }

    public void setGPLocalCode(CsvCell GPLocalCode) {
        this.GPLocalCode = GPLocalCode;
    }

    public CsvCell getIDOrganisation() {
        return IDOrganisation;
    }

    public void setIDOrganisation(CsvCell IDOrganisation) {
        this.IDOrganisation = IDOrganisation;
    }

    public CsvCell getGmpID() {
        return GmpID;
    }

    public void setGmpID(CsvCell gmpID) {
        GmpID = gmpID;
    }


    public CsvCell getRemovedData() {
        return RemovedData;
    }

    public void setRemovedData(CsvCell removedData) {
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
